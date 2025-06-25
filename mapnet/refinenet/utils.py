import logging

import polars as pl
from datasets import Dataset
from transformers import AutoModelForSequenceClassification, AutoTokenizer

logger = logging.getLogger(__name__)

MODELS = {
    "Bio_ClinicalBERT": "emilyalsentzer/Bio_ClinicalBERT",  ## used by BERTMAP, may be better for clinical use cases.
    "PubMedBERT": "microsoft/BiomedNLP-BiomedBERT-base-uncased-abstract-fulltext",  ## PubMedBERT, uses PubMed so may be good for research terms
    "SapBERT": "cambridgeltl/SapBERT-from-PubMedBERT-fulltext",  ## SapBert trained with UMLS as KG
}


def load_model(model_name: str):
    """loads a given model and its tokenizer."""
    logger.info(f"loading pre-trained model from {model_name}")
    model = AutoModelForSequenceClassification.from_pretrained(
        MODELS[model_name], num_labels=3
    )
    tokenizer = AutoTokenizer.from_pretrained(MODELS[model_name])
    return model, tokenizer


def tokenize_factory(tokenizer, evaluable: bool = True, max_length: int = 256):
    """returns a tokenize function."""

    def tokenize(batch: dict):
        rep = tokenizer(
            batch["txt"],
            padding="max_length",
            truncation=True,
            max_length=max_length,
        )
        if evaluable:
            rep["label"] = batch["label"]
        return rep

    return tokenize


def parse_txt_line(txt_line):
    """revert a parsed text line to original format"""
    source_part, target_part = txt_line.split(" [SEP] ")
    source_fields = source_part.split(" | ")
    target_fields = target_part.split(" | ")

    return {
        "source prefix": source_fields[0],
        "source name": source_fields[1],
        "source descendant names": (
            source_fields[2].split(", ") if source_fields[2] else []
        ),
        "source ancestor names": (
            source_fields[3].split(", ") if source_fields[3] else []
        ),
        "target prefix": target_fields[0],
        "target name": target_fields[1],
        "target descendant names": (
            target_fields[2].split(", ") if target_fields[2] else []
        ),
        "target ancestor names": (
            target_fields[3].split(", ") if target_fields[3] else []
        ),
    }


def parse_raw_refinenet_dataset(df: pl.DataFrame, evaluable: bool):
    """loads a raw dataset for refinenet"""
    lines = []
    for row in df.iter_rows(named=True):
        line = {}
        line["txt"] = (
            f"{row['source prefix']} | {row['source name']} | {', '.join(row['source descendant names'])} | {', '.join(row['source ancestor names'])} [SEP] {row['target prefix']} | {row['target name']} | {', '.join(row['target descendant names'])} | {', '.join(row['target ancestor names'])}"
        )
        if evaluable:
            line["label"] = row["class"]
        lines.append(line)
    return lines


def get_refinenet_dataset(df: pl.DataFrame, tokenizer):
    """wrapper function for loading and formatting a dataset."""
    evaluable = "class" in df.columns  ## if have class labels can evaluate
    lines = parse_raw_refinenet_dataset(df=df, evaluable=evaluable)
    tokenize = tokenize_factory(
        evaluable=evaluable, max_length=256, tokenizer=tokenizer
    )
    return Dataset.from_list(lines).map(tokenize, batched=True)
