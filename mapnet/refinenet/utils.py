import logging
import os

import networkx as nx
import numpy as np
import polars as pl
from datasets import Dataset
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from mapnet.utils import get_name_from_curie

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


def format_mapping_input(row, k=3, relation: bool = True):
    def format_list(label, items):
        if not items:
            return f"{label}: None"
        return f"{label}:\n  " + "\n  ".join(items[:k])

    line = [
        f"SOURCE_NAME: {row['source name']}",
        f"TARGET_NAME: {row['target name']}",
        # "[SEP]",
        # f"SOURCE_ONTOLOGY: {row['source prefix']}",
        # f"TARGET_ONTOLOGY: {row['target prefix']}",
    ]
    if relation:
        line += [
            "[SEP]",
            format_list("SOURCE_ANCESTORS", row.get("source ancestor names", [])),
            format_list("SOURCE_DESCENDANTS", row.get("source descendant names", [])),
            format_list("TARGET_ANCESTORS", row.get("target ancestor names", [])),
            format_list("TARGET_DESCENDANTS", row.get("target descendant names", [])),
        ]
    return "\n".join(line)


def parse_formatted_mapping_input(text, relation: bool = True):
    lines = text.strip().splitlines()
    result = {
        "source name": None,
        "source prefix": None,
        "target name": None,
        "target prefix": None,
    }
    if relation:
        result = result | {
            "source ancestor names": [],
            "source descendant names": [],
            "target ancestor names": [],
            "target descendant names": [],
        }

    current_list_key = None
    for line in lines:
        line = line.strip()
        if not line or line == "[SEP]":
            continue
        elif line.startswith("SOURCE_NAME:"):
            result["source name"] = line.split(":", 1)[1].strip()
        elif line.startswith("SOURCE_ONTOLOGY:"):
            result["source prefix"] = line.split(":", 1)[1].strip()
        elif line.startswith("TARGET_NAME:"):
            result["target name"] = line.split(":", 1)[1].strip()
        elif line.startswith("TARGET_ONTOLOGY:"):
            result["target prefix"] = line.split(":", 1)[1].strip()
        elif current_list_key and line.startswith("  "):  # list item
            result[current_list_key].append(line.strip())
        elif relation:
            if line.startswith("SOURCE_ANCESTORS:"):
                current_list_key = "source ancestor names"
                result[current_list_key] = []
            elif line.startswith("SOURCE_DESCENDANTS:"):
                current_list_key = "source descendant names"
                result[current_list_key] = []
            elif line.startswith("TARGET_ANCESTORS:"):
                current_list_key = "target ancestor names"
                result[current_list_key] = []
            elif line.startswith("TARGET_DESCENDANTS:"):
                current_list_key = "target descendant names"
                result[current_list_key] = []
    return result


def parse_raw_refinenet_dataset(df: pl.DataFrame, evaluable: bool, relation: bool):
    """loads a raw dataset for refinenet"""
    lines = []
    for row in df.iter_rows(named=True):
        line = {}
        line["txt"] = format_mapping_input(row, k=3, relation=relation)
        if evaluable:
            line["label"] = row["class"]
        line["orig"] = row
        lines.append(line)
    return lines


def get_refinenet_dataset(df: pl.DataFrame, tokenizer, relation: bool):
    """wrapper function for loading and formatting a dataset."""
    evaluable = "class" in df.columns  ## if have class labels can evaluate
    lines = parse_raw_refinenet_dataset(df=df, evaluable=evaluable, relation=relation)
    tokenize = tokenize_factory(
        evaluable=evaluable, max_length=256, tokenizer=tokenizer
    )
    return Dataset.from_list(lines).map(tokenize, batched=True)
