import logging

import numpy as np
import polars as pl
from datasets import Dataset
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from mapnet.refinenet.constants import MODELS

logger = logging.getLogger(__name__)


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
    """Format mapping row as prompt for model."""

    def format_list(label, items):
        if not items:
            return f"{label}: None"
        return f"{label}:\n  " + "\n  ".join(items[:k])

    line = [
        f"SOURCE_NAME: {row['source name']}",
        f"TARGET_NAME: {row['target name']}",
        "[SEP]",
        f"NAME_DISTANCE_BIN: {row['edit_similarity']}",
        "[SEP]",
        f"SOURCE_ONTOLOGY: {row['source prefix']}",
        f"TARGET_ONTOLOGY: {row['target prefix']}",
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


def parse_raw_refinenet_dataset(df: pl.DataFrame, evaluable: bool, relation: bool):
    """Format a dataset of mappings for training or inference of RefineNet models."""
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
    """wrapper function for loading and formatting datasets for training and inference with a RefineNset model"""
    evaluable = "class" in df.columns  ## if have class labels can evaluate
    lines = parse_raw_refinenet_dataset(df=df, evaluable=evaluable, relation=relation)
    tokenize = tokenize_factory(
        evaluable=evaluable, max_length=256, tokenizer=tokenizer
    )
    return Dataset.from_list(lines).map(tokenize, batched=True)
