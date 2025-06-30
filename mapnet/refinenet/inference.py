"""Run inference on a processed dataset using a trained refinenet model"""

import argparse
import logging
import os
from datetime import datetime

import polars as pl
import torch
from torch.utils.data import DataLoader
from tqdm import tqdm
from transformers import AutoModelForSequenceClassification

from mapnet.refinenet import (get_refinenet_dataset, load_model,)
from mapnet.utils import file_safety_check

from mapnet.refinenet.constants import LABEL_MAP

logger = logging.getLogger(__name__)


def load_trained_model(model_path: str):
    """load trained model from default path or one provided by user."""
    ## get default path if not already present
    if model_path == "":
        model_dir = os.path.join("output", "refinenet")
        dates = os.listdir(model_dir)
        dates = filter(lambda x: x != "predictions", dates)
        dates = sorted(
            dates, key=lambda d: datetime.strptime(d, "%Y_%m_%d"), reverse=True
        )
        dir_path = os.path.join(model_dir, dates[0])
        checkpoints = os.listdir(dir_path)
        checkpoints = sorted(
            checkpoints, key=lambda x: x.removeprefix("checkpoint-"), reverse=True
        )
        model_path = os.path.join(dir_path, checkpoints[0])

    logger.info(f"loading model from {model_path}...")
    return AutoModelForSequenceClassification.from_pretrained(model_path, num_labels=3)


def collate_fn(batch):
    """function to collate all numeric cols, but keep txt as is so can associate with original rows"""
    batch_keys = batch[0].keys()
    collated = {}
    for key in batch_keys:
        values = [item[key] for item in batch]
        if key in ["txt", "orig"]:
            collated[key] = values
        else:
            collated[key] = torch.tensor(values)
    return collated


def get_inference_dataset(dataset):
    return DataLoader(dataset, batch_size=16, collate_fn=collate_fn)


def main(
    model_path: str, model_name: str, dataset_path: str, output_dir: str, relation: bool
):
    logger.info("Running inference...")
    _, tokenizer = load_model(model_name)
    df = pl.read_parquet(dataset_path)
    model = load_trained_model(model_path=model_path)
    model.eval()
    dataset = get_refinenet_dataset(df=df, tokenizer=tokenizer, relation=relation)
    loader = get_inference_dataset(dataset)
    rows = []
    j = 0
    for batch in tqdm(loader, desc="Running inference"):
        output = model(
            input_ids=batch["input_ids"],
            token_type_ids=batch["token_type_ids"],
            attention_mask=batch["attention_mask"],
        )
        preds = torch.argmax(output.logits, dim=1)
        row = batch["orig"]
        for i, pred in enumerate(preds):
            row[i]["pred"] = LABEL_MAP[pred.item()]
        rows += row
        if j > 20:
            break
        j += 1
    res_df = pl.DataFrame(rows)
    write_path = os.path.join(output_dir, "predictions")
    pq_path = os.path.join(write_path, "full_preds.parquet")
    tsv_path = os.path.join(write_path, "non_nested_preds.tsv")
    file_safety_check(pq_path)
    file_safety_check(tsv_path)
    res_df.write_parquet(pq_path)
    res_df.select(
        [
            "source prefix",
            "source identifier",
            "source name",
            "target prefix",
            "target identifier",
            "target name",
            "pred",
        ]
    ).write_csv(tsv_path, separator="\t")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--model_path",
        type=str,
        default="",
        help="path to trained refinenet model if not is provided will load the most recently saved model in the default save location.",
    )
    parser.add_argument(
        "-m",
        "--model-name",
        type=str,
        default="SapBERT",
        help="name of bertmodel to use must be in mapnet.refinenet.Models",
    )
    parser.add_argument(
        "-d",
        "--dataset-path",
        type=str,
        default="logmap_maps.parquet",
        help="path to parquet file to run inference on",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default="output/refinenet/",
        help="path to directory to save predictions",
    )
    parser.add_argument(
        "-r",
        "--relation",
        action="store_true",
        help="if to use relations in input",
    )
    args = parser.parse_args()
    main(**vars(args))
