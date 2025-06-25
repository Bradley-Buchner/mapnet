"""Train and evaluate a refine net model on a generated dataset (can be generated with `scripts/generate_broad_narrow_dataset.py`"""

import argparse
import logging
from os import makedirs
from os.path import join

import polars as pl
from datasets import Dataset
from numpy import argmax
from sklearn.metrics import precision_recall_fscore_support
from transformers import Trainer, TrainingArguments

from mapnet.refinenet import get_refinenet_dataset, load_model
from mapnet.utils import get_current_date_ymd

logger = logging.getLogger(__name__)


def split_dataset(dataset: Dataset):
    """utility function for splitting dataset into train test and split."""
    split_dataset = dataset.train_test_split(test_size=0.3, seed=101, shuffle=True)
    train_dataset = split_dataset["train"]
    temp_dataset = split_dataset["test"]
    val_test_split = temp_dataset.train_test_split(
        test_size=0.3, seed=101, shuffle=True
    )
    val_dataset = val_test_split["train"]
    test_dataset = val_test_split["test"]
    return train_dataset, val_dataset, test_dataset


def compute_metrics(p):
    """computes the a set of desired metrics for use on the evaluation set."""
    preds = argmax(p.predictions, axis=1)
    labels = p.label_ids
    precision, recall, f1, _ = precision_recall_fscore_support(
        labels, preds, average="macro", zero_division=0
    )
    return {"precision": precision, "recall": recall, "f1": f1}


def main(
    model_name: str, dataset_path: str, output_dir: str, epochs: int, batch_size: int
):
    logger.info("Training model...")
    output_dir = join(output_dir, get_current_date_ymd())
    makedirs(output_dir, exist_ok=True)
    ## initiate model and tokenizer
    model, tokenizer = load_model(model_name=model_name)
    ## load raw dataset
    df = pl.read_parquet(dataset_path)
    ## format and put in dataset
    dataset = get_refinenet_dataset(df=df, tokenizer=tokenizer)
    train_dataset, val_dataset, test_dataset = split_dataset(dataset=dataset)
    ## train model
    training_args = TrainingArguments(
        output_dir=output_dir,
        evaluation_strategy="epoch",
        save_strategy="epoch",
        logging_strategy="epoch",
        learning_rate=2e-5,
        per_device_train_batch_size=batch_size,
        per_device_eval_batch_size=16,
        num_train_epochs=epochs,
        weight_decay=0.01,
        save_total_limit=1,
        logging_dir="./logs",
    )
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=val_dataset,
        compute_metrics=compute_metrics,
    )
    ## TODO: add class weights
    trainer.train()
    logger.info(f"Model training complete!")
    logger.info(f"Model has been written to {output_dir}")
    ## test model on test dataset
    test_metrics = trainer.evaluate(eval_dataset=test_dataset)
    logging.info("Test set evaluation:")
    logging.info(str(test_metrics))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
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
        default="generated_maps.parquet",
        help="path to parquet file with training dataset",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default="output/refinenet/",
        help="path to directory to save model",
    )
    parser.add_argument(
        "-e",
        "--epochs",
        type=int,
        default=10,
        help="number of epochs to train model for.",
    )
    parser.add_argument(
        "-b",
        "--batch-size",
        type=int,
        default=16,
        help="Size of batch to use when training/evaluating model.",
    )
    args = parser.parse_args()
    main(**vars(args))
