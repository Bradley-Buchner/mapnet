#!/bin/bash 

bash -c \
	"
	uv run \
		mapnet/refinenet/train.py \
		--model-name SapBERT \
		--dataset-path generated_maps.parquet \
		--output-dir output/refinenet \
		--epochs 10 \
		--batch-size 16
	"

