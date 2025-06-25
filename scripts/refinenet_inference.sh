#!/bin/bash 

bash -c \
	"
	uv run \
		mapnet/refinenet/inference.py \
		--model-name SapBERT \
		--dataset-path logmap_maps.parquet \
		--output-dir output/refinenet
	"


