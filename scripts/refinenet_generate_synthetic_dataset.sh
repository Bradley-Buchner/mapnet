#!/bin/bash 

bash -c \
	"
	uv run \
		mapnet/refinenet/dataset.py \
		--config-path mapnet/utils/configs/disease_landscape.json \
		--max-distance 3 \
		-o generated_maps.parquet \
		--synthetic
	"

