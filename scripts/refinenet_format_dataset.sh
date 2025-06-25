#!/bin/bash 

bash -c \
	"
	uv run \
		mapnet/refinenet/dataset.py \
		--config-path mapnet/utils/configs/disease_landscape.json \
		--max-distance 3 \
		-o logmap_maps.parquet \
		--mappings-path output/logmap/disease_landscape/full_analysis/semra_novel_mappings.tsv \
		--edit-cutoff 0.95
	"

