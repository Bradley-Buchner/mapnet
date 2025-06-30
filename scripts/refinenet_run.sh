#!/bin/bash
: '
Script for setting up training and running inference with RefineNet Model
Steps:
    1. Generate synthetic training dataset.
    2. Format inference dataset.
    3. Train RefineNet model.
    4. Use RefineNet Model for inferences.
'

# Optional argument: --step <N>
STEP_TO_RUN=""

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --step)
            STEP_TO_RUN="$2"
            shift 2
            ;;
        *)
            echo "Unknown parameter passed: $1"
            exit 1
            ;;
    esac
done

run_step() {
    local step_num=$1
    shift
    if [[ -z "$STEP_TO_RUN" || "$STEP_TO_RUN" == "$step_num" ]]; then
        echo "Running step $step_num"
        "$@"
    fi
}

# Step 1
run_step 1 bash -c \
    "uv run \
        mapnet/refinenet/dataset.py \
        --config-path mapnet/utils/configs/disease_landscape.json \
        --max-distance 1 \
        -o ./generated_maps.parquet \
        --synthetic"

# Step 2
run_step 2 bash -c \
	"uv run \
		mapnet/refinenet/dataset.py \
		--config-path mapnet/utils/configs/disease_landscape.json \
		--max-distance 1 \
		-o ./logmap_maps.parquet \
		--mappings-path output/logmap/disease_landscape/full_analysis/semra_novel_mappings.tsv \
		--edit-cutoff 0.00"

# Step 3
run_step 3 bash -c \
    "uv run \
        mapnet/refinenet/train.py \
        --model-name SapBERT \
        --dataset-path generated_maps.parquet \
        --output-dir output/refinenet \
        --epochs 10 \
        --batch-size 16"

# Step 4
run_step 4 bash -c \
    "uv run \
        mapnet/refinenet/inference.py \
        --model-name SapBERT \
        --dataset-path logmap_maps.parquet \
        --output-dir output/refinenet"
