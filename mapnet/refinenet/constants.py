"""Constants for refine map"""

import polars as pl

LABEL_MAP = {
    0: "exact match",
    1: "broad match",
    2: "narrow match",
}


GENERATED_DATASET_SCHEMA = pl.Schema(
    [
        ("source identifier", pl.String),
        ("source name", pl.String),
        ("source prefix", pl.String),
        ("target identifier", pl.String),
        ("target name", pl.String),
        ("target prefix", pl.String),
        ("class", pl.Int64),
        ("source descendant identifiers", pl.List(pl.String)),
        ("source descendant names", pl.List(pl.String)),
        ("target descendant identifiers", pl.List(pl.String)),
        ("target descendant names", pl.List(pl.String)),
        ("source ancestor identifiers", pl.List(pl.String)),
        ("source ancestor names", pl.List(pl.String)),
        ("target ancestor identifiers", pl.List(pl.String)),
        ("target ancestor names", pl.List(pl.String)),
        ("edit_similarity", pl.String),
    ]
)

INFERENCE_DATASET_SCHEMA = pl.Schema(
    [
        ("source identifier", pl.String),
        ("source name", pl.String),
        ("source prefix", pl.String),
        ("target identifier", pl.String),
        ("target name", pl.String),
        ("target prefix", pl.String),
        ("source descendant identifiers", pl.List(pl.String)),
        ("source descendant names", pl.List(pl.String)),
        ("target descendant identifiers", pl.List(pl.String)),
        ("target descendant names", pl.List(pl.String)),
        ("source ancestor identifiers", pl.List(pl.String)),
        ("source ancestor names", pl.List(pl.String)),
        ("target ancestor identifiers", pl.List(pl.String)),
        ("target ancestor names", pl.List(pl.String)),
        ("edit_similarity", pl.String),
    ]
)

MODELS = {
    "Bio_ClinicalBERT": "emilyalsentzer/Bio_ClinicalBERT",  ## used by BERTMAP, may be better for clinical use cases.
    "PubMedBERT": "microsoft/BiomedNLP-BiomedBERT-base-uncased-abstract-fulltext",  ## PubMedBERT, uses PubMed so may be good for research terms
    "SapBERT": "cambridgeltl/SapBERT-from-PubMedBERT-fulltext",  ## SapBert trained with UMLS as KG
}
