"""
Match resources for the disease landscape pairwise using logmap
Notes:
    - not running umls since it has high resource requirements for now
"""

import os
import polars as pl
from textdistance import levenshtein
from bioregistry import normalize_curie, normalize_prefix
import subprocess
from mapnet.utils import (
    download_raw_obo_files,
    get_onto_subsets,
    convert_onto_format,
    get_novel_mappings,
)
from mapnet.logmap import run_logmap_for_target_pairs, merge_logmap_mappings

## define our subsets
dataset_def = {
    "resources": {
        "ICD10": {"version": "2019", "subset": False, "subset_identifiers": []},
        "ICD11": {"version": "2025-01", "subset": False, "subset_identifiers": []},
        "MONDO": {"version": "2025-03-04", "subset": False, "subset_identifiers": []},
    },
    "meta": {
        "dataset_dir": os.path.join(os.getcwd(), "resources"),
        "subset_dir": "disease_subset",
        "landscape": "disease",
    },
}
additional_namespaces = {
    "hp": {"version": None},
    "go": {"version": None},
    "orphanet.ordo": {"version": "4.6"},
    "chebi": {"version": None},
    "hgnc": {"version": None},
    "uberon": {"version": None},
}
run_args = {
    "tag": "0.01",
    "build": True,
    "analysis_name": "mondo_matching",
    "target_resource_prefix": "mondo",
}
MONDO_REPORT_URLS = {
    "icd11": "https://raw.githubusercontent.com/monarch-initiative/mondo-ingest/refs/heads/main/src/ontology/reports/icd11foundation_mapping_status.tsv",
    "icd10": "https://raw.githubusercontent.com/monarch-initiative/mondo-ingest/refs/heads/main/src/ontology/reports/icd10who_mapping_status.tsv",
}
prefix_to_check = [
    "icd11",
    "icd10",
]


## helper methods
def normalized_edit_similarity(x):
    """
    calculate the normalized edit similarity for all target and source class names
    """
    return levenshtein.normalized_similarity(
        x["source name"].upper(), x["target name"].upper()
    )


def load_novel_mondo_maps():
    """
    load maps that are potentially novel from mondo to other resources
    """
    novel_maps = pl.read_csv(
        f"output/logmap/{run_args['analysis_name']}/full_analysis/semra_novel_mappings.tsv",
        separator="\t",
    )
    novel_maps = novel_maps.with_columns(
        edit_similarity=pl.struct(["source name", "target name"]).map_elements(
            normalized_edit_similarity, return_dtype=pl.Float32
        )
    )
    return novel_maps.remove(
        pl.col("target identifier") == pl.col("source identifier")
    ).filter(pl.col("source prefix").eq("mondo"))


def get_mondo_report(prefix: str):
    """
    download mondo mapping report if not already present, and load in the df
    """
    url = MONDO_REPORT_URLS[prefix]
    save_path = f"mondo-{prefix}-provided.tsv"
    if not os.path.exists(save_path):
        cmd = ["wget", "-O", save_path, url]
        print("running", cmd)
        subprocess.run(cmd)
    else:
        print("loading cached df")
    df = pl.read_csv(save_path, separator="\t")
    return df.with_columns(
        pl.col("subject_id")
        .map_elements(normalize_curie, return_dtype=pl.String)
        .alias("target identifier")
    )


def compare_to_mondo(logmap_maps, mondo_report, prefix):
    """
    compare a mondo report to maps from logmap for a given prefix.
    """
    logmap_maps = logmap_maps.filter(pl.col("target prefix").eq(prefix))
    joined_maps = logmap_maps.join(mondo_report, on="target identifier", how="inner")
    joined_maps = joined_maps.remove(
        pl.col("is_excluded") | pl.col("is_deprecated")
    ).filter(~pl.col("is_mapped"))
    return joined_maps


def check_mondo_against_prefix(prefix: str, logmap_maps):
    """
    find mappings identified by logmap, that are outstanding in MODO for a specific prefix
    """
    prefix = normalize_prefix(prefix)
    mondo_report = get_mondo_report(prefix=prefix)
    return compare_to_mondo(
        logmap_maps=logmap_maps, prefix=prefix, mondo_report=mondo_report
    )


def get_novel_mondo(prefix_to_check: list):
    """
    finds mappings from logmap that are unmatched in mondo across a set of prefixes.
    """
    mondo_reports = {}
    full_maps = None
    logmap_maps = load_novel_mondo_maps()
    for prefix in prefix_to_check:
        print(f"running for {normalize_prefix(prefix)}")
        mondo_against_prefix = check_mondo_against_prefix(
            prefix=prefix, logmap_maps=logmap_maps
        )
        mondo_reports[normalize_prefix(prefix)] = mondo_against_prefix
        if full_maps is None:
            full_maps = mondo_against_prefix
        else:
            full_maps = full_maps.vstack(mondo_against_prefix)
    return mondo_reports, full_maps


def format_results(res):
    """
    format the novel mappings for Biomappings
    """
    res = res.filter(pl.col("edit_similarity").eq(1))
    ## write the result
    res = res.select(
        [
            "source prefix",
            "source identifier",
            "source name",
            "relation",
            "target prefix",
            "target identifier",
            "target name",
            "type",
            "confidence",
            "source",
        ]
    )
    res = res.with_columns(
        pl.col("type").alias("prediction_type"),
        pl.col("source").alias("prediction_source"),
    )
    res.write_csv("mondo_report.tsv", separator="\t")


if __name__ == "__main__":
    ## download the obo files for each resource
    download_raw_obo_files(dataset_def=dataset_def)
    ## subset the resources
    get_onto_subsets(dataset_def=dataset_def, verbose=True)
    ## run logmap on each pairwise resource
    run_logmap_for_target_pairs(**dataset_def, **run_args)
    ## merge the mappings
    predicted_mappings = merge_logmap_mappings(
        additional_namespaces=additional_namespaces, **dataset_def, **run_args
    )
    ## filter for only those that are novel relative to Biomapings and Semra
    novel, right, wrong = get_novel_mappings(
        predicted_mappings=predicted_mappings,
        additional_namespaces=additional_namespaces,
        **dataset_def,
        **run_args,
    )
    ## get matches for mondo
    res_dict, res = get_novel_mondo(prefix_to_check=prefix_to_check)
    ## filter for only near perfect lexical matches
    res = res.filter(pl.col("edit_similarity").eq(1))
    format_results(res=res)
