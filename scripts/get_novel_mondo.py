import polars as pl
from textdistance import levenshtein
from bioregistry import normalize_curie, normalize_prefix
import subprocess
import os

MONDO_REPORT_URLS = {
    "icd11": "https://raw.githubusercontent.com/monarch-initiative/mondo-ingest/refs/heads/main/src/ontology/reports/icd11foundation_mapping_status.tsv",
    "icd10": "https://raw.githubusercontent.com/monarch-initiative/mondo-ingest/refs/heads/main/src/ontology/reports/icd10who_mapping_status.tsv",
    "doid": "https://raw.githubusercontent.com/monarch-initiative/mondo-ingest/refs/heads/main/src/ontology/reports/doid_mapping_status.tsv",
    "ncit": "https://raw.githubusercontent.com/monarch-initiative/mondo-ingest/refs/heads/main/src/ontology/reports/ncit_mapping_status.tsv",
    "orphanet": "https://raw.githubusercontent.com/monarch-initiative/mondo-ingest/refs/heads/main/src/ontology/reports/ordo_mapping_status.tsv",
    "omim.ps": "https://raw.githubusercontent.com/monarch-initiative/mondo-ingest/refs/heads/main/src/ontology/reports/omim_mapping_status.tsv",
}


def normalized_edit_similarity(x):
    return levenshtein.normalized_similarity(
        x["source name"].upper(), x["target name"].upper()
    )


def load_novel_mondo_maps():
    """
    load maps that are potentially novel from mondo to other resources
    """
    novel_maps = pl.read_csv(
        "output/logmap/disease_landscape/full_analysis/semra_novel_mappings.tsv",
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
    logmap_maps = logmap_maps.filter(pl.col("target prefix").eq(prefix))
    joined_maps = logmap_maps.join(mondo_report, on="target identifier", how="inner")
    joined_maps = joined_maps.remove(
        pl.col("is_excluded") | pl.col("is_deprecated")
    ).filter(~pl.col("is_mapped"))
    return joined_maps


def check_mondo_against_prefix(prefix: str, logmap_maps):
    prefix = normalize_prefix(prefix)
    mondo_report = get_mondo_report(prefix=prefix)
    return compare_to_mondo(
        logmap_maps=logmap_maps, prefix=prefix, mondo_report=mondo_report
    )


def main(prefix_to_check: list):
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


prefix_to_check = ["icd11", "icd10", "doid", "ncit", "orphanet", "omim.ps"]


if __name__ == "__main__":
    res_dict, res = main(prefix_to_check=prefix_to_check)
    res = res.filter(pl.col("edit_similarity").eq(1))
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
    # res.write_csv('mondo_report_full.tsv', separator = '\t')
    res.write_csv("mondo_report.tsv", separator="\t")
