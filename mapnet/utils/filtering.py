import polars as pl
import biomappings
from typing import Callable
from itertools import combinations, combinations_with_replacement
from mapnet.utils.utils import make_undirected, sssom_to_biomappings
from mapnet.utils.obo import load_known_mappings_df
import os
import subprocess


def load_biomappings_df(target_prefix: str, source_prefix: str):
    """return a polars data frame with the mappings from biomapping for two given ontologies."""
    df = (
        (
            pl.from_records(
                biomappings.load_mappings(), strict=False, infer_schema_length=None
            )
            .filter(pl.col("source prefix").eq(source_prefix.lower()))
            .filter(pl.col("target prefix").eq(target_prefix.lower()))
        )
        .with_columns(
            (
                pl.col("source prefix")
                + ":"
                + pl.col("source identifier").str.split(":").list.get(-1)
            ).alias("source identifier"),
            (
                pl.col("target prefix")
                + ":"
                + pl.col("target identifier").str.split(":").list.get(-1)
            ).alias("target identifier"),
        )
        .select(
            [
                "source identifier",
                "source name",
                "source prefix",
                "target identifier",
                "target name",
                "target prefix",
            ]
        )
    )
    return make_undirected(df)


def batch_load_biomappings_df(resources: dict, **_):
    full_df = None
    for source_prefix, target_prefix in combinations(resources, r=2):
        df = load_biomappings_df(
            target_prefix=target_prefix, source_prefix=source_prefix
        )
        reverse_maps = load_biomappings_df(
            target_prefix=source_prefix, source_prefix=target_prefix
        )
        if full_df is None:
            full_df = df.vstack(reverse_maps)
        else:
            full_df = full_df.vstack(df.vstack(reverse_maps))
    return full_df.unique()


def pull_semra_landscape_mappings(landscape_name: str, output_name: str):
    """download semra landscape mapping file"""
    landscape_urls = {
        "disease": "https://zenodo.org/records/15164180/files/processed.sssom.tsv?download=1"
    }
    cmd = ["wget", "-O", output_name, landscape_urls[landscape_name]]
    subprocess.run(cmd)


def load_semera_landscape_df(
    landscape_name: str,
    resources: dict,
    additional_namespaces: dict,
    sssom: bool = False,
):
    """
    load in the mappings df for a semra landscape
    """
    df_path = os.path.join(
        os.getcwd(), "resources", f"semra_{landscape_name}_landscape_mappings.tsv"
    )
    ## download the mapping file if not already present
    if not os.path.exists(df_path):
        pull_semra_landscape_mappings(
            landscape_name=landscape_name, output_name=df_path
        )
    df = pl.read_csv(df_path, separator="\t")
    if sssom:
        return df
    else:
        return sssom_to_biomappings(
            df, resources=resources, additional_namespaces=additional_namespaces
        )


def get_novel_mappings(
    predicted_mappings: pl.DataFrame,
    resources: dict,
    meta: dict,
    output_dir: str = None,
    analysis_name: str = None,
    additional_namespaces: dict = None,
    check_biomappings: bool = True,
    check_known_mappings: bool = True,
    check_semra: bool = True,
    **_,
):
    """filter out mappings that are already in biomappings and or known mappings from a tsv file"""
    if output_dir is not None:
        output_dir = output_dir
    elif "output_dir" in meta:
        output_dir = meta["output_dir"]
    else:
        output_dir = os.path.join(os.getcwd(), "output", "logmap", analysis_name)
        os.makedirs(output_dir, exist_ok=True)
    ## load in evidence
    evidence = None
    if check_biomappings:
        evidence = batch_load_biomappings_df(resources=resources)
    if check_known_mappings:
        known_mappings = make_undirected(
            load_known_mappings_df(
                resources=resources,
                meta=meta,
                additional_namespaces=additional_namespaces,
                sssom=False,
            )
        )
        evidence = (
            known_mappings if (evidence is None) else evidence.vstack(known_mappings)
        )
    # if check_semra:
    #     semra_landscape_df = load_semera_landscape_df(landscape_name=meta['landscape'], additional_namespaces=additional_namespaces, resources=resources, sssom=False)
    #     matched_resources = predicted_mappings['source prefix'].unique()
    #     semra_landscape_df.filter(
    #             (pl.col("source prefix").is_in(matched_resources))
    #             &
    #             (pl.col("target prefix").is_in(matched_resources))
    #             )
    #     return semra_landscape_df
    ## find classes that have no name for either target or source and save them
    predicted_mappings.filter(
        (pl.col("source name").eq("NO_NAME_FOUND"))
        | (pl.col("target name").eq("NO_NAME_FOUND"))
    ).write_csv(os.path.join(output_dir, "maps_with_no_names.tsv"), separator="\t")
    predicted_mappings = predicted_mappings.remove(
        (pl.col("source name").eq("NO_NAME_FOUND"))
        | (pl.col("target name").eq("NO_NAME_FOUND"))
    )
    ## find classes that had mappings in the predictions and no mappings in the evidence
    cols = predicted_mappings.columns
    cols.remove("confidence")
    joined = predicted_mappings.join(
        evidence, on=["source identifier", "target prefix"], how="inner", suffix="_b"
    )
    grp_cols = ["source identifier", "target prefix", "target identifier"]
    grouped = joined.group_by(grp_cols).agg(
        [
            pl.col("target identifier_b").unique().alias("matched_true_ids"),
            pl.col("target name_b").unique().alias("matched_true_names"),
        ]
    )
    right_keys = grouped.filter(
        pl.col("target identifier").is_in(pl.col("matched_true_ids"))
    )
    wrong_keys = grouped.filter(
        ~pl.col("target identifier").is_in(pl.col("matched_true_ids"))
    )
    right = predicted_mappings.join(right_keys, on=grp_cols, how="inner")
    right = right.select(
        [
            pl.col("source identifier"),
            pl.col("source name"),
            pl.col("target prefix"),
            pl.col("target identifier"),
            pl.col("target name"),
            pl.col("confidence"),
        ]
    )
    right.write_csv(os.path.join(output_dir, "right_mappings.tsv"), separator="\t")
    wrong = predicted_mappings.join(wrong_keys, on=grp_cols, how="inner")
    wrong = wrong.with_columns(
        pl.col("matched_true_ids").list.join(", ").alias("matched_true_ids"),
        pl.col("matched_true_names").list.join(", ").alias("matched_true_names"),
    ).select(
        [
            pl.col("source identifier"),
            pl.col("source name"),
            pl.col("target prefix"),
            pl.col("target identifier").alias("predicted identifier"),
            pl.col("target name").alias("predicted name"),
            pl.col("matched_true_ids").alias("true identifier"),
            pl.col("matched_true_names").alias("true name"),
            pl.col("confidence"),
        ]
    )
    wrong.write_csv(os.path.join(output_dir, "wrong_mappings.tsv"), separator="\t")

    novel = predicted_mappings.join(grouped, on=grp_cols, how="anti")
    novel.write_csv(os.path.join(output_dir, "novel_mappings.tsv"), separator="\t")
    return novel, right, wrong
