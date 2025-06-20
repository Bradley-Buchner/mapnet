import polars as pl
import biomappings
from typing import Callable
from itertools import combinations, combinations_with_replacement
from mapnet.utils.utils import make_undirected, sssom_to_biomappings
from mapnet.utils.obo import load_known_mappings_df
import os
import subprocess
import logging
logger = logging.getLogger(__name__)


def load_biomappings_df(
    target_prefix: str, source_prefix: str, undirected: bool = True
):
    """return a polars data frame with the mappings from biomapping for two given ontologies."""

    
    df = (
            pl.from_records(
                biomappings.load_mappings(), strict=False, infer_schema_length=None
            )
        )
    df = sssom_to_biomappings(df)
    df = (
        df
            .filter(pl.col("source prefix").eq(source_prefix.lower()))
            .filter(pl.col("target prefix").eq(target_prefix.lower()))
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
    if undirected:
        return make_undirected(df)
    else:
        return df


def batch_load_biomappings_df(matched_resources: dict, **_):
    full_df = None
    logger.info(matched_resources)
    for source_prefix, target_prefix in combinations(matched_resources, r=2):
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


def repair_names_with_semra(predicted_mappings, semra_landscape_df):
    """
    try to find names that were missing in the pyobos in the Semra dataset
    """
    semra_name_map = {}
    for row in semra_landscape_df.iter_rows(named=True):
        semra_name_map[row["target identifier"]] = row["target name"]
        semra_name_map[row["source identifier"]] = row["source name"]

    def map_func(x):
        if x in semra_name_map:
            return semra_name_map[x]
        else:
            return "NO_NAME_FOUND"

    return predicted_mappings.with_columns(
        pl.when(pl.col("target name").eq("NO_NAME_FOUND"))
        .then(
            pl.col("target identifier").map_elements(map_func, return_dtype=pl.String)
        )
        .otherwise(pl.col("target name"))
        .alias("target name"),
        pl.when(pl.col("source name").eq("NO_NAME_FOUND"))
        .then(
            pl.col("source identifier").map_elements(map_func, return_dtype=pl.String)
        )
        .otherwise(pl.col("source name"))
        .alias("source name"),
    )


def get_right_wrong_mappings(predictions_df, ground_truth_df):
    """
    finds maps that are true false or can't be classfied from ground truth df
    """
    joined = predictions_df.join(
        ground_truth_df,
        on=["source identifier", "target prefix"],
        how="inner",
        suffix="_b",
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
    right = predictions_df.join(right_keys, on=grp_cols, how="inner")
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
    wrong = predictions_df.join(wrong_keys, on=grp_cols, how="inner")
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
    novel = predictions_df.join(grouped, on=grp_cols, how="anti")
    ## make sure that wrongs are symmetrical
    recovered_wrong = novel.join(
        wrong,
        left_on=["source identifier", "target identifier"],
        right_on=["predicted identifier", "source identifier"],
        how="inner",
    ).select(
        [
            pl.col("source identifier"),
            pl.col("source name"),
            pl.col("target prefix"),
            pl.col("target identifier").alias("predicted identifier"),
            pl.col("target name").alias("predicted name"),
            pl.col("true identifier"),
            pl.col("true name"),
            pl.col("confidence"),
        ]
    )
    wrong = wrong.vstack(recovered_wrong)
    novel = novel.join(
        wrong,
        left_on=["source identifier", "target identifier"],
        right_on=["predicted identifier", "source identifier"],
        how="anti",
    )
    return right, wrong, novel


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
        output_dir = os.path.join(
            os.getcwd(), "output", "logmap", analysis_name, "full_analysis"
        )
        os.makedirs(output_dir, exist_ok=True)
    ## load in evidence
    evidence = None
    matched_resources = predicted_mappings["source prefix"].unique()
    if check_biomappings:
        evidence = batch_load_biomappings_df(matched_resources=matched_resources)
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
    if check_semra:
        semra_landscape_df = load_semera_landscape_df(
            landscape_name=meta["landscape"],
            additional_namespaces=additional_namespaces,
            resources=resources,
            sssom=False,
        )
        predicted_mappings = repair_names_with_semra(
            predicted_mappings=predicted_mappings, semra_landscape_df=semra_landscape_df
        )
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
    right, wrong, novel = get_right_wrong_mappings(
        predictions_df=predicted_mappings, ground_truth_df=evidence
    )
    right.write_csv(os.path.join(output_dir, "right_mappings.tsv"), separator="\t")
    wrong.write_csv(os.path.join(output_dir, "wrong_mappings.tsv"), separator="\t")
    novel.write_csv(os.path.join(output_dir, "novel_mappings.tsv"), separator="\t")
    if check_semra:
        right_semra, wrong_semra, novel_semra = get_right_wrong_mappings(
            predictions_df=novel, ground_truth_df=semra_landscape_df
        )
        right_semra.write_csv(
            os.path.join(output_dir, "semra_right_mappings.tsv"), separator="\t"
        )
        wrong_semra.write_csv(
            os.path.join(output_dir, "semra_wrong_mappings.tsv"), separator="\t"
        )
        novel_semra.write_csv(
            os.path.join(output_dir, "semra_novel_mappings.tsv"), separator="\t"
        )
    return novel, right, wrong
