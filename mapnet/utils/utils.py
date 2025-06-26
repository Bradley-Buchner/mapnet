import datetime
import json
import logging
import os
import subprocess
import sys

import bioregistry
import networkx as nx
import polars as pl
from bioregistry import normalize_curie, normalize_prefix
from bioregistry.resolve import get_owl_download
from pyobo import get_id_name_mapping
from textdistance import levenshtein

logger = logging.getLogger(__name__)


def get_current_date_ymd():
    """Returns the current date as a string in YYYY_MM_DD format."""
    now = datetime.datetime.now()
    return now.strftime("%Y_%m_%d")


def download_owl(
    target_ontology_train: str,
    source_ontology_train: str,
    source_ontologies_inference: list,
    target_ontologies_inference: list,
    ontologies_path: str,
):
    """Download OWL Files for specified ontologies."""
    os.makedirs(ontologies_path, exist_ok=True)
    ontology_paths = {}
    for ontology in (
        [target_ontology_train, source_ontology_train]
        + source_ontologies_inference
        + target_ontologies_inference
    ):
        if ontology.upper() == "MESH":
            ## bio-registry does not have a download link for mesh so adding this
            ext = ".ttl"
            url = "https://data.bioontology.org/ontologies/MESH/submissions/28/download?apikey=8b5b7825-538d-40e0-9e9e-5ab9274a9aeb"
        else:
            ext = ".owl"
            url = get_owl_download(ontology.upper())
        ontology_path = os.path.join(ontologies_path, ontology.lower() + ext)
        ontology_paths[ontology.lower()] = ontology_path
        if not os.path.isfile(ontology_path):
            logger.info("Downloading {0}".format(ontology))
            cmd = ["wget", "-O", ontology_path, url]
            subprocess.run(cmd)
            ## mesh is large so download the zip and unzip it.
            if ontology.upper() == "MESH":
                cmd_1 = ["mv", ontology_path, ontology_path + ".zip"]
                subprocess.run(cmd_1)
                cmd_2 = ["unzip", ontology_path + ".zip", "-d", ontology_path + "_dir"]
                subprocess.run(cmd_2)
                cmd_3 = ["mv", ontology_path + "_dir/MESH.ttl", ontology_path]
                subprocess.run(cmd_3)
        else:
            logger.info("found {0} at {1}".format(ontology.lower(), ontology_path))
    return ontology_paths


def get_name_from_curie(curie: str, name_maps: dict):
    """map curies back to name"""
    ids = curie.split(":")
    identfier = ids[-1]
    prefix = normalize_prefix(ids[-2].replace("#", ""))
    try:
        return name_maps[prefix][identfier]
    except:
        return "NO_NAME_FOUND"


def get_name_maps(resources: dict, additional_namespaces: dict = None, **_):
    if additional_namespaces is not None:
        resources = resources | additional_namespaces
    name_maps = {}
    for prefix in resources:
        prefix_n = normalize_prefix(prefix)
        prefix_id_map = get_id_name_mapping(
            prefix=prefix_n,
            version=resources[prefix]["version"],
        )
        ## if can not find a name mapping check for an ordo one
        try:
            if len(prefix_id_map) == 0:
                prefix_id_map = get_id_name_mapping(
                    prefix=prefix_n + ".ordo", version=resources[prefix]["version"]
                )
        except:
            pass
        name_maps[prefix_n] = prefix_id_map
    return name_maps


def parse_identifier(x):
    part_one, part_two = x.split("/")[-2:]
    res = part_two.replace("_", ":").split(":")
    if len(res) == 2:
        curie = f"{res[-2]}:{res[-1]}"
    elif len(res) > 2:
        work = res[-3].strip("#").lower()
        if work == res[-2].lower():
            curie = f"{res[-2]}:{res[-1]}"
        else:
            curie = f"{res[-2].lower()}.{work}:{res[-1]}"
    else:
        curie = f"{part_one}:{res[-1]}"
    return bioregistry.normalize_curie(curie)


def format_mappings(
    df: pl.DataFrame,
    source_prefix: str,
    target_prefix: str,
    matching_source: str,
    resources: dict,
    additional_namespaces: dict = None,
    undirected: bool = False,
    only_mapping_cols: bool = True,
    relation: str = "skos:exactMatch",
    match_type: str = "semapv:SemanticSimilarityThresholdMatching",
):
    """formats a polars dataframe of mappings for use in biomapings"""
    df = df.with_columns(
        pl.lit(relation).alias("relation"),
        pl.lit(match_type).alias("type"),
        pl.lit(matching_source).alias("source"),
        # pl.lit(match_type).alias("prediction_type"),
        # pl.lit(matching_source).alias("prediction_source"),
        pl.col("Score").alias("confidence"),
    )
    df = df.with_columns(
        pl.col("SrcEntity")
        .map_elements(parse_identifier, return_dtype=pl.String)
        .alias("source identifier"),
        pl.col("TgtEntity")
        .map_elements(parse_identifier, return_dtype=pl.String)
        .alias("target identifier"),
    )
    name_maps = get_name_maps(
        resources=resources, additional_namespaces=additional_namespaces
    )
    name_map_func = lambda x: get_name_from_curie(x, name_maps=name_maps)
    df = df.with_columns(
        pl.col("source identifier")
        .map_elements(name_map_func, return_dtype=pl.String)
        .alias("source name"),
        pl.col("target identifier")
        .map_elements(name_map_func, return_dtype=pl.String)
        .alias("target name"),
    ).with_columns(
        pl.col("source identifier").str.split(":").list.get(0).alias("source prefix"),
        pl.col("target identifier").str.split(":").list.get(0).alias("target prefix"),
    )
    df = (
        df
        if not only_mapping_cols
        else df.select(
            [
                "source prefix",
                "source identifier",
                "source name",
                "relation",
                "target prefix",
                "target identifier",
                "target name",
                "type",
                # "prediction_type",
                "confidence",
                "source",
                # "prediction_source",
            ]
        )
    )
    if undirected:
        df = make_undirected(df)
    return df


def make_undirected(df):
    """add rows to a df going in the reverse direction"""
    reversed_df = df.clone()
    # Rename the source and target columns need the spaces in case there is a source column
    rename_dict = {
        x: x.replace("source ", "target ")
        for x in df.columns
        if x.startswith("source ")
    } | {
        x: x.replace("target ", "source ")
        for x in df.columns
        if x.startswith("target ")
    }
    reversed_df = reversed_df.rename(rename_dict).select(df.columns)
    return pl.concat([df, reversed_df]).unique()


def get_landscape_mappings(landscape_name: str):
    landscape_urls = {
        "disease": "https://zenodo.org/records/15164180/files/processed.sssom.tsv?download=1"
    }
    output_name = os.path.join(
        os.getcwd(), "resources", f"semra_{landscape_name}_landscape_mappings.tsv"
    )
    cmd = ["wget", "-O", output_name, landscape_urls[landscape_name]]
    subprocess.run(cmd)


def sssom_to_biomappings(
    df, resources: dict = None, additional_namespaces: dict = None
):
    """
    convert sssom formated df to a df in biomappings format
    """
    df = df.with_columns(
        pl.col("subject_id").str.split(":").list.get(0).alias("source prefix"),
        pl.col("object_id").str.split(":").list.get(0).alias("target prefix"),
    )
    if "subject_label" not in df.columns:
        name_maps = get_name_maps(
            resources=resources, additional_namespaces=additional_namespaces
        )
        name_map_func = lambda x: get_name_from_curie(x, name_maps=name_maps)
        df = df.with_columns(
            pl.col("subject_id")
            .map_elements(name_map_func, return_dtype=pl.String)
            .alias("subject_label"),
            pl.col("object_id")
            .map_elements(name_map_func, return_dtype=pl.String)
            .alias("object_label"),
        )
    return df.rename(
        {
            "subject_id": "source identifier",
            "subject_label": "source name",
            "object_id": "target identifier",
            "object_label": "target name",
        }
    ).select(
        [
            "source identifier",
            "source name",
            "source prefix",
            "target identifier",
            "target name",
            "target prefix",
        ]
    )


def biomappings_to_sssom(
    df, resources: dict = None, additional_namespaces: dict = None
):
    """
    convert biommaings formated df to a df in sssom format
    """

    df = df.with_columns(
        pl.struct("source prefix", "source identifier")
        .map_elements(
            lambda x: normalize_curie(f"{x['source prefix']}:{x['source identifier']}"),
            return_dtype=pl.String,
        )
        .alias("subject_id"),
        pl.struct("target prefix", "target identifier")
        .map_elements(
            lambda x: normalize_curie(f"{x['target prefix']}:{x['target identifier']}"),
            return_dtype=pl.String,
        )
        .alias("object_id"),
    )
    if "source name" not in df.columns:
        name_maps = get_name_maps(
            resources=resources, additional_namespaces=additional_namespaces
        )
        name_map_func = lambda x: get_name_from_curie(x, name_maps=name_maps)
        df = df.with_columns(
            pl.col("subject_id")
            .map_elements(name_map_func, return_dtype=pl.String)
            .alias("source name"),
            pl.col("object_id")
            .map_elements(name_map_func, return_dtype=pl.String)
            .alias("target name"),
        )
    return df.rename(
        {
            "source name": "subject_label",
            "target name": "object_label",
        }
    ).select(
        [
            "subject_id",
            "subject_label",
            "object_id",
            "object_label",
        ]
    )


def load_config_from_json(config_path: str):
    with open(config_path, "r") as f:
        config = json.load(f)
    return config


def top_k_named_relations(
    G, source, name_map_func, k: int = 3, max_distance: int = 3, descendants=False
):
    """Returns a list of the top k ancestors or descendants for a given graph and source"""
    if source not in G.nodes:
        return [], []
    candidates = [
        child
        for _, child in nx.bfs_edges(
            G, source, reverse=not descendants, depth_limit=max_distance
        )
    ]
    curies = []
    names = []
    added = 0
    for candidate_curie in candidates:
        candidate_name = name_map_func(candidate_curie)
        if candidate_name != "NO_NAME_FOUND":
            added += 1
            curies.append(candidate_curie)
            names.append(candidate_name)
        if added == k:
            break
    return curies, names


def descendants_within_distance(G, source, max_distance: int = None):
    """get all  of a node in a directed graph up a max distance"""
    return {
        child
        for _, child in nx.bfs_edges(G, source, reverse=False, depth_limit=max_distance)
    }


def ancestors_within_distance(G, source, max_distance: int = None):
    """get all ancestors of a node in a directed graph up a max distance"""
    return {
        child
        for _, child in nx.bfs_edges(G, source, reverse=True, depth_limit=max_distance)
    }


def normalized_edit_similarity(x):
    """
    calculate the normalized edit similarity for all target and source class names
    """
    return levenshtein.normalized_similarity(
        x["source name"].upper(), x["target name"].upper()
    )


def file_safety_check(pth: str, auto=True, dir_mode: bool = None):
    """
    confirms that a desired output file can be written.
    If a file is already there, check with user if they want to remove it.
    if yes delete the file and continue
    otherwise exit
    args:
        pth : path to output directory or file
        dir_mode : if looking at a directory
        auto : if to detect if file or directory automatically
    """
    if dir_mode is None:
        auto = True
    if auto:
        dir_mode = os.path.isdir(pth) or (not os.path.splitext(pth)[1])
    ## if file does not exist, make the directory
    if not os.path.exists(pth):
        if dir_mode:
            os.makedirs(pth, exist_ok=True)
        else:
            os.makedirs(os.path.dirname(pth), exist_ok=True)
        return
    ## otherwise ask confirm it should be removed
    else:
        resp = input(
            f"Output file {pth} already exists, would you like to overwrite it? (y/n): "
        )
        resp = resp.lower()
        if resp == "y":
            if dir_mode:
                subprocess.run(["rm", "-r", pth])
            else:
                os.remove(pth)
        elif resp == "n":
            logger.info(
                f"keeping {pth}, exiting program. Please either change output path or move existing file"
            )
            sys.exit(1)
        else:
            logger.error(f"Unrecognized response {resp}. Please respond either Y or N")
            sys.exit(1)
