import datetime
import subprocess
import os
from pyobo import get_id_name_mapping
from bioregistry import parse_curie, normalize_prefix
from bioregistry.resolve import get_owl_download
import polars as pl
from typing import Callable


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
            print("Downloading {0}".format(ontology))
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
            print("found {0} at {1}".format(ontology.lower(), ontology_path))
    return ontology_paths
def get_name_from_curie(curie:str, name_maps:dict):
    """map curies back to name"""
    curie = curie.replace("#ORDO:", '')
    curie = curie .replace("_", ":")
    ids = curie.split(":")
    if len(ids) == 1:
        identfier = ids[0]
        prefix = 'mesh'
    else:
        identfier = ids[-1]
        prefix = normalize_prefix(ids[-2].replace("#", ''))
    try:
        return name_maps[prefix][identfier]
    except:
        print(curie, identfier, prefix)
        return 'NO_NAME_FOUND'
def get_name_maps(resources:dict):
    name_maps = {}
    for prefix in resources:
        prefix_n = normalize_prefix(prefix)
        prefix_id_map = get_id_name_mapping(prefix = prefix_n, version = resources[prefix]['version'])
        if len(prefix_id_map)==0:
            prefix_id_map = get_id_name_mapping(prefix = prefix_n+'.ordo', version = resources[prefix]['version'])
        name_maps[prefix_n] = prefix_id_map

    name_maps['hp'] = get_id_name_mapping(prefix = 'hp')
    return name_maps
        


def format_mappings(
    df: pl.DataFrame,
    source_prefix: str,
    target_prefix: str,
    matching_source: str,
    resources:dict, 
    only_mapping_cols: bool = True,
    relation: str = "skos:exactMatch",
    match_type: str = "semapv:SemanticSimilarityThresholdMatching",
):
    """formats a polars dataframe of mappings for use in biomapings"""
    df = df.with_columns(
        pl.col("SrcEntity")
        .str.split("/")
        .list.get(-1)
        .str.replace("_", ":")
        .alias("source identifier"),
        pl.col("TgtEntity")
        .str.split("/")
        .list.get(-1)
        .str.replace("_", ":")
        .alias("target identifier"),
        pl.lit(source_prefix.upper()).alias("source prefix"),
        pl.lit(target_prefix.upper()).alias("target prefix"),
        pl.lit(relation).alias("relation"),
        pl.lit(match_type).alias("type"),
        pl.lit(matching_source).alias("source"),
        pl.col("Score").alias("confidence"),
    )
    name_maps = get_name_maps(resources=resources)
    name_map_func = lambda x: get_name_from_curie(x, name_maps = name_maps)
    # try:
    df = df.with_columns(
    pl.col("source identifier")
    .map_elements(name_map_func, return_dtype=pl.String)
    .alias("source name"),
    pl.col("target identifier")
    .map_elements(name_map_func, return_dtype=pl.String)
    .alias("target name"),
)
    # except:
    #     import ipdb; ipdb.set_trace()
    return (
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
                "confidence",
                "source",
            ]
        )
    )
