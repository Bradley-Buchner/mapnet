"""
Methods for working with obo files.
"""

import pyobo
import networkx as nx
import os
import bioregistry
from shutil import copyfile
from pyobo.utils.path import prefix_directory_join
import polars as pl
from mapnet.utils import get_name_maps, get_name_from_curie
from mapnet.utils.utils import sssom_to_biomappings
import logging 

logger = logging.getLogger(__name__)

def download_raw_obo_files(dataset_def: dict, save_mappings: bool = True):
    """download raw obo files for a set of resources"""
    version_mappings = {
        bioregistry.normalize_prefix(prefix): dataset_def["resources"][prefix]
        for prefix in dataset_def["resources"]
    }
    if "dataset_dir" in dataset_def["meta"]:
        resource_path = dataset_def["meta"]["dataset_dir"]
    else:
        resource_path = "resources/"
    for prefix in version_mappings:
        prefix = bioregistry.normalize_prefix(prefix)
        save_dir = os.path.join(
            resource_path, prefix, version_mappings[prefix]["version"]
        )
        resource_fname = os.path.join(save_dir, prefix + ".obo")
        if not os.path.exists(resource_fname):
            os.makedirs(save_dir, exist_ok=True)
            logger.info(
                f"downloading {prefix}, version {version_mappings[prefix]['version']}"
            )
            ## check if a .obo file is already cached
            pyobo_dir = prefix_directory_join(
                prefix=prefix,
                version=version_mappings[prefix]["version"],
                ensure_exists=False,
            )
            if os.path.exists(pyobo_dir):
                src_file = [x for x in os.listdir(pyobo_dir) if x.endswith(".obo")]
                if len(src_file) != 0:
                    src_file = os.path.join(pyobo_dir, src_file[0])
                    logger.info(f"copying cached file from {src_file}")
                    copyfile(src=src_file, dst=resource_fname)
                else:
                    logger.info("explicitly writing to obo")
                    onto = pyobo.get_ontology(
                        prefix=prefix,
                        version=version_mappings[prefix]["version"],
                    )
                    # explicitly save the obo files for easy access
                    onto.write_obo(resource_fname)
            else:
                logger.info("explicitly writing to obo")
                onto = pyobo.get_ontology(
                    prefix=prefix,
                    version=version_mappings[prefix]["version"],
                )
                # explicitly save the obo files for easy access
                onto.write_obo(resource_fname)
        else:
            logger.info(
                f"{prefix}, version {version_mappings[prefix]['version']} already present at {resource_fname}"
            )
        if save_mappings:
            write_mappings(
                resource_fname=resource_fname,
                prefix=prefix,
                version=version_mappings[prefix]["version"],
            )


def write_mappings(resource_fname: str, prefix: str, version: str):
    """
    extract and save the mappings from a .obo file.
    """

    save_path = os.path.join(os.path.dirname(resource_fname), "mappings.tsv")
    if not os.path.exists(save_path):
        onto = pyobo.from_obo_path(resource_fname, prefix=prefix, version=version)
        mappings_df = onto.get_mappings_df()
        mappings_df.to_csv(save_path, sep="\t", index=False)
    else:
        logger.info(f"mappings already saved at {save_path}")


def subset_graph(full_graph: pyobo.Obo, subset_identifiers: list):
    """takes a default obo and outputs the network graph of a specfied subset subset. This will take both the ancestors and descendants of the class
    if no subset is specfied will just return the original graph"""
    if len(subset_identifiers) == 0:
        return full_graph
    relations = set()
    for ref in subset_identifiers:
        relations = (
            relations
            | set(nx.ancestors(full_graph, ref))
            | {ref}
            | set(nx.descendants(full_graph, ref))
        )
    subset_graph = full_graph.subgraph(relations).copy()
    return subset_graph


def subset_graph_to_obo(subset_graph: nx.DiGraph, prefix: str, version: str):
    """takes the subset network graph and writes it to an OBO"""
    subset_graph.graph["ontology"] = prefix
    subset_version = f"{prefix}_{version}_subset"
    subset_obo = pyobo.from_obonet(graph=subset_graph, version=subset_version)
    subset_obo.write_obo(f"resources/{subset_version}.obo")
    return subset_obo


def subset_from_obo(subset_def: dict):
    """saves an OBO subset of a graph given a base prefix and version as well as terms to base subset on"""
    for prefix in subset_def:
        version = subset_def[prefix]["version"]
        subset_identifiers = subset_def[prefix]["subset_identifiers"]
        logger.info("loading obo")
        obo = pyobo.get_ontology(prefix=prefix, version=version)
        logger.info("loading graph")
        full_graph = obo.get_graph().get_networkx()
        logger.info("subseting graph")
        sg = subset_graph(full_graph=full_graph, subset_identifiers=subset_identifiers)
        logger.info("writing res")
        subset_obo = subset_graph_to_obo(
            subset_graph=sg, prefix=prefix, version=version
        )
        logger.info(f"The full {prefix} graph has {len(obo.get_ids())} term")
        logger.info(f"The {prefix} subset has {len(subset_obo.get_ids())} term")
        logger.info("-" * 50)


def format_known_mappings(
    resource_fname: str,
    resources: dict,
    additional_namespaces: dict = None,
    sssom: bool = True,
):
    """helper method for formatting a dataframe with known_mappings"""
    if additional_namespaces:
        normalized_resource_names = [
            bioregistry.normalize_prefix(x) for x in resources | additional_namespaces
        ]
    else:
        normalized_resource_names = [bioregistry.normalize_prefix(x) for x in resources]
    name_maps = get_name_maps(
        resources=resources, additional_namespaces=additional_namespaces
    )
    df = pl.read_csv(resource_fname, separator="\t")
    if len(df) > 0:
        df = df.with_columns(
            pl.col("subject_id")
            .map_elements(bioregistry.normalize_curie, return_dtype=pl.String)
            .alias("subject_id"),
            pl.col("object_id")
            .map_elements(bioregistry.normalize_curie, return_dtype=pl.String)
            .alias("object_id"),
        ).with_columns(
            pl.col("subject_id").str.split(":").list.get(0).alias("subject_prefix"),
            pl.col("object_id").str.split(":").list.get(0).alias("object_prefix"),
        )
        df = df.filter(
            (pl.col("subject_prefix").is_in(normalized_resource_names))
            & (pl.col("object_prefix").is_in(normalized_resource_names))
            & (pl.col("predicate_id").str.contains(r"Xref"))
        )
        if sssom:
            return df
        else:
            return sssom_to_biomappings(
                df, resources=resources, additional_namespaces=additional_namespaces
            )


def load_known_mappings_df(
    resources: dict,
    meta: dict,
    additional_namespaces: dict = None,
    sssom: bool = True,
    **_,
):
    """
    get the known mappings for a set of resources
    """
    version_mappings = {
        bioregistry.normalize_prefix(prefix): resources[prefix] for prefix in resources
    }
    if "dataset_dir" in meta:
        resource_path = meta["dataset_dir"]
    else:
        resource_path = "resources/"
    full_mappings_df = None
    for prefix in version_mappings:
        save_dir = os.path.join(
            resource_path, prefix, version_mappings[prefix]["version"]
        )
        resource_fname = os.path.join(save_dir, "mappings.tsv")
        mappings_df = format_known_mappings(
            resource_fname=resource_fname,
            resources=resources,
            additional_namespaces=additional_namespaces,
            sssom=sssom,
        )
        if mappings_df is None:
            continue
        if full_mappings_df is None:
            full_mappings_df = mappings_df
        else:
            full_mappings_df = full_mappings_df.vstack(mappings_df)
    return full_mappings_df


def normalize_dataset_def(dataset_def):
    """normalize a dataset defention"""
    version_mappings = {
        bioregistry.normalize_prefix(prefix): dataset_def["resources"][prefix]
        for prefix in dataset_def["resources"]
    }
    dataset_def["resources"] = version_mappings
    return dataset_def
