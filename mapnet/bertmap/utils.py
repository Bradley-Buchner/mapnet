"""utility functions for bertmap"""

import os
from deeponto.align.bertmap import DEFAULT_CONFIG_FILE, BERTMapPipeline
from deeponto.onto import Ontology
from huggingface_hub import snapshot_download

# import biomappings
# from biomappings.resources import append_prediction_tuples

from bioregistry import get_iri, normalize_prefix
import polars as pl
from mapnet.utils import load_biomappings_df, load_known_mappings_df

PREFIX_MAP = {
    "mesh": "http://id.nlm.nih.gov/mesh/",
}


def identifier_to_iri(x: str):
    """
    wrapper method for getting iri from identifier
    """
    prefix, identifier = x.split(":")
    return get_iri(prefix=prefix, identifier=identifier, prefix_map=PREFIX_MAP)


def biomappings_format_to_bertmap(df: pl.DataFrame):
    """
    converts a dataframe formatted in the biomappings style to the BertMap style
    """
    return df.with_columns(
        pl.col("source identifier")
        .map_elements(identifier_to_iri, return_dtype=pl.String)
        .alias("SrcEntity"),
        pl.col("target identifier")
        .map_elements(identifier_to_iri, return_dtype=pl.String)
        .alias("TgtEntity"),
        pl.lit(1.0).alias("score"),
    ).select("SrcEntity", "TgtEntity", "score")


def get_known_maps(
    target_def: dict,
    source_def: dict,
    resources: dict,
    meta: dict,
    check_biomappings: bool = True,
    **_,
):
    """
    saves a file with known mappings parsed from the provided obo file, and also biomappings optionally
    """
    evidence = None
    if check_biomappings:
        forward = load_biomappings_df(
            target_prefix=target_def["prefix"],
            source_prefix=source_def["prefix"],
            undirected=True,
        )
        reverse = load_biomappings_df(
            source_prefix=target_def["prefix"],
            target_prefix=source_def["prefix"],
            undirected=True,
        )
        evidence = forward.vstack(reverse).filter(
            (pl.col("source prefix").eq(source_def["prefix"]))
            & (pl.col("target prefix").eq(target_def["prefix"]))
        )
    known_mappings_df = load_known_mappings_df(resources, meta, sssom=False)
    if evidence is None:
        evidence = known_mappings_df
    else:
        evidence = evidence.vstack(known_mappings_df)
    evidence = biomappings_format_to_bertmap(evidence).unique()
    os.makedirs(meta["known_mappings_path"], exist_ok=True)
    save_pth = os.path.join(
        meta["known_mappings_path"],
        f"{source_def['prefix']}-{source_def['version']}-{target_def['prefix']}-{target_def['version']}-known_maps.tsv",
    )
    evidence.write_csv(
        save_pth,
        separator="\t",
    )
    return save_pth


def normalize_resource_def(resource_def: dict = None, resources: dict = None):
    """
    helper function for normalizing the prefix of a resource def
    """
    if resource_def:
        resource_def["prefix"] = normalize_prefix(resource_def.pop("prefix"))
        return resource_def
    else:
        normalized_resources = {}
        for prefix in resources:
            normalized_resources[normalize_prefix(prefix)] = resources[prefix]
        return normalized_resources


def get_resource_file_name(
    resource_def: dict, resource_path: dict, meta: dict = None, prefix: str = None
):
    """
    returns the path to a given resource from its definition and top level directory
    """
    resource_def = resource_def.copy()
    if prefix:
        resource_def["prefix"] = prefix
    if meta:
        resource_def["subset_name"] = meta["subset_dir"]
    resource_dir = os.path.join(
        resource_path, resource_def["prefix"], resource_def["version"]
    )
    resource_name = (
        resource_def["prefix"] + ".obo"
        if not resource_def["subset"]
        else os.path.join(resource_def["subset_name"], resource_def["prefix"] + ".obo")
    )
    return os.path.join(resource_dir, resource_name)


def get_config(
    config_path: str,
    resources: dict,
    meta: dict,
    target_def: dict,
    source_def: dict,
    resource_path: str,
    output_path: str = None,
    global_matching: bool = True,
    use_auxiliary_mappings: bool = False,
):
    """
    defines a configuration.yaml file for bertmap
    """
    if not config_path is None:
        return BERTMapPipeline.load_bertmap_config(config_path)
    config = BERTMapPipeline.load_bertmap_config(DEFAULT_CONFIG_FILE)
    config.output_path = output_path or os.path.join(
        os.getcwd(),
        "output",
        "bertmap",
        meta["landscape"],
        f'{source_def["prefix"]}-{target_def["prefix"]}',
    )
    config.global_matching = global_matching
    ## TODO: need to look into annotation property iris
    if use_auxiliary_mappings:
        config.auxiliary_ontos = [
            get_resource_file_name(
                resource_def=resources[x],
                resource_path=resource_path,
                meta=meta,
                prefix=x,
            )
            for x in resources
            if ((x != target_def["prefix"]) and (x != source_def["prefix"]))
        ]

    return config


def load_bertmap(
    target_def: dict,
    source_def: dict,
    resources: dict,
    meta: dict,
    config_path: str = None,
    check_biomappings: bool = True,
    check_known_mappings: bool = True,
    known_map_path: str = None,
    train_model: bool = False,
    global_matching: bool = True,
    use_auxiliary_mappings: bool = False,
    **_,
):
    """Load in the bertmap model (will download from hugging face if not present in ./bertmap)."""
    if "dataset_dir" in meta:
        resource_path = meta["dataset_dir"]
    else:
        resource_path = "resources/"
    resources = normalize_resource_def(resources=resources)
    source_def = normalize_resource_def(resource_def=source_def)
    target_def = normalize_resource_def(resource_def=target_def)
    config = get_config(
        config_path=config_path,
        resource_path=resource_path,
        resources=resources,
        meta=meta,
        target_def=target_def,
        source_def=source_def,
        global_matching=global_matching,
        use_auxiliary_mappings=use_auxiliary_mappings,
    )
    known_map_path = known_map_path or get_known_maps(
        target_def=target_def,
        source_def=source_def,
        resources=resources,
        meta=meta,
        check_biomappings=check_biomappings,
    )
    config.known_mappings = known_map_path
    if not train_model:
        config.global_matching.enabled = False
        if not os.path.isdir("bertmap"):
            print("downloading model from hugging face")
            snapshot_download(
                repo_id="buzgalbraith/BERTMAP-BioMappings", local_dir="./"
            )
        else:
            print("Model found at bertmap")
    source_fname = get_resource_file_name(
        resource_def=source_def, resource_path=resource_path
    )
    print("loading source onto")
    source_onto = Ontology(source_fname)
    print("loading target onto")
    target_fname = get_resource_file_name(
        resource_def=target_def, resource_path=resource_path
    )
    target_onto = Ontology(target_fname)
    print("loading model")
    return BERTMapPipeline(source_onto, target_onto, config)


# # inference


def bertmap_inference():
    """Run inference on a single pair of ontologies using BertMap."""
