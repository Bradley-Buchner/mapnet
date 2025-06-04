"""Utility functions for matching with logmap"""

import shlex
import subprocess
import os
from itertools import combinations
from bioregistry import normalize_prefix
import re
import polars as pl
from mapnet.utils import format_mappings


def build_image(tag: str = "0.01", **_):
    """build the logmap image with a specfied tag"""
    cmd = [
        "docker",
        "build",
        "-f",
        "mapnet/logmap/container/Dockerfile",
        "./",
        "-t",
        f"logmap:{shlex.quote(tag)}",
    ]
    print(f"running, {cmd}")
    subprocess.check_call(cmd)


def run_logmap(
    target_onto_file: str = None,
    source_onto_file: str = None,
    output_path: str = None,
    dataset_dir: str = "resources/",
    tag: str = "0.01",
    target_def: dict = None,
    source_def: dict = None,
    **_,
):
    output_path = output_path or os.path.join(os.getcwd(), "mapnet", "logmap", "output")
    if target_onto_file == None:
        if target_def == None:
            raise ValueError("must define either target_onto_file or target_def")
        else:
            target_name = (
                target_def["prefix"] + ".obo"
                if not target_def["subset"]
                else os.path.join(
                    target_def["subset_name"], target_def["prefix"] + ".obo"
                )
            )
            target_onto_file = os.path.join(
                target_def["prefix"], target_def["version"], target_name
            )
    if source_onto_file == None:
        if source_def == None:
            raise ValueError("must define either source_onto_file or source_def")
        else:
            source_name = (
                source_def["prefix"] + ".obo"
                if not source_def["subset"]
                else os.path.join(
                    source_def["subset_name"], source_def["prefix"] + ".obo"
                )
            )
            source_onto_file = os.path.join(
                source_def["prefix"], source_def["version"], source_name
            )
    # ## define the java command to run
    java_cmd = [
        "java",
        "-jar",
        "-Xmx32g",
        "--add-opens",
        "java.base/java.lang=ALL-UNNAMED",
        "/package/logmap-matcher-4.0.jar",
        "MATCHER",
        f"file:///package/resources/{shlex.quote(target_onto_file)}",
        f"file:///package/resources/{shlex.quote(source_onto_file)}",
        "/package/output/",
        "true",  # classify input ontology as well as map
    ]
    ##  define the command to run
    cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{shlex.quote(output_path)}:/package/output",  # Mount where output will be written
        "-v",
        f"{shlex.quote(dataset_dir)}:/package/resources/",  ## mount directory with resources
        f"logmap:{shlex.quote(tag)}",  ## specify the image and tag
        "sh",
        "-c",
        shlex.join(java_cmd),  ## add the java command as a string
    ]
    print(cmd)
    # run the command
    subprocess.check_call(cmd)


def logmap_arg_factory(
    analysis_name: str,
    resources: dict,
    meta: dict,
    tag: str,
    dataset_dir: str = None,
    output_dir: str = None,
    **_,
):
    """returns a generator of args for running logmap pairwise on a dataset"""
    if dataset_dir is not None:
        dataset_dir = dataset_dir
    elif "dataset_dir" in meta:
        dataset_dir = meta["dataset_dir"]
    else:
        dataset_dir = os.path.join(os.getcwd(), "resources")
    if output_dir is not None:
        output_dir = output_dir
    elif "output_dir" in meta:
        output_dir = meta["output_dir"]
    else:
        output_dir = os.path.join(os.getcwd(), "output", "logmap", analysis_name)
        os.makedirs(output_dir, exist_ok=True)
    logmap_args = {"tag": tag, "dataset_dir": dataset_dir}
    for source, target in combinations(resources, r=2):
        logmap_args["output_path"] = os.path.join(output_dir, f"{source}-{target}")
        logmap_args["source_def"] = {
            "prefix": source,
            "version": resources[source]["version"],
            "subset": resources[source]["subset"],
            "subset_name": meta["subset_dir"],
        }
        logmap_args["target_def"] = {
            "prefix": target,
            "version": resources[target]["version"],
            "subset": resources[target]["subset"],
            "subset_name": meta["subset_dir"],
        }
        yield logmap_args


def run_logmap_pairwise(
    analysis_name: str,
    resources: dict,
    meta: dict,
    tag: str,
    build: bool = False,
    dataset_dir: str = None,
    output_dir: str = None,
    **_,
):
    """runs logmap pairwise over a set of resources"""
    version_mappings = {
        normalize_prefix(prefix): resources[prefix] for prefix in resources
    }
    resources = version_mappings
    if build:
        print(f"building image with tag {tag}")
        build_image(tag=tag)
    for logmap_arg in logmap_arg_factory(
        analysis_name=analysis_name,
        resources=resources,
        meta=meta,
        tag=tag,
        dataset_dir=dataset_dir,
        output_dir=output_dir,
    ):

        os.makedirs(logmap_arg["output_path"], exist_ok=True)
        run_logmap(**logmap_arg)


def run_logmap_for_target_pairs(
    target_resource_prefix: str,
    analysis_name: str,
    resources: dict,
    meta: dict,
    tag: str,
    build: bool = False,
    dataset_dir: str = None,
    output_dir: str = None,
    **_,
):
    """Runs logmap for all pairs only containing a target resource"""
    version_mappings = {
        normalize_prefix(prefix): resources[prefix] for prefix in resources
    }
    resources = version_mappings
    if build:
        print(f"building image with tag {tag}")
        build_image(tag=tag)
    for logmap_arg in logmap_arg_factory(
        analysis_name=analysis_name,
        resources=resources,
        meta=meta,
        tag=tag,
        dataset_dir=dataset_dir,
        output_dir=output_dir,
    ):
        if (
            logmap_arg["source_def"]["prefix"] == target_resource_prefix
            or logmap_arg["target_def"]["prefix"] == target_resource_prefix
        ):
            os.makedirs(logmap_arg["output_path"], exist_ok=True)
            run_logmap(**logmap_arg)


def walk_logmap_output_dir(
    meta: dict = None,
    analysis_name: str = None,
    output_dir: str = None,
    resources: dict = None,
    **_,
):
    """walk the output directory and get the paths to all matching files"""
    if output_dir is not None:
        output_dir = output_dir
    elif "output_dir" in meta:
        output_dir = meta["output_dir"]
    else:
        output_dir = os.path.join(os.getcwd(), "output", "logmap", analysis_name)
    for root, _, files in os.walk(output_dir):
        if root.endswith("full_analysis"):
            continue
        else:
            for mappings in filter(lambda x: x.endswith("mappings.tsv"), files):
                source, target = root.split("/")[-1].split("-")
                source = normalize_prefix(source)
                target = normalize_prefix(target)
                yield (source, target, os.path.join(root, mappings))


def format_logmap_mappings(
    source_prefix: str,
    target_prefix: str,
    resources: dict,
    mapping_path: str,
    additional_namespaces: dict = None,
):
    """format logmap mappings to be consistent with biomappings"""
    intermediate_representation = pl.read_csv(
        mapping_path,
        separator="\t",
        has_header=False,
        new_columns=["TgtEntity", "SrcEntity", "Score"],
    )
    return format_mappings(
        df=intermediate_representation,
        target_prefix=target_prefix,
        source_prefix=source_prefix,
        matching_source="logmap",
        resources=resources,
        only_mapping_cols=True,
        additional_namespaces=additional_namespaces,
        undirected=True,
    )


def merge_logmap_mappings(
    meta: dict,
    analysis_name: str,
    output_dir: str = None,
    resources: dict = None,
    additional_namespaces: dict = None,
    write_dir: str = None,
    **_,
):
    """
    read in and merge the logmap matching files into one tsv file
    """
    if output_dir is not None:
        output_dir = output_dir
    elif "output_dir" in meta:
        output_dir = meta["output_dir"]
    else:
        output_dir = os.path.join(os.getcwd(), "output", "logmap", analysis_name)
    write_dir = write_dir or os.path.join(
        output_dir,
        "full_analysis",
    )
    os.makedirs(write_dir, exist_ok=True)
    write_path = os.path.join(write_dir, "full_mappings.tsv")

    mapping_df = None
    for source_prefix, target_prefix, mapping_path in walk_logmap_output_dir(
        output_dir=output_dir, resources=resources
    ):
        print(source_prefix, target_prefix, mapping_path)
        print("-" * 40)
        if mapping_df is None:
            mapping_df = format_logmap_mappings(
                source_prefix=source_prefix,
                target_prefix=target_prefix,
                mapping_path=mapping_path,
                resources=resources,
                additional_namespaces=additional_namespaces,
            )
        else:
            mapping_df = mapping_df.vstack(
                format_logmap_mappings(
                    source_prefix=source_prefix,
                    target_prefix=target_prefix,
                    mapping_path=mapping_path,
                    resources=resources,
                    additional_namespaces=additional_namespaces,
                )
            )
    mapping_df = mapping_df.unique()
    ## remove rows that only differ by score, take the max ###
    cols = mapping_df.columns
    cols.remove("confidence")
    mapping_df = mapping_df.group_by(cols).max()
    mapping_df.write_csv(write_path, separator="\t")
    return mapping_df
