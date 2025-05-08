"""Utility functions for matching with logmap"""

import shlex
import subprocess
import os
from itertools import combinations


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
    ## define the java command to run
    java_cmd = [
        "java",
        "-jar",
        "-Xmx32g",
        "--add-opens",
        "java.base/java.lang=ALL-UNNAMED",
        "/package/logmap/logmap-matcher-4.0.jar",
        "MATCHER",
        f"file:///package/resources/{shlex.quote(target_onto_file)}",
        f"file:///package/resources/{shlex.quote(source_onto_file)}",
        "/package/output/",
        "false",  # classify input ontology as well as map
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
        os.makedirs(logmap_args["output_path"], exist_ok=True)
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
        print(
            f"Matching {logmap_arg['source_def']['prefix']} and {logmap_arg['target_def']['prefix']} with logmap"
        )
        run_logmap(**logmap_arg)
