"""
helper methods for using Robot when parsing and filtering ontologies
"""

from bioontologies.robot import ROBOT_COMMAND
from subprocess import check_call
from shlex import quote
import os
from bioregistry import get_iri, normalize_prefix

# override bioregistry mesh map
prefix_map = {
    "mesh": "http://id.nlm.nih.gov/mesh/",
}

SKIP_CHECK = ["EFO"]


def convert_onto_format(input_file: str, desired_format: str, output_path: str = None):
    """use robot to convert an ontology from one format to another"""
    desired_format = (
        desired_format if desired_format.startswith(".") else "." + desired_format
    )
    output_path = output_path or os.path.join(
        os.path.dirname(input_file),
        os.path.splitext(os.path.basename(input_file))[0] + desired_format,
    )
    cmd = ROBOT_COMMAND + [
        "convert",
        "--input",
        quote(input_file),
        "--output",
        output_path,
        "--check",
        "false",
        "-vvv",
    ]
    print(cmd)
    return check_call(cmd)


def get_directional_onto_subset(
    prefix: str,
    onto_path: str,
    subset_identifiers: list,
    ancestors: bool = False,
    output_path: str = None,
    verbose: bool = False,
):
    """returns a subset with all descendant (or ancestors if ancestor=True) terms of a list of terms in a given ontology"""
    subset = "descendants" if not ancestors else "ancestors"
    subset_arg = "--branch-from-term" if not ancestors else "--lower-term"
    output_path = output_path or os.path.join(
        os.path.dirname(onto_path),
        subset + "_" + os.path.splitext(os.path.basename(onto_path))[0] + ".owl",
    )
    cmd = ROBOT_COMMAND + [
        "extract",
        "--method",
        "MIREOT",
        "--input",
        quote(onto_path),
        "--output",
        output_path,
    ]
    for term in subset_identifiers:
        cmd += [subset_arg, get_iri(prefix, term, prefix_map=prefix_map)]
    if verbose:
        cmd += ["-vvv"]
    print("running", cmd)
    check_call(cmd)


def merge_ontos(output_path: str, input_ontos: list, delete_inputs: bool = False):
    """merges a set of ontologies into one combined file"""
    cmd = ROBOT_COMMAND + ["merge"]
    for onto in input_ontos:
        cmd += ["--input", onto]
    cmd += ["--output", output_path]
    print("running", cmd)
    if delete_inputs:
        clean_cmd = ["rm"] + [onto for onto in input_ontos]
        print(clean_cmd)
        check_call(cmd)
        return check_call(clean_cmd)
    return check_call(cmd)


def get_onto_subset_from_file(
    prefix: str,
    onto_path: str,
    subset_identifiers: list,
    method: str = "full",
    output_path: str = None,
    verbose: bool = False,
):
    """returns a subset with all descendant (or ancestors if ancestor=True) terms of a list of terms in a given ontology from an obo file"""
    assert method in ["ancestor", "descendant", "full"]
    if method == "ancestor":
        return get_directional_onto_subset(
            verbose=verbose,
            prefix=prefix,
            onto_path=onto_path,
            subset_identifiers=subset_identifiers,
            ancestors=True,
        )
    elif method == "descendant":
        return get_directional_onto_subset(
            verbose=verbose,
            prefix=prefix,
            onto_path=onto_path,
            subset_identifiers=subset_identifiers,
            ancestors=False,
        )
    else:
        ## get the subsets in both directions and merge them
        get_directional_onto_subset(
            verbose=verbose,
            prefix=prefix,
            onto_path=onto_path,
            subset_identifiers=subset_identifiers,
            ancestors=True,
        )
        get_directional_onto_subset(
            verbose=verbose,
            prefix=prefix,
            onto_path=onto_path,
            subset_identifiers=subset_identifiers,
            ancestors=False,
        )
        output_path = output_path or os.path.join(
            os.path.dirname(onto_path),
            "full_subset" + "_" + os.path.basename(onto_path),
        )
        input_ontos = [
            os.path.join(
                os.path.dirname(onto_path),
                "ancestors"
                + "_"
                + os.path.splitext(os.path.basename(onto_path))[0]
                + ".owl",
            ),
            os.path.join(
                os.path.dirname(onto_path),
                "descendants"
                + "_"
                + os.path.splitext(os.path.basename(onto_path))[0]
                + ".owl",
            ),
            os.path.join(
                os.path.dirname(onto_path),
                "full_subset"
                + "_"
                + os.path.splitext(os.path.basename(onto_path))[0]
                + ".owl",
            ),
        ]
        merge_ontos(
            output_path=input_ontos[2], input_ontos=input_ontos[:2], delete_inputs=True
        )
        convert_onto_format(
            input_file=input_ontos[2], output_path=output_path, desired_format=".obo"
        )
        cmd = ["rm", quote(input_ontos[2])]
        return check_call(cmd)


def get_onto_subset(
    prefix: str, dataset_def: dict, method: str = "full", verbose: bool = True
):
    """returns a subset with all descendant (or ancestors if ancestor=True) terms of a list of terms in a given ontology"""
    assert method in ["ancestor", "descendant", "full"]
    version = dataset_def["resources"][prefix]["version"]
    subset_identifiers = dataset_def["resources"][prefix]["subset_identifiers"]
    save_dir = os.path.join(
        dataset_def["meta"]["dataset_dir"],
        prefix,
        version,
        dataset_def["meta"]["subset_dir"],
    )
    onto_paths = [
        os.path.join(
            dataset_def["meta"]["dataset_dir"], prefix, version, prefix + ".obo"
        ),
        os.path.join(save_dir, "ancestors" + "_" + prefix + ".owl"),
        os.path.join(save_dir, "descendant" + "_" + prefix + ".owl"),
        os.path.join(save_dir, prefix + ".owl"),
        os.path.join(save_dir, prefix + ".obo"),
    ]
    if os.path.exists(onto_paths[4]):
        print(
            f"{prefix} version {version} subset named {dataset_def['meta']['subset_dir']} already exists at {onto_paths[4]}, delete it if you want to recreate"
        )
        return 1
    if method == "ancestor":
        return get_directional_onto_subset(
            verbose=verbose,
            prefix=prefix,
            onto_path=onto_paths[0],
            subset_identifiers=subset_identifiers,
            ancestors=True,
            output_path=onto_paths[1],
        )
    elif method == "descendant":
        return get_directional_onto_subset(
            verbose=verbose,
            prefix=prefix,
            onto_path=onto_paths[0],
            subset_identifiers=subset_identifiers,
            ancestors=False,
            output_path=onto_paths[2],
        )
    else:
        ## get the subets in both directions and merge them
        get_directional_onto_subset(
            verbose=verbose,
            prefix=prefix,
            onto_path=onto_paths[0],
            subset_identifiers=subset_identifiers,
            ancestors=True,
            output_path=onto_paths[1],
        )
        get_directional_onto_subset(
            verbose=verbose,
            prefix=prefix,
            onto_path=onto_paths[0],
            subset_identifiers=subset_identifiers,
            ancestors=False,
            output_path=onto_paths[2],
        )
        merge_ontos(
            output_path=onto_paths[3], input_ontos=onto_paths[1:3], delete_inputs=True
        )
        convert_onto_format(
            input_file=onto_paths[3], output_path=onto_paths[4], desired_format=".obo"
        )
        cmd = ["rm", quote(onto_paths[3])]
        return check_call(cmd)


def get_onto_subsets(dataset_def: dict, method: str = "full", verbose: bool = False):
    """returns a subset with all descendant (or ancestors if ancestor=True) terms of a list of terms for a set of ontologies"""
    assert method in ["ancestor", "descendant", "full"]
    version_mappings = {
        normalize_prefix(prefix): dataset_def["resources"][prefix]
        for prefix in dataset_def["resources"]
    }
    dataset_def["resources"] = version_mappings
    for prefix in dataset_def["resources"]:
        if dataset_def["resources"][prefix]["subset"]:
            print(f"sub-setting {prefix}")
            get_onto_subset(
                prefix=prefix, dataset_def=dataset_def, method=method, verbose=verbose
            )
