"""
helper methods for using Robot when parsing and filtering ontologies
TODO: use that ROBOT_COMMAND as the base writing wrapper methods using ROBOT for ontologies.
ideally we are going to want helper functions for converting formats, extracting, and merging
try doing this with check output and check call I think
"""

from bioontologies.robot import ROBOT_COMMAND
from subprocess import check_call
from shlex import quote
import os
from bioregistry import get_iri


prefix_map = prefix_map = {
    "mesh": "http://id.nlm.nih.gov/mesh/",
}


def convert_onto_format(input_file: str, desired_format: str, output_path: str = None):
    """use robot to convert an ontology from one format to another"""
    cmd = ROBOT_COMMAND + ["convert"]
    cmd += ["--input", quote(input_file)]
    desired_format = (
        desired_format if desired_format.startswith(".") else "." + desired_format
    )
    output_path = output_path or os.path.join(
        os.path.dirname(input_file),
        os.path.splitext(os.path.basename(input_file))[0] + desired_format,
    )
    cmd += ["--output", output_path]
    cmd += ["--check", "false"]
    return check_call(cmd)


def get_directionial_onto_subset(
    prefix: str,
    onto_path: str,
    subset_identifiers: list,
    ancestors: bool = False,
    output_path: str = None,
):
    """returns a subset with all descendant (or ancestors if ancestor=True) terms of a list of terms in a given ontology"""
    cmd = ROBOT_COMMAND + ["extract", "--method", "MIREOT"]
    cmd += ["--input", quote(onto_path)]
    subset = "descendants" if not ancestors else "ancestors"
    subset_arg = "--branch-from-term" if not ancestors else "--lower-term"
    output_path = output_path or os.path.join(
        os.path.dirname(onto_path), subset + "_" + os.path.basename(onto_path)
    )
    cmd += ["--output", output_path]
    for term in subset_identifiers:
        cmd += [subset_arg, get_iri(prefix, term, prefix_map=prefix_map)]
    cmd += ["-vvv"]
    print("running", cmd)
    check_call(cmd)


def merge_ontos(output_path: str, input_ontos: list):
    """merges a set of ontologies into one combined file"""
    cmd = ROBOT_COMMAND + ["merge"]
    for onto in input_ontos:
        cmd += ["--input", onto]
    cmd += ["--output", output_path]
    print("running", cmd)
    return check_call(cmd)


def get_onto_subset(
    prefix: str,
    onto_path: str,
    subset_identifiers: list,
    method: str = "full",
    output_path: str = None,
):
    """returns a subset with all descendant (or ancestors if ancestor=True) terms of a list of terms in a given ontology"""
    assert method in ["ancestor", "descendant", "full"]
    if method == "ancestor":
        return get_directionial_onto_subset(
            prefix=prefix,
            onto_path=onto_path,
            subset_identifiers=subset_identifiers,
            ancestors=True,
        )
    elif method == "descendant":
        return get_directionial_onto_subset(
            prefix=prefix,
            onto_path=onto_path,
            subset_identifiers=subset_identifiers,
            ancestors=False,
        )
    else:
        ## get the subets in both directions and merge them
        get_directionial_onto_subset(
            prefix=prefix,
            onto_path=onto_path,
            subset_identifiers=subset_identifiers,
            ancestors=True,
        )
        get_directionial_onto_subset(
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
                "ancestors" + "_" + os.path.basename(onto_path),
            ),
            os.path.join(
                os.path.dirname(onto_path),
                "descendants" + "_" + os.path.basename(onto_path),
            ),
        ]
        return merge_ontos(output_path=output_path, input_ontos=input_ontos)


if __name__ == "__main__":
    print(ROBOT_COMMAND)
