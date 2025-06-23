"""
script to add positive mappings between efo<->chebi from biomappings (more generally can be used to add xrefs from a given ontology to an obo file)
"""

import argparse
import re

import biomappings
import bioregistry


def load_mappings(subject_prefix="efo", object_prefix="chebi"):
    """
    Load EFO-ChEBI mappings from Biomappings.

    Args:
        subject_prefix (str): prefix of ontology to update
        object_prefix (str): prefix of ontology  from which to add xrefs to

    Returns:
        dict: A mapping from subject CURIEs to object CURIEs (as strings).
    """
    mappings = dict()
    for x in biomappings.load_mappings():
        if x["subject_id"].startswith(subject_prefix) and x["object_id"].startswith(
            object_prefix
        ):
            mappings[x["subject_id"]] = x["object_id"]
        elif x["subject_id"].startswith(object_prefix) and x["object_id"].startswith(
            subject_prefix
        ):
            mappings[x["object_id"]] = x["subject_id"]
    return mappings


def insert_xref_into_ontology(onto_path: str, target: str, target_xref: str):
    """
    Iterate through owl file and a given target_xref to a target efo class.

    Args:
        onto_path (str): path to owl file to update
        target (str): current target class
        target_xref (str): xref to add to target class

    Returns:
        None
    """
    ## read in efo file content
    with open(onto_path, mode="r") as f:
        lines = f.readlines()
    ## parse target and target_xref for ease
    target = bioregistry.parse_curie(target)
    target_xref = bioregistry.parse_curie(target_xref)
    ## define regex patterns for parsing file
    start_pattern = rf"^<owl:Class.*{target.prefix.upper()}_{target.identifier}\">$"
    xref_pattern = r"^<oboInOwl:hasDbXref>(\w{1,}:\w{1,})"
    ## define conditions for logic flow and a set for already present xref prefixes
    class_start, to_add, xref_check = False, False, False
    xref_prefixes = set()
    ## loop through the lines of the file
    for i, line in enumerate(lines):
        line = line.strip()
        ## look for start of target class definition
        if not class_start:
            if re.match(start_pattern, line):
                class_start = True
        ## if in target class
        else:
            ## look for xrefs
            xref_match = re.match(xref_pattern, line)
            if xref_match:
                xref_check = True
                grp = xref_match.groups()[0]
                grp = bioregistry.parse_curie(grp)
                if grp:
                    xref_prefixes.add(grp.prefix)
            else:
                ## if class definition over or past last xref
                if line == "</owl:Class>" or xref_check:
                    ## set index to add new xref
                    add_line = i
                    ## if the given class does not already have an xref from the ontology add it
                    if target_xref.prefix not in xref_prefixes:
                        to_add = True
                    break
    ## insert the new xref add the correct position
    if to_add:
        lines.insert(
            add_line,
            f"\t\t<oboInOwl:hasDbXref>{target_xref.prefix.upper()}:{target_xref.identifier}</oboInOwl:hasDbXref>\n",
        )
        ## over write the file
        with open(onto_path, mode="w") as f:
            f.writelines(lines)
        return 1
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--path",
        type=str,
        default="../efo/src/ontology/efo-edit.owl",
        help="path to editable subject owl file.",
    )
    parser.add_argument(
        "-s",
        "--subject-prefix",
        type=str,
        default="efo",
        help="prefix of ontology to update.",
    )
    parser.add_argument(
        "-o",
        "--object-prefix",
        type=str,
        default="chebi",
        help="prefix of ontology to add xrefs for.",
    )
    args = parser.parse_args()
    mappings = load_mappings(
        subject_prefix=args.subject_prefix, object_prefix=args.object_prefix
    )
    print(
        f"Attempting to add {len(mappings)} xrefs from {args.subject_prefix} to {args.object_prefix}..."
    )
    update_count = 0
    for target in mappings:
        update_count += insert_xref_into_ontology(
            onto_path=args.path, target=target, target_xref=mappings[target]
        )
    print(f"COMPLETE\nSummary:")
    print("-" * 50)
    print(
        f"- Successfully added {update_count} xrefs from {args.subject_prefix} to {args.object_prefix}"
    )
    print(
        f"- {len(mappings) - update_count} xrefs could not be added (they may either conflict with existing xrefs or already be present)"
    )
    if update_count > 0:
        print(f"- {args.path} was updated")
    else:
        print(f"- {args.path} was not updated")
    print("-" * 50)
