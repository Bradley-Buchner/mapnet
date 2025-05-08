"""
Methods for working with obo files.
"""

import pyobo
import networkx as nx
import os
import bioregistry
from shutil import copyfile
from pyobo.utils.path import prefix_directory_join


def download_raw_obo_files(dataset_def: dict):
    """download raw obo files for a set of resources"""
    version_mappings = dataset_def["resources"]
    if "dataset_dir" in dataset_def["meta"]:
        resource_path = dataset_def["meta"]["dataset_dir"]
    else:
        resource_path = "resources/"
    for prefix in version_mappings:
        save_dir = os.path.join(
            resource_path, prefix, version_mappings[prefix]["version"]
        )
        resource_fname = os.path.join(save_dir, prefix + ".obo")
        print(resource_fname)
        if not os.path.exists(resource_fname):
            os.makedirs(save_dir, exist_ok=True)
            print(
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
                    print(f"copying cached file from {src_file}")
                    copyfile(src=src_file, dst=resource_fname)
            else:
                print("explicitly writing to obo")
                onto = pyobo.get_ontology(
                    prefix=prefix,
                    version=version_mappings[prefix]["version"],
                )
                # explicitly save the obo files for easy access
                onto.write_obo(resource_fname)
        else:
            print(
                f"{prefix}, version {version_mappings[prefix]['version']} already present at {resource_fname}"
            )


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
        print("loading obo")
        obo = pyobo.get_ontology(prefix=prefix, version=version)
        print("loading graph")
        full_graph = obo.get_graph().get_networkx()
        print("subseting graph")
        sg = subset_graph(full_graph=full_graph, subset_identifiers=subset_identifiers)
        print("writing res")
        subset_obo = subset_graph_to_obo(
            subset_graph=sg, prefix=prefix, version=version
        )
        print(f"The full {prefix} graph has {len(obo.get_ids())} term")
        print(f"The {prefix} subset has {len(subset_obo.get_ids())} term")
        print("-" * 50)
