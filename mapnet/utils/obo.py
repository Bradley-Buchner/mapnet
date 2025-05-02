"""
Methods for working with obo files.
"""

import pyobo
import networkx as nx


def download_raw_obo_files(version_mappings: dict):
    """download raw obo files for a set of reasources"""
    for prefix in version_mappings:
        res = pyobo.get_id_name_mapping(prefix=prefix, version=version_mappings[prefix])
        if len(res) == 0:
            print(f"Could not get obo for {prefix} version {version_mappings[prefix]}")
        else:
            print(
                f"Sucessfully downloaded obo for {prefix} version {version_mappings[prefix]}"
            )


def subset_graph(full_graph: pyobo.Obo, subset_identifiers: list):
    """takes a default obo and outputs the network graph of a specfied subset subset. This will take both the ancestors and descendants of the class"""
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
