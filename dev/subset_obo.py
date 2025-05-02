"""Method for subsetting ontologies in the same way as was done in the landscape analysis and saving the ressults.
Produces the same number of classes as the MESH subset mentioned in the Semra DB Paper
"""

import pyobo
import networkx as nx

## define our subsets
config = {
    "mesh": {
        "version": "2025",
        "subset_identifiers": [
            "mesh:D007239",
            "mesh:D001520",
            "mesh:D011579",
            "mesh:D001523",
            "mesh:D004191",
        ],
    },
    "efo": {"version": "3.76.0", "subset_identifiers": ["efo:0000408"]},
    "ncit": {"version": "25.03c", "subset_identifiers": ["ncit:C2991"]},
    "umls": {
        "version": "2024AB",
        "subset_identifiers": [
            "sty:T049",
            "sty:T047",
            "sty:T191",
            "sty:T050",
            "sty:T048",
        ],
    },
}

# helper functions
def get_subset_graph(full_graph: pyobo.Obo, subset_identifiers: list):
    """takes a default obo and outputs the network graph of its subset. This will take both the ancestors and descendants of the class"""
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
    subset_obo.write_obo(f"{subset_version}.obo")
    return subset_obo


def get_subset(prefix: str, version: str, subset_identifiers: list):
    """saves an OBO subset of a graph given a base prefix and version as well as terms to base subset on"""
    print("loading obo")
    obo = pyobo.get_ontology(prefix=prefix, version=version)
    print("loading graph")
    full_graph = obo.get_graph().get_networkx()
    print("subseting graph")
    subset_graph = get_subset_graph(
        full_graph=full_graph, subset_identifiers=subset_identifiers
    )
    print("writing res")
    subset_obo = subset_graph_to_obo(
        subset_graph=subset_graph, prefix=prefix, version=version
    )
    print(f"The full {prefix} graph has {len(obo.get_ids())} term")
    print(f"The {prefix} subset has {len(subset_obo.get_ids())} term")
    return subset_obo


def main(config: dict):
    """define subsets from a dictionary"""
    for prefix in config:
        get_subset(
            prefix=prefix,
            version=config[prefix]["version"],
            subset_identifiers=config[prefix]["subset_identifiers"],
        )


if __name__ == "__main__":
    main(config=config)
