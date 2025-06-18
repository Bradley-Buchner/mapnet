"""Generate EFO <-> CHEBI mappings using Gilda. More generally can be used to generate pairwise mappings between a list of ontologies which have obo download links on Bioregistries."""

import argparse
from collections import Counter
from itertools import permutations
from urllib.error import URLError

import gilda
import obonet
from bioregistry import get_obo_download, normalize_curie, parse_curie
from indra.ontology.standardize import standardize_db_refs
from indra.tools.fix_invalidities import fix_invalidities_db_refs

from biomappings import load_false_mappings, load_mappings, load_unsure
from biomappings.resources import PredictionTuple, append_prediction_tuples


def generate_inferred_mappings(
    subject_prefix: str, object_prefix: str, graphs: dict, normalized_graph_nodes: dict
):
    """Generate a set of inferred mappings from the given object ontology to the given subject ontology with Gilda."""
    predictions = []
    curated_mappings = set()
    for m in list(load_mappings()) + list(load_unsure()) + list(load_false_mappings()):
        observed_subject = parse_curie(m["subject_id"])
        observed_object = parse_curie(m["object_id"])
        if observed_subject.prefix == subject_prefix and observed_object.prefix == object_prefix:
            curated_mappings.add(observed_subject.identifier)
        elif observed_object.prefix == subject_prefix and observed_subject.prefix == object_prefix:
            curated_mappings.add(observed_object.identifier)
    # We now iterate over all subject entries and check for possible mappings
    mappings = {}
    existing_refs_to_object = set()
    already_mappable = set()
    g = graphs[subject_prefix]
    for node, data in g.nodes(data=True):
        # Skip external entries
        if not node.lower().startswith(subject_prefix):
            continue
        # Make sure we have a name
        if "name" not in data:
            continue
        # Skip if already curated
        if node in curated_mappings:
            continue
        # Get existing xrefs as a standardized dict
        xrefs = [xref.split(":", maxsplit=1) for xref in data.get("xref", []) if ":" in xref]
        xrefs_dict = fix_invalidities_db_refs(dict(xrefs))
        standard_refs = standardize_db_refs(xrefs_dict)
        # If there are already mappings to the object, we keep track of that
        if object_prefix.upper() in standard_refs:
            already_mappable.add(node)
        existing_refs_to_object |= {
            id for ns, id in standard_refs.items() if ns == object_prefix.upper()
        }
        # We can now ground the name and specifically look for subject matches
        matches = gilda.ground(data["name"], namespaces=[object_prefix.upper()])
        # If we got a match, we add the object ID as a mapping
        if matches:
            for grounding in matches[0].get_groundings():
                if grounding[0] == object_prefix.upper():
                    ## ensure that the found object prefix is actually in the obo graph before adding it.
                    object_id = normalize_curie(f"{object_prefix}:{matches[0].term.id}")
                    subject_id = normalize_curie(node)
                    if object_id in normalized_graph_nodes[object_prefix]:
                        mappings[subject_id] = object_id
    print(f"Found {len(mappings)} {subject_prefix}->{object_prefix} mappings.")
    # We makes sure that (i) the node is not already mappable to object and that
    # (ii) there isn't some other node that was not already mapped to the
    # given object ID
    mappings = {
        k: v
        for k, v in mappings.items()
        if v not in existing_refs_to_object and k not in already_mappable
    }
    # We now need to make sure that we don't reuse the same object ID across
    # multiple predicted mappings
    cnt = Counter(mappings.values())
    mappings = {k: v for k, v in mappings.items() if cnt[v] == 1}

    print(f"Found {len(mappings)} {subject_prefix}->{object_prefix} filtered mappings.")
    # We can now add the predictions
    # swap subject and object so we add novel predictions
    for subject_id, object_id in mappings.items():
        pred = PredictionTuple(
            object_id=subject_id,
            object_label=normalized_graph_nodes[subject_prefix][subject_id],
            predicate_id="skos:exactMatch",
            subject_id=object_id,
            subject_label=normalized_graph_nodes[object_prefix][object_id],
            mapping_justification="semapv:LexicalMatching",
            confidence=0.9,
            mapping_tool="generate_efo_chebi_mappings.py",
        )
        predictions.append(pred)
    print(f"Derived {len(predictions)} {object_prefix}->{subject_prefix} mappings.")
    return predictions


def normalize_graph_nodes(onto_list: list, graphs: dict):
    """Return a mapping of normalized nodes to their corresponding names for all onto graphs."""
    normalized_graph_nodes = {}
    for prefix in onto_list:
        normalized_graph_nodes[prefix] = {}
        g = graphs[prefix]
        for node in g.nodes:
            normalized_node = normalize_curie(node)
            ## confirm the node curie can be normalized and has a corresponding name in the graph before adding.
            if (normalized_node is not None) & ("name" in g.nodes[node]):
                normalized_graph_nodes[prefix][normalized_node] = g.nodes[node]["name"]
    return normalized_graph_nodes


def main(onto_list: list):
    """Perform pairwise matching on given list of ontologies."""
    ## read in ontology graphs ##
    print("Loading OBO Graphs...")
    graphs = {}
    for prefix in onto_list:
        try:
            graphs[prefix] = obonet.read_obo(get_obo_download(prefix))
        except URLError as e:
            raise URLError(f"Unable to download obo graph for {prefix} \n {e}")
            
    ## get a normalized list of nodes to simplify lookup ##
    print("Normalizing OBO Graphs...")
    normalized_graph_nodes = normalize_graph_nodes(onto_list=onto_list, graphs=graphs)
    predictions = []
    for subject_prefix, object_prefix in permutations(onto_list, r=2):
        print(f"finding {object_prefix}->{subject_prefix} mappings")
        predictions += generate_inferred_mappings(
            subject_prefix=subject_prefix,
            object_prefix=object_prefix,
            graphs=graphs,
            normalized_graph_nodes=normalized_graph_nodes,
        )
        print("-" * 50)
    append_prediction_tuples(predictions, deduplicate=True, sort=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-l",
        "--onto_list",
        nargs="+",
        help="Ontologies to match",
        default=["efo", "chebi"],
    )
    args = parser.parse_args()
    main(onto_list=args.onto_list)
