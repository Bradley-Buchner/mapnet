"""Generate mappings using BERTmap."""

import argparse
import os
import subprocess
from functools import reduce
from itertools import product

import torch
from deeponto.align.bertmap import DEFAULT_CONFIG_FILE, BERTMapPipeline
from deeponto.onto import Ontology
from huggingface_hub import snapshot_download

import biomappings
from biomappings.resources import append_prediction_tuples
from bioregistry import parse_iri, get_iri

from mapnet.utils import download_owl

parser = argparse.ArgumentParser(description="Train BERTMap Model")
parser.add_argument("--config", default=DEFAULT_CONFIG_FILE, help="Path to Config File")
parser.add_argument(
    "--source_ontology_train",
    default="MESH",
    help="Ontologies to match as source during training",
)
parser.add_argument(
    "--target_ontology_train",
    default="DOID",
    help="Ontologies to match as target during training",
)
parser.add_argument(
    "--source_ontologies_inference",
    nargs="+",
    default=["MESH"],
    help="Ontologies to match as source during inference",
)
parser.add_argument(
    "--target_ontologies_inference",
    nargs="+",
    default=["DOID", "CHEBI", "HGNC", "GO"],
    help="Ontologies to match as target during inference",
)
parser.add_argument(
    "--ontologies_path", default="resources", help="Directory with ontology files"
)
parser.add_argument(
    "--mappings_path",
    default="mesh_ambig_mappings.tsv",
    help="Path with mappings to be evaluated with BERRTMap",
)
parser.add_argument(
    "--train_model",
    action="store_true",
    help="If present will locally train a model otherwise will pull from hugging face.",
)


def get_iri_overload(prefix: str, identifier: str):
    """overload of bioregistry's get_iri method to work with mesh path"""
    identifier = identifier.split(":", 1)[-1]
    prefix = prefix.lower()
    if prefix.lower() != "mesh":
        return get_iri(prefix, identifier)
    else:
        return "http://purl.bioontology.org/ontology/MESH/" + identifier


def load_bertmap(
    config: str,
    target_ontology_train: str,
    source_ontology_train: str,
    ontology_paths: dict,
    use_biomappings: bool = False,
    known_map_path: str = None,
    train_model: bool = False,
):
    """Load in the bertmap model (will download from hugging face if not present in ./bertmap)."""
    config = BERTMapPipeline.load_bertmap_config(config)
    print("-" * 100)
    use_biomappings = (known_map_path is None) & use_biomappings
    if use_biomappings:
        print("generating known mappings")
        config.known_mappings = save_known_maps(
            target_ontology_train=target_ontology_train,
            source_ontology_train=source_ontology_train,
            mappings_path="knownMaps",
        )
    elif known_map_path:
        print(f"loading known mappings from {known_map_path}")
        config.known_mappings = known_map_path
    else:
        print("not loading known mappings")
    print("-" * 100)
    print("Using config: \n{0}".format(config))
    print("-" * 100)
    if not train_model:
        config.global_matching.enabled = False
        if not os.path.isdir("bertmap"):
            print("downloading model from hugging face")
            snapshot_download(
                repo_id="buzgalbraith/BERTMAP-BioMappings", local_dir="./"
            )
        else:
            print("Model found at bertmap")
    src_onto = Ontology(ontology_paths[source_ontology_train.lower()])
    target_onto = Ontology(ontology_paths[target_ontology_train.lower()])
    return BERTMapPipeline(src_onto, target_onto, config)


def save_known_maps(
    target_ontology_train: str, source_ontology_train: str, mappings_path="knownMaps"
):
    """Use true mappings from Biomappings as ground truth for training BertMap Model."""
    true_mappings = biomappings.load_mappings()

    def onto_filter(x):
        return (x["target prefix"].lower() == target_ontology_train.lower()) & (
            x["source prefix"].lower() == source_ontology_train.lower()
        )

    def iri_map(x):
        return (
            get_iri_overload(x["source prefix"], x["source identifier"])
            + "\t"
            + get_iri_overload(x["target prefix"], x["target identifier"])
            + "\t1.0\n"
        )

    known_maps = map(iri_map, filter(onto_filter, true_mappings))

    os.makedirs(mappings_path, exist_ok=True)
    known_maps_path = os.path.join(
        mappings_path,
        source_ontology_train.lower() + "-" + target_ontology_train.lower() + ".tsv",
    )

    f = open(known_maps_path, mode="w")
    f.write("SrcEntity\tTgtEntity\tScore\n")
    f.writelines(known_maps)
    f.close()
    return known_maps_path


# inference


def bertmap_inference(
    ambig_maps_to_check,
    nonambig_maps_to_check,
    target_onto_prefix: str,
    source_onto_prefix: str,
    bertmap,
    target_onto,
    source_onto,
):
    """Run inference on a single pair of ontologies using BertMap."""
    target_prefix = target_onto_prefix.lower()
    source_prefix = source_onto_prefix.lower()
    # setting constants
    relation = "skos:exactMatch"
    match_type = "semapv:SemanticSimilarityThresholdMatching"
    source = "generate_BERTMap_predictions.py"
    # re-write this as a new lst
    rows = []
    for src_class_iri, src_name, tgt_class_iri, target_name in nonambig_maps_to_check:
        src_class_annotations = bertmap.src_annotation_index[src_class_iri]
        tgt_class_annotations = bertmap.tgt_annotation_index[tgt_class_iri]
        conf = bertmap.mapping_predictor.bert_mapping_score(
            src_class_annotations, tgt_class_annotations
        )
        source_identifier = parse_iri(src_class_iri)[1]
        target_identifier = parse_iri(tgt_class_iri)[1]
        target_annotations = target_onto.get_annotations(tgt_class_iri)
        target_known_maps = [
            x for x in target_annotations if source_prefix.upper() in x
        ]
        source_annotations = source_onto.get_annotations(src_class_iri)
        source_known_maps = [
            x for x in source_annotations if target_prefix.upper() in x
        ]
        # if it is mapped at all that means the mapping is either already done or wrong so just skip
        if len(target_known_maps) > 0 or len(source_known_maps) > 0:
            pass
        elif len(src_class_annotations) == 0 or len(tgt_class_annotations) == 0:
            pass
        elif conf < 0.5:
            pass
        else:

            rows.append(
                biomappings.PredictionTuple(
                    source_prefix,
                    source_identifier,
                    src_name,
                    relation,
                    target_prefix,
                    target_identifier,
                    target_name,
                    match_type,
                    conf,
                    source,
                )
            )
    for src_class_iri, src_name, tgt_class_iri, target_name in ambig_maps_to_check:
        src_class_annotations = bertmap.src_annotation_index[src_class_iri]
        tgt_class_annotations = bertmap.tgt_annotation_index[tgt_class_iri]

        class_annotation_pairs = list(
            product(src_class_annotations, tgt_class_annotations)
        )
        synonym_scores = bertmap.bert_synonym_classifier.predict(class_annotation_pairs)
        # only one element tensor is able to be extracted as a scalar by .item()
        conf = float(torch.mean(synonym_scores).item())
        source_identifier = parse_iri(src_class_iri)[1]
        target_identifier = parse_iri(tgt_class_iri)[1]
        # check if in provided map
        target_annotations = target_onto.get_annotations(tgt_class_iri)
        target_known_maps = [
            x for x in target_annotations if source_prefix.upper() in x
        ]

        source_annotations = source_onto.get_annotations(src_class_iri)
        source_known_maps = [
            x for x in source_annotations if target_prefix.upper() in x
        ]
        # if it is mapped at all that means the mapping is either already done or wrong so just skip
        if len(target_known_maps) > 0 or len(source_known_maps) > 0:
            pass
        elif len(src_class_annotations) == 0 or len(tgt_class_annotations) == 0:
            pass
        elif conf < 0.5:
            pass
        else:
            rows.append(
                biomappings.PredictionTuple(
                    source_prefix,
                    source_identifier,
                    src_name,
                    relation,
                    target_prefix,
                    target_identifier,
                    target_name,
                    match_type,
                    conf,
                    source,
                )
            )
    return rows


def get_novel_mappings(
    target_onto_prefix: str, source_onto_prefix: str, maps_path: str
):
    """Get mappings to use for inference."""
    # get all potential mappings
    maps_to_check = get_maps_to_evaluate(
        target_onto_prefix=target_onto_prefix,
        source_onto_prefix=source_onto_prefix,
        maps_path=maps_path,
    )
    # filter out those already in biomappings
    maps_not_in_biomappings = filter_for_biomappings(
        target_onto_prefix=target_onto_prefix,
        source_onto_prefix=source_onto_prefix,
        maps_to_check=maps_to_check,
    )
    # break the remaining into an ambagious an non-ambagious set
    ambagious_maps, non_ambagious_maps = check_ambagious_maps(
        maps_not_in_biomappings=maps_not_in_biomappings
    )
    return ambagious_maps, non_ambagious_maps


def get_maps_to_evaluate(
    target_onto_prefix: str, source_onto_prefix: str, maps_path: str
):
    """Read in the initial set of mappings from a file."""
    target_name = target_onto_prefix.lower()
    source_name = source_onto_prefix.lower()
    mappings = open(maps_path).readlines()

    def split_map(x):
        """Split an iterable of strings by tab."""
        return x.split("\t")

    def onto_filter(x):
        """Filter to ensure that maps are in the right ontology."""
        return (x[0].lower() == source_name) & (x[3].lower() == target_name)

    def iri_map(x):
        """Map to IRI."""
        return (
            get_iri_overload(source_name, x[1]),
            x[2],
            get_iri_overload(target_name, x[4]),
            x[5].strip(),
        )

    maps_to_check = map(iri_map, filter(onto_filter, map(split_map, mappings)))
    return maps_to_check


def filter_for_biomappings(
    target_onto_prefix: str, source_onto_prefix: str, maps_to_check
):
    """Ensure that none of the mappings being evaluated are in biomappings already."""
    target_name = target_onto_prefix.lower()
    source_name = source_onto_prefix.lower()
    biomappings_maps = (
        biomappings.load_mappings()
        + biomappings.load_false_mappings()
        + biomappings.load_predictions()
    )

    def onto_filter(x):
        """Filter to ensure that maps are in the right ontology."""
        return (x["target prefix"].lower() == target_name) & (
            x["source prefix"].lower() == source_name
        )

    def iri_map(x):
        """Map each row to it's iri."""
        return (
            get_iri_overload(x["target prefix"], x["target identifier"]),
            get_iri_overload(x["source prefix"], x["source identifier"]),
        )

    def known_filter(x):
        """Filter for mappings not in biomappings."""
        return x not in biomappping_maps_to_check_against

    biomappping_maps_to_check_against = set(
        map(iri_map, filter(onto_filter, biomappings_maps))
    )
    maps_not_in_biomappings = filter(known_filter, maps_to_check)
    return maps_not_in_biomappings


def check_ambagious_maps(maps_not_in_biomappings):
    """Split maps into ambagious and non-ambagious."""
    maps_not_in_biomappings = list(maps_not_in_biomappings)
    value_counts = reduce(
        lambda acc, item: {
            **acc,
            **{
                f: {**acc.get(f, {}), item[f]: acc.get(f, {}).get(item[f], 0) + 1}
                for f in [0, 2]
            },
        },
        maps_not_in_biomappings,
        {},
    )
    value_counts = {**value_counts[0], **value_counts[2]}
    ambagious_iris = set(filter(lambda x: value_counts[x] > 1, value_counts))
    ambagious_maps = filter(
        lambda x: (x[0] in ambagious_iris) or (x[2] in ambagious_iris),
        maps_not_in_biomappings,
    )
    non_ambagious_maps = filter(
        lambda x: (x[0] not in ambagious_iris) and (x[2] not in ambagious_iris),
        maps_not_in_biomappings,
    )
    return ambagious_maps, non_ambagious_maps


def inference_across_ontologies(
    config_path: str,
    target_ontologies_inference: list,
    source_ontologies_inference: list,
    mappings_path: str,
    ontology_paths: dict,
):
    """Run inference using BERTMap model over multiple ontologies."""
    for target_onto_prefix, source_onto_prefix in product(
        target_ontologies_inference, source_ontologies_inference
    ):
        print("Filtering Mappings")
        ambagious_maps, non_ambagious_maps = get_novel_mappings(
            target_onto_prefix=target_onto_prefix,
            source_onto_prefix=source_onto_prefix,
            maps_path=mappings_path,
        )
        print("loading ontologies")
        src_onto = Ontology(ontology_paths[source_onto_prefix.lower()])
        target_onto = Ontology(ontology_paths[target_onto_prefix.lower()])
        config = BERTMapPipeline.load_bertmap_config(config_path)
        config.global_matching.enabled = False
        print("loading model")
        bertmap = BERTMapPipeline(src_onto, target_onto, config)
        print("running inference")
        rows = bertmap_inference(
            ambig_maps_to_check=ambagious_maps,
            nonambig_maps_to_check=non_ambagious_maps,
            target_onto_prefix=target_onto_prefix,
            source_onto_prefix=source_onto_prefix,
            bertmap=bertmap,
            target_onto=target_onto,
            source_onto=src_onto,
        )
        append_prediction_tuples(rows)


if __name__ == "__main__":
    args = parser.parse_args()
    ontology_paths = download_owl(
        target_ontology_train=args.target_ontology_train,
        source_ontology_train=args.source_ontology_train,
        source_ontologies_inference=args.source_ontologies_inference,
        target_ontologies_inference=args.target_ontologies_inference,
        ontologies_path=args.ontologies_path,
    )
    model = load_bertmap(
        config=args.config,
        target_ontology_train=args.target_ontology_train,
        source_ontology_train=args.source_ontology_train,
        ontology_paths=ontology_paths,
        use_biomappings=True,
        train_model=args.train_model,
    )
    inference_across_ontologies(
        config_path=args.config,
        target_ontologies_inference=args.target_ontologies_inference,
        source_ontologies_inference=args.source_ontologies_inference,
        mappings_path=args.mappings_path,
        ontology_paths=ontology_paths,
    )
