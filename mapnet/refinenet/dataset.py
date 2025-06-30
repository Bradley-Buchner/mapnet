"""methods for generating training and inference dataset for use in RefineNet model"""

import argparse
import os

import polars as pl

from mapnet.refinenet.constants import (
    GENERATED_DATASET_SCHEMA,
    INFERENCE_DATASET_SCHEMA,
)
from mapnet.utils import (
    ancestors_within_distance,
    descendants_within_distance,
    file_safety_check,
    get_name_from_curie,
    get_name_maps,
    get_network_graph,
    load_config_from_json,
    load_known_mappings_df,
    load_semera_landscape_df,
    normalize_dataset_def,
    normalized_edit_similarity,
    sssom_to_biomappings,
    top_k_named_relations,
)


def add_ancestors_and_descendants(
    row,
    name_map_func,
    source_graph,
    target_graph,
    max_distance,
    max_relations,
    bin_edit_similarity: bool = True,
    edit_cutoff: float = 0.00,
):
    """adds ancestor and descendant names and identifiers to a row"""
    (
        row["source descendant identifiers"],
        row["source descendant names"],
    ) = top_k_named_relations(
        source_graph,
        row["source identifier"],
        name_map_func,
        k=max_relations,
        descendants=False,
        max_distance=max_distance,
    )
    (
        row["target descendant identifiers"],
        row["target descendant names"],
    ) = top_k_named_relations(
        target_graph,
        row["target identifier"],
        name_map_func,
        k=max_relations,
        descendants=False,
        max_distance=max_distance,
    )
    (
        row["source ancestor identifiers"],
        row["source ancestor names"],
    ) = top_k_named_relations(
        source_graph,
        row["source identifier"],
        name_map_func,
        k=max_relations,
        descendants=True,
        max_distance=max_distance,
    )
    (
        row["target ancestor identifiers"],
        row["target ancestor names"],
    ) = top_k_named_relations(
        target_graph,
        row["target identifier"],
        name_map_func,
        k=max_relations,
        descendants=True,
        max_distance=max_distance,
    )
    e_sim = normalized_edit_similarity(row)
    if e_sim < edit_cutoff:
        return None
    if bin_edit_similarity:
        if e_sim < 0.33:
            e_sim = "LOW"
        elif e_sim < 0.66:
            e_sim = "MEDIUM"
        else:
            e_sim = "HIGH"
    row["edit_similarity"] = e_sim
    return row


def process_known_maps(dataset_def):
    """load in known maps from semra"""
    tags = ["skos:exactMatch", "skos:broadMatch", "skos:narrowMatch"]
    cols = [
        "subject_id",
        "object_id",
        "predicate_id",
        "mapping_justification",
    ]
    ## load in semra dataset
    semra_df = load_semera_landscape_df(
        landscape_name="disease",
        resources=dataset_def["resources"],
        sssom=True,
        additional_namespaces=dict(),
    )
    provided_df = load_known_mappings_df(
        **dataset_def, additional_namespaces=None, sssom=True
    )
    df = semra_df.select(cols).vstack(provided_df.select(cols)).unique()

    ## get and process exact matches
    exact_maps = df.filter(pl.col("predicate_id").eq(tags[0]))
    ## get and process broad and narrow maps
    broad_maps = df.filter(pl.col("predicate_id").eq(tags[1]))
    narrow_maps = df.filter(pl.col("predicate_id").eq(tags[2]))
    return (
        sssom_to_biomappings(
            df=exact_maps, resources=dataset_def["resources"]
        ).drop_nulls(),
        sssom_to_biomappings(
            df=broad_maps, resources=dataset_def["resources"]
        ).drop_nulls(),
        sssom_to_biomappings(
            df=narrow_maps, resources=dataset_def["resources"]
        ).drop_nulls(),
    )


def synthetic_step(
    dataset_def: dict, exact_maps: pl.DataFrame, network_graphs: dict, max_distance: int
):
    """Generate a dataset of synthetic broad and narrow mappings from true exact mappings"""
    ## get mappings from id to name for each ontology
    name_maps = get_name_maps(**dataset_def)
    name_map_func = lambda x: get_name_from_curie(x, name_maps).lower()
    class_counts = {"exact": 0, "broad": 0, "narrow": 0}
    known_maps = exact_maps.sample(fraction=1.0, shuffle=True)
    generated_maps = []
    ## use exact mappings to generate a synthetic dataset with broad and narrow mappings
    for row in known_maps.iter_rows(named=True):
        mapped = False
        target_class = min(class_counts, key=class_counts.get)
        generated_map = row.copy()
        ## make sure this mapping is from an ontology we have graphs for
        if (
            row["source prefix"] not in network_graphs
            or row["target prefix"] not in network_graphs
        ):
            continue
        target_graph = network_graphs[row["target prefix"]]
        source_graph = network_graphs[row["source prefix"]]
        ## make sure the node is in the graph before proceeding
        if generated_map["target identifier"] not in target_graph.nodes:
            continue
        ## try to make a narrow match first
        if target_class == "narrow":
            candidates = ancestors_within_distance(
                target_graph,
                generated_map["target identifier"],
                max_distance=max_distance,
            )
            candidates = list(filter(lambda x: x in target_graph.nodes, candidates))
            if len(candidates) > 0:
                generated_map["target identifier"] = candidates[0]
                generated_map["target name"] = name_map_func(candidates[0])
                generated_map["class"] = 2
                class_counts["narrow"] += 1
                mapped = True
        ## otherwise try to make a broad match
        elif (target_class == "broad") and (not mapped):
            candidates = descendants_within_distance(
                target_graph,
                generated_map["target identifier"],
                max_distance=max_distance,
            )
            candidates = list(filter(lambda x: x in target_graph.nodes, candidates))
            if len(candidates) > 0:
                generated_map["target identifier"] = candidates[0]
                generated_map["target name"] = name_map_func(candidates[0])
                generated_map["class"] = 1
                class_counts["broad"] += 1
                mapped = True
        ## if none of the above are met it will just use the original (exact match)
        if not mapped:
            generated_map["class"] = 0
            class_counts["exact"] += 1
            mapped = True
        ## add ancestor and descendant information to the row directly
        generated_map = add_ancestors_and_descendants(
            row=generated_map,
            name_map_func=name_map_func,
            source_graph=source_graph,
            target_graph=target_graph,
            max_distance=max_distance,
            max_relations=3,
            bin_edit_similarity=True,
            edit_cutoff=0.00,  ## not using distance cutoff
        )
        generated_maps.append(generated_map)
    return generated_maps


def real_step(
    minority_maps: list,
    dataset_def: dict,
    network_graphs: dict,
    max_distance: int,
):
    """add real minority classes to the training data"""
    generated_maps = []
    name_maps = get_name_maps(**dataset_def)
    name_map_func = lambda x: get_name_from_curie(x, name_maps).lower()
    for i, known_maps in enumerate(minority_maps):
        for row in known_maps.iter_rows(named=True):
            if (
                row["source prefix"] not in network_graphs
                or row["target prefix"] not in network_graphs
            ):
                continue
            target_graph = network_graphs[row["target prefix"]]
            source_graph = network_graphs[row["source prefix"]]

            generated_map = row.copy()
            generated_map = add_ancestors_and_descendants(
                row=generated_map,
                name_map_func=name_map_func,
                source_graph=source_graph,
                target_graph=target_graph,
                max_distance=max_distance,
                max_relations=3,
                bin_edit_similarity=True,
                edit_cutoff=0.00,  ## not using distance cutoff
            )
            generated_map["class"] = i + 1
            ## add  and ancestors for source to row
            generated_maps.append(generated_map)
    return generated_maps


def make_synthetic_dataset(dataset_def: dict, max_distance: int, output_path: str):
    """generate a synthetic training dataset for Refinenet models.
    Loads in known mappings both directly from the source ontologies and Semra.
    Takes broad and narrow maps from those sources directly, and uses exact mappings
    to generate synthetic broad and narrow matchings.
    Note: Tries to make classes as balanced as possible, but often there are more exact mappings.
    """
    ## load raw mappings from provided by ontologies and Semra
    exact_maps, broad_maps, narrow_maps = process_known_maps(dataset_def=dataset_def)
    ## load in obo graphs for each ontology as a dict
    network_graphs = {
        x: get_network_graph(**dataset_def, prefix=x) for x in dataset_def["resources"]
    }
    ## add synthetic broad and narrow mappings
    generated_maps = synthetic_step(
        dataset_def=dataset_def,
        exact_maps=exact_maps,
        network_graphs=network_graphs,
        max_distance=max_distance,
    )
    ## add any real examples of the minority classes to the training data to improve signal
    generated_maps += real_step(
        minority_maps=[broad_maps, narrow_maps],
        dataset_def=dataset_def,
        network_graphs=network_graphs,
        max_distance=max_distance,
    )
    ## read the maps into a polars datafarme
    generated_maps_df = (
        pl.from_records(generated_maps, schema=GENERATED_DATASET_SCHEMA)
        .unique()
        .with_columns(
            pl.col("source name").str.to_lowercase(),
            pl.col("target name").str.to_lowercase(),
        )
    )
    ## define the output path and confirm with the user it is ok to overwrite an existing one
    output_path = "generated_maps.parquet" if output_path == "" else output_path
    file_safety_check(output_path)
    ## write output (to parquet file since contains nested data-types)
    generated_maps_df.write_parquet(output_path)


def make_inference_dataset(
    mappings_path: str,
    dataset_def: dict,
    edit_cutoff: float,
    max_distance: int,
    output_path: str,
):
    ## read in base of inference dataset
    known_maps = pl.read_csv(
        mappings_path,
        separator="\t",
    )
    ## load dictionary of obo graphs
    network_graphs = {
        x: get_network_graph(**dataset_def, prefix=x) for x in dataset_def["resources"]
    }
    ## get name maps
    name_maps = get_name_maps(
        **dataset_def,
    )
    name_map_func = lambda x: get_name_from_curie(x, name_maps).lower()
    generated_maps = []
    ## format the dataset
    for row in known_maps.iter_rows(named=True):
        if (
            row["source prefix"] not in network_graphs
            or row["target prefix"] not in network_graphs
        ):
            continue

        target_graph = network_graphs[row["target prefix"]]
        source_graph = network_graphs[row["source prefix"]]

        generated_map = row.copy()
        generated_map = add_ancestors_and_descendants(
            row=generated_map,
            name_map_func=name_map_func,
            source_graph=source_graph,
            target_graph=target_graph,
            max_distance=max_distance,
            max_relations=3,
            bin_edit_similarity=True,
            edit_cutoff=edit_cutoff,  ## not using distance cutoff
        )
        generated_maps.append(generated_map)
    generated_maps_df = (
        pl.from_records(generated_maps, schema=INFERENCE_DATASET_SCHEMA)
        .unique()
        .with_columns(
            pl.col("source name").str.to_lowercase(),
            pl.col("target name").str.to_lowercase(),
        )
    )
    output_path = "logmap_maps.parquet" if output_path is None else output_path
    file_safety_check(output_path)
    ## write output (to parquet file since contains nested data-types)
    generated_maps_df.write_parquet(output_path)


def main(
    config_path: str,
    max_distance: int,
    output_path: str,
    synthetic: bool,
    mappings_path: str,
    edit_cutoff: float,
):
    """dispatch methods to either generate a dataset for training or inference"""
    config = load_config_from_json(config_path=config_path)
    dataset_def = config["dataset_def"]
    dataset_def = normalize_dataset_def(dataset_def=dataset_def)
    if synthetic:
        make_synthetic_dataset(
            dataset_def=dataset_def,
            max_distance=max_distance,
            output_path=output_path,
        )
    else:
        make_inference_dataset(
            mappings_path=mappings_path,
            dataset_def=dataset_def,
            edit_cutoff=edit_cutoff,
            max_distance=max_distance,
            output_path=output_path,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config-path",
        type=str,
        default="mapnet/utils/configs/disease_landscape.json",
        help="Path to json run configuration see 'mapnet/utils/configs' for examples",
    )
    parser.add_argument(
        "-m",
        "--max-distance",
        type=int,
        default=3,
        help="Max distance to use when looking for relatives to a node",
    )
    parser.add_argument(
        "-o",
        "--output-path",
        type=str,
        default="./generated_maps.parquet",
        help="where to write generated mappings file",
    )
    parser.add_argument(
        "-s",
        "--synthetic",
        action="store_true",
        help="if making a syntehic dataset, if false takes a set of mappings to make a refinenet dataset",
    )
    parser.add_argument(
        "-d",
        "--mappings-path",
        type=str,
        default="output/logmap/disease_landscape/full_analysis/semra_novel_mappings.tsv",
        help="if not synthetic, what dataset to use as base",
    )
    parser.add_argument(
        "-e",
        "--edit-cutoff",
        type=float,
        default=0.00,
        help="min edit similarity to use for mappings. (only if synthetic is false) defaults to zero ie know mappings will be exuded",
    )
    args = parser.parse_args()
    main(**vars(args))
