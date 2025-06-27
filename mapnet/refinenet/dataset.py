"""Generates synthetic broad, narrow exact match dataset from a set of true mappings"""

# from itertools import combinations
import argparse
import os
from itertools import combinations

import numpy as np
import polars as pl

from mapnet.utils import (ancestors_within_distance,
                          descendants_within_distance, file_safety_check,
                          get_name_from_curie, get_name_maps,
                          get_network_graph, load_config_from_json,
                          load_known_mappings_df, normalize_dataset_def, sssom_to_biomappings, load_semera_landscape_df,
                          normalized_edit_similarity, top_k_named_relations)

LABEL_MAP = {
    0: "exact match",
    1: "broad match",
    2: "narrow match",
}


GENERATED_SCHEMA = pl.Schema(
    [
        ("source identifier", pl.String),
        ("source name", pl.String),
        ("source prefix", pl.String),
        ("target identifier", pl.String),
        ("target name", pl.String),
        ("target prefix", pl.String),
        ("class", pl.Int64),
        ("source descendant identifiers", pl.List(pl.String)),
        ("source descendant names", pl.List(pl.String)),
        ("target descendant identifiers", pl.List(pl.String)),
        ("target descendant names", pl.List(pl.String)),
        ("source ancestor identifiers", pl.List(pl.String)),
        ("source ancestor names", pl.List(pl.String)),
        ("target ancestor identifiers", pl.List(pl.String)),
        ("target ancestor names", pl.List(pl.String)),   
        ("edit_similarity", pl.String),
    ]
)

def add_ancestors_and_descendants(
        row, name_map_func, source_graph, target_graph, max_distance, max_relations, bin_edit_similarity:bool = True, edit_cutoff:float = 0.00
):
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
           e_sim = 'LOW'
        elif e_sim < 0.66:
            e_sim = 'MEDIUM'
        else:
            e_sim = 'HIGH'
    row['edit_similarity'] = e_sim
    return row


def update_synthetic_dataset(
    known_maps: pl.DataFrame,
    source_graph,
    target_graph,
    name_maps: dict,
    max_distance: int = 3,
    output_path: str = None,
):
    """adds rows to a .
    Note:
        - distance_cutoff sets the maximum distance (ie number of edges) to use when getting ancestors or
    """

    name_map_func = lambda x: get_name_from_curie(x, name_maps).lower()
    class_counts = {"exact": 0, "broad": 0, "narrow": 0}
    known_maps = known_maps.sample(fraction=1.0, shuffle=True)
    generated_maps = []
    for  row in known_maps.iter_rows(named=True):
        mapped = False
        target_class = min(class_counts, key=class_counts.get)
        generated_map = row.copy()
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
        generated_map = add_ancestors_and_descendants(
            row=generated_map,
            name_map_func=name_map_func,
            source_graph=source_graph,
            target_graph=target_graph,
            max_distance=max_distance,
            max_relations=3,
            bin_edit_similarity=True, 
            edit_cutoff=0.00 ## not using distance cutoff
        )
        generated_maps.append(generated_map)
    generated_maps_df = pl.from_records(generated_maps, schema=GENERATED_SCHEMA)
    pq_path = "generated_maps.parquet" if output_path is None else output_path
    if os.path.exists(pq_path):
        df = pl.read_parquet(pq_path, schema=GENERATED_SCHEMA)
        generated_maps_df = generated_maps_df.vstack(df).unique()
    generated_maps_df = generated_maps_df.with_columns(
            pl.col("source name").str.to_lowercase(),
            pl.col("target name").str.to_lowercase(),
            )
    generated_maps_df.write_parquet(pq_path)
    return generated_maps_df


def add_known_broad_and_narrow_maps(broad_maps, narrow_maps, network_graphs, name_maps, max_distance, output_path, dataset_def) :
    broad_maps = sssom_to_biomappings(df = broad_maps, resources = dataset_def['resources']).drop_nulls()
    narrow_maps = sssom_to_biomappings(df = narrow_maps, resources = dataset_def['resources']).drop_nulls()

    name_map_func = lambda x: get_name_from_curie(x, name_maps).lower()
    generated_maps = []
    for i, known_maps in enumerate([broad_maps, narrow_maps]):
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
                edit_cutoff=0.00 ## not using distance cutoff
            )
            generated_map['class'] = i + 1
            ## add  and ancestors for source to row
            generated_maps.append(generated_map)

    generated_maps_df = pl.from_records(generated_maps, schema=GENERATED_SCHEMA)
    if os.path.exists(output_path):
        df = pl.read_parquet(output_path, schema=GENERATED_SCHEMA)
        generated_maps_df = generated_maps_df.vstack(df).unique()
    generated_maps_df.write_parquet(output_path)

def make_synthetic_dataset(
    dataset_def: dict, run_args: dict, max_distance: int, output_path: str
):
    from_resources_df = load_known_mappings_df(
        **dataset_def, **run_args, additional_namespaces=None
    )
    semra_df = load_semera_landscape_df(landscape_name='disease', resources=dataset_def['resources'], sssom=True, additional_namespaces=dict())
    tags = ['skos:exactMatch', 'skos:broadMatch', 'skos:narrowMatch']
    exact_semra_maps = semra_df.filter(pl.col('predicate_id').eq(tags[0])).unique()
    exact_semra_maps = sssom_to_biomappings(df = exact_semra_maps, resources = dataset_def['resources']).drop_nulls()
    exact_maps = from_resources_df.vstack(exact_semra_maps).unique()
    network_graphs = {
        x: get_network_graph(**dataset_def, prefix=x) for x in dataset_def["resources"]
    }
    for source_prefix, target_prefix in combinations(dataset_def["resources"], r=2):
        ## now lets filter this for mappings from mondo to mesh
        e_maps = exact_maps.clone()
        e_maps = e_maps.filter(pl.col("source prefix").eq(source_prefix))
        e_maps = e_maps.filter(pl.col("target prefix").eq(target_prefix))
        source_graph = network_graphs[source_prefix]
        target_graph = network_graphs[target_prefix]
        name_maps = get_name_maps(**dataset_def)
        update_synthetic_dataset(
            known_maps=e_maps,
            source_graph=source_graph,
            target_graph=target_graph,
            name_maps=name_maps,
            max_distance=max_distance,
            output_path=output_path,
        )
    broad_maps = semra_df.filter(pl.col('predicate_id').eq(tags[1])).unique()
    narrow_maps = semra_df.filter(pl.col('predicate_id').eq(tags[2])).unique()
    add_known_broad_and_narrow_maps(narrow_maps=narrow_maps, broad_maps= broad_maps, dataset_def=dataset_def, network_graphs=network_graphs, name_maps=name_maps, max_distance=max_distance, output_path=output_path)

def update_refinemap_dataset(
    known_maps: pl.DataFrame,
    network_graphs,
    name_maps: dict,
    max_distance: int = 3,
    output_path: str = None,
    edit_cutoff:float = 0.00
):
    """takes a set of mappings and prepares it for use with refinemap models, will add ancestors and descendants
    Note:
        - distance_cutoff sets the maximum distance (ie number of edges) to use when getting ancestors or
    """

    consistent_schema = pl.Schema(
        [
            ("source identifier", pl.String),
            ("source name", pl.String),
            ("source prefix", pl.String),
            ("target identifier", pl.String),
            ("target name", pl.String),
            ("target prefix", pl.String),
            ("source descendant identifiers", pl.List(pl.String)),
            ("source descendant names", pl.List(pl.String)),
            ("target descendant identifiers", pl.List(pl.String)),
            ("target descendant names", pl.List(pl.String)),
            ("source ancestor identifiers", pl.List(pl.String)),
            ("source ancestor names", pl.List(pl.String)),
            ("target ancestor identifiers", pl.List(pl.String)),
            ("target ancestor names", pl.List(pl.String)),
            ("edit_similarity", pl.String),
        ]
    )
    name_map_func = lambda x: get_name_from_curie(x, name_maps).lower()
    generated_maps = []
    for i, row in enumerate(known_maps.iter_rows(named=True)):
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
            edit_cutoff=0.00 ## not using distance cutoff
        )
        ## add  and ancestors for source to row
        generated_maps.append(generated_map)
    generated_maps_df = pl.from_records(generated_maps, schema=consistent_schema)
    pq_path = "logmap_maps.parquet" if output_path is None else output_path
    if os.path.exists(pq_path):
        df = pl.read_parquet(pq_path, schema=consistent_schema)
        generated_maps_df = generated_maps_df.vstack(df).unique()
    generated_maps_df = generated_maps_df.with_columns(
            pl.col("source name").str.to_lowercase(),
            pl.col("target name").str.to_lowercase(),
            )
    generated_maps_df.write_parquet(pq_path)
    return generated_maps_df


def make_refinenet_dataset(
    mappings_path: str,
    dataset_def: dict,
    edit_cutoff: float,
    max_distance: int,
    output_path: str,
):
    known_maps = pl.read_csv(
        mappings_path,
        separator="\t",
    )
    network_graphs = {
        x: get_network_graph(**dataset_def, prefix=x) for x in dataset_def["resources"]
    }
    name_maps = get_name_maps(
        **dataset_def,
    )
    update_refinemap_dataset(
        known_maps=known_maps,
        network_graphs=network_graphs,
        name_maps=name_maps,
        max_distance=max_distance,
        output_path=output_path,
        edit_cutoff = edit_cutoff
    )


def main(
    config_path: str,
    max_distance: int,
    output_path: str,
    synthetic: bool,
    mappings_path: str,
    edit_cutoff: float,
):
    config = load_config_from_json(config_path=config_path)
    dataset_def = config["dataset_def"]
    run_args = config["run_args"]
    dataset_def = normalize_dataset_def(dataset_def=dataset_def)
    if synthetic:
        output_path = "generated_maps.parquet" if output_path == "" else output_path
        file_safety_check(output_path)
        make_synthetic_dataset(
            dataset_def=dataset_def,
            run_args=run_args,
            max_distance=max_distance,
            output_path=output_path,
        )
    else:
        output_path = "logmap_maps.parquet" if output_path == "" else output_path
        file_safety_check(output_path)
        make_refinenet_dataset(
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
