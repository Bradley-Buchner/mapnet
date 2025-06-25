import polars as pl
from textdistance import levenshtein
import os
from mapnet.utils import (
    load_known_mappings_df,
    normalize_dataset_def,
    get_name_maps,
    get_network_graph,
    make_broad_narrow_dataset,
)
from itertools import combinations
dataset_def = {
    "resources": {
        "DOID": {"version": "2025-03-03", "subset": False, "subset_identifiers": []},
        "EFO": {
            "version": "3.76.0",
            "subset": True,
            "subset_identifiers": ["0000408"],
        },
        "GARD": {"version": "", "subset": False, "subset_identifiers": []},
        "ICD10": {"version": "2019", "subset": False, "subset_identifiers": []},
        "ICD11": {"version": "2025-01", "subset": False, "subset_identifiers": []},
        "MESH": {
            "version": "2025",
            "subset": True,
            "subset_identifiers": [
                "D007239",
                "D001520",
                "D011579",
                "D001523",
                "D004191",
                ### adding all mesh disease tree headers
                "D007239",  # c01
                "D009369",  # c04
                "D009140",  # c05
                "D004066",  # co6
                "D009057",  # co7
                "D012140",  # co8
                "D010038",  # co9
                "D009422",  # c10
                "D005128",  # c11
                "D000091642",  # c12
                "D002318",  # c14
                "D006425",  # c15
                "D009358",  # c16
                "D017437",  # c17
                "D009750",  # c18
                "D004700",  # c19
                "D007154",  # c20
                "D007280",  # c21
                "D000820",  # c22
                "D013568",  # c23
                "D009784",  # c24
                "D064419",  # c25
                "D014947",  # c26
            ],
        },
        "MONDO": {"version": "2025-03-04", "subset": False, "subset_identifiers": []},
        "NCIT": {
            "version": "25.03c",
            "subset": True,
            "subset_identifiers": ["C2991"],
        },
        "OMIMPS": {"version": "2025-03-24", "subset": False, "subset_identifiers": []},
        "orphanet": {"version": "4.6", "subset": False, "subset_identifiers": []},
        # "UMLS": {
        #     "version": "2024AB",
        #     "subset": True,
        #     "subset_identifiers": [
        #         "T049",
        #         "T047",
        #         "T191",
        #         "T050",
        #         "T048",
        #     ],
        # },
    },
    "meta": {
        "dataset_dir": os.path.join(os.getcwd(), "resources"),
        "subset_dir": "disease_subset",
        "landscape": "disease",
    },
}
additional_namespaces = dict()
run_args = {"sssom": False}


## helper methods
def normalized_edit_similarity(x):
    """
    calculate the normalized edit similarity for all target and source class names
    """
    return levenshtein.normalized_similarity(
        x["source name"].upper(), x["target name"].upper()
    )


if __name__ == "__main__":
    dataset_def = normalize_dataset_def(dataset_def=dataset_def)
    novel_maps = pl.read_csv(
        f"output/logmap/disease_landscape/full_analysis/semra_novel_mappings.tsv",
        separator="\t",
    )
    novel_maps = novel_maps.with_columns(
        edit_similarity=pl.struct(["source name", "target name"]).map_elements(
            normalized_edit_similarity, return_dtype=pl.Float32
        )
    )
    known_maps = novel_maps.filter(
        pl.col('edit_similarity') > 0.95
    )
    # known_maps = load_known_mappings_df(
    #     **dataset_def, **run_args, additional_namespaces=additional_namespaces
    # )
    dataset_def = normalize_dataset_def(dataset_def=dataset_def)
    network_graphs = {x:get_network_graph(**dataset_def, prefix=x) for x in dataset_def['resources']}
    for source_prefix, target_prefix in combinations(dataset_def['resources'], r = 2):
        ## now lets filter this for mappings from mondo to mesh
        k_maps = known_maps.clone()
        k_maps = k_maps.filter(pl.col("source prefix").eq(source_prefix))
        k_maps = k_maps.filter(pl.col("target prefix").eq(target_prefix))
        ## lets read in the network graph for both
        # if previous_source_prefix != source_prefix:
        #     source_graph = get_network_graph(**dataset_def, prefix=source_prefix)
        # if previous_target_prefix != target_prefix:
        #     target_graph = get_network_graph(**dataset_def, prefix=target_prefix)
        source_graph = network_graphs[source_prefix]
        target_graph = network_graphs[target_prefix]
        name_maps = get_name_maps(
            **dataset_def, additional_namespaces=additional_namespaces
        )
        max_distance = 3
        res = make_broad_narrow_dataset(
            known_maps=k_maps,
            source_graph=source_graph,
            target_graph=target_graph,
            name_maps=name_maps,
            max_distance=max_distance,
            output_path='logmap_maps.parquet'
        )
