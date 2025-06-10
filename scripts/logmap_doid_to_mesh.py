"""
Re-run the matching of DOID to MESH using Logmap.
"""

from os.path import join
from os import getcwd
from mapnet.utils import download_raw_obo_files, get_onto_subset
from mapnet.logmap import build_image, run_logmap

tag = "0.01"
full_mesh_obo_file = "resources/mesh-2025.obo"
dataset_def = {
    "resources": {
        "MESH": {
            "version": "2025",
            "subset": True,
            "subset_identifiers": [
                "D007239",
                "D001520",
                "D011579",
                "D001523",
                "D004191",
            ],
        },
        "DOID": {"version": "2025-03-03", "subset": False, "subset_identifiers": []},
    },
    "meta": {
        "dataset_dir": join(getcwd(), "resources"),
        "subset_dir": "disease_subset",
    },
}

logmap_args = {
    "tag": "0.01",
    "dataset_dir": dataset_def["meta"]["dataset_dir"],
    "target_def": {
        "prefix": "MESH",
        "version": "2025",
        "subset": True,
        "subset_name": "disease_subset",
    },
    "source_def": {
        "prefix": "DOID",
        "version": "2025-03-03",
        "subset": False,
        "subset_name": "",
    },
}

if __name__ == "__main__":
    ## download obo file if not already present
    download_raw_obo_files(dataset_def=dataset_def)
    # ## subset the mesh obo file
    get_onto_subset(prefix="MESH", method="full", dataset_def=dataset_def)
    ## build image for logmap from docker file
    build_image(**logmap_args)
    ## run the matching
    run_logmap(**logmap_args)
