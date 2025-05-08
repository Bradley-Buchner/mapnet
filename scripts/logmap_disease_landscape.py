"""
Match resources for the disease landscape pairwise using logmap
Notes:
    - this assumes you have run `scripts/download_raw_disease_resources.py` and `scripts/subset_obo.py` to get the prerequisite data
"""

import os
from mapnet.logmap import build_image, logmap_arg_factory, run_logmap_pairwise

## define our subsets
dataset_def = {
    "resources": {
        "DOID": {"version": "2025-03-03", "subset": False, "subset_identifiers": []},
        # "EFO": {
        #     "version": "3.76.0",
        #     "subset": True,
        #     "subset_identifiers": ["0000408"],
        # },
        # "GARD": {"version": "", "subset": False, "subset_identifiers": []},
        # "ICD10": {"version": "2019", "subset": False, "subset_identifiers": []},
        # "ICD11": {"version": "2025-01", "subset": False, "subset_identifiers": []},
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
        # "MONDO": {"version": "2025-03-04", "subset": False, "subset_identifiers": []},
        # "NCIT": {
        #     "version": "25.03c",
        #     "subset": True,
        #     "subset_identifiers": ["C2991"],
        # },
        # "OMIMPS": {"version": "2025-03-24", "subset": False, "subset_identifiers": []},
        # "Orphanet": {"version": "4.6", "subset": False, "subset_identifiers": []},
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
    },
}
run_args = {"tag": "0.01", "build": True, "analysis_name": "disease_landscape"}
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
    ## build image for logmap from docker file
    # build_image(**logmap_args)
    ## run the matching
    # run_container(**logmap_args)
    # lm_args = logmap_arg_factory(**dataset_def, **run_args)
    run_logmap_pairwise(**dataset_def, **run_args)
