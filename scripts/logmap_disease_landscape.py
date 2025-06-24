"""
Match resources for the disease landscape pairwise using logmap
Notes:
    - not running umls since it has high resource requirements for now
"""

import os
from mapnet.utils import download_raw_obo_files, get_onto_subsets, normalize_dataset_def
from mapnet.logmap import run_logmap_pairwise

## define our subsets
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
                # "D007239", # c01
                # "D009369", # c04
                # "D009140", # c05
                # "D004066", # co6
                # "D009057", # co7
                # "D012140", # co8
                # "D010038", # co9
                # "D009422", # c10
                # "D005128", # c11
                # "D000091642", # c12
                # "D002318", #c14
                # "D006425", # c15
                # "D009358", #c16
                # "D017437", # c17
                # "D009750", # c18
                # "D004700", # c19
                # "D007154", # c20
                # "D007280", # c21
                # "D000820", # c22
                # "D013568", # c23
                # "D009784", # c24
                # "D064419", # c25
                # "D014947", # c26
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
    },
}
run_args = {"tag": "0.01", "build": False, 'singularity': False, "analysis_name": "disease_landscape"}

if __name__ == "__main__":
    dataset_def = normalize_dataset_def(dataset_def=dataset_def)
    ## download the obo files for each resource
    download_raw_obo_files(dataset_def=dataset_def)
    ## subset the resources
    get_onto_subsets(dataset_def=dataset_def, verbose=True)
    # ## run logmap on each pairwise resource
    run_logmap_pairwise(**dataset_def, **run_args)
