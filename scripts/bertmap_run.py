"""
Re-run the matching of DOID to MESH using Logmap.
"""

import os
from mapnet.bertmap.utils import load_bertmap


tag = "0.01"
full_mesh_obo_file = "resources/mesh-2025.obo"

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
        "known_mappings_path": os.path.join(os.getcwd(), "known_mappings"),
    },
}

bertmap_args = {
    "dataset_dir": dataset_def["meta"]["dataset_dir"],
    "check_known_maps": True,
    "check_biomappings": True,
    "train_model": True,
    "global_matching": False,
    "use_auxiliary_mappings": False,
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
    load_bertmap(**dataset_def, **bertmap_args)
