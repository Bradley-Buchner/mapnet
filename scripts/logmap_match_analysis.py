"""
code for formatting the output of running logamp

"""

import os
from mapnet.logmap.utils import merge_logmap_mappings
from mapnet.utils import get_novel_mappings

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
    },
}
## additional namepsaces to check for names from ###
additional_namespaces = {
    "hp": {"version": None},
    "go": {"version": None},
    "orphanet.ordo": {"version": "4.6"},
}
run_args = {"tag": "0.01", "build": False, "analysis_name": "disease_landscape"}

if __name__ == "__main__":
    predicted_mappings = merge_logmap_mappings(
        additional_namespaces=additional_namespaces, **dataset_def, **run_args
    )
    novel, right, wrong = get_novel_mappings(
        predicted_mappings=predicted_mappings,
        additional_namespaces=additional_namespaces,
        **dataset_def,
        **run_args,
    )
