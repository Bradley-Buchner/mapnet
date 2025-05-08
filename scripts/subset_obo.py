"""script for sub setting ontologies in the same way as was done in the landscape analysis and saving the results.
Produces the same number of classes as the MESH subset mentioned in the Semra DB Paper, and slightly more classes
for the other resources, since we are including the ancestor classes as well.
Notes:
    - going to hold of on the UMLS subset for now as it may be two big
"""

from mapnet.utils import get_onto_subsets
import os

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
            ],
        },
        "MONDO": {"version": "2025-03-04", "subset": False, "subset_identifiers": []},
        "NCIT": {
            "version": "25.03c",
            "subset": True,
            "subset_identifiers": ["C2991"],
        },
        "OMIMPS": {"version": "2025-03-24", "subset": False, "subset_identifiers": []},
        "Orphanet": {"version": "4.6", "subset": False, "subset_identifiers": []},
        "UMLS": {
            "version": "2024AB",
            "subset": True,
            "subset_identifiers": [
                "T049",
                "T047",
                "T191",
                "T050",
                "T048",
            ],
        },
    },
    "meta": {
        "dataset_dir": os.path.join(os.getcwd(), "resources"),
        "subset_dir": "disease_subset",
    },
}

if __name__ == "__main__":
    get_onto_subsets(dataset_def=dataset_def, verbose=True)
