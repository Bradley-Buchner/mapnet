"""
Here we can install the raw reasources for this landscape with pyobo.
Notes:
- Some of these require an API key registered in pyobo
- I am not going to download the ontologies that were marked as O in the landscape, as many of those dont have easily obtainable obo files.
- UMLS is to large for me to do locally, so I will hold off on that until I get HPC access
"""

from mapnet.utils import download_raw_obo_files
import os

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
    download_raw_obo_files(dataset_def=dataset_def)
