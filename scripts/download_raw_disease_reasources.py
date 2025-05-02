"""
Here we can install the raw reasources for this landscape with pyobo.
Notes:
- Some of these require an API key registered in pyobo
- I am not going to download the ontologies that were marked as O in the landscape, as many of those dont have easilty obtrainable obo files.
"""

from mapnet.utils import download_raw_obo_files

version_mappings = {
    "DOID": "2025-03-03",
    "EFO": "3.76.0",
    "GARD": "",
    "ICD10": "2019",
    "ICD11": "2025-01",
    "MESH": "2025",
    "MONDO": "2025-03-04",
    "NCIT": "25.03c",
    "OMIMPS": "2025-03-24",
    "Orphanet": "4.6",
    "UMLS": "2024AB",
}

if __name__ == "__main__":
    download_raw_obo_files(version_mappings=version_mappings)
