"""Here we can install the raw reasources for this landscape with pyobo note that some of these require an API key registered in pyobo"""

import pyobo

version_mappings = {
    "DOID": "2025-03-03",
    "EFO": "3.76.0",
    "GARD": "",
    "ICD10": "2019",
    "ICD10-CM": "",
    "ICD11": "2025-01",
    "ICD9": "",
    "ICD9-CM": "",
    "ICD-0": "",
    "MESH": "2025",
    "MONDO": "2025-03-04",
    "NCIT": "25.03c",
    "OMIM": "2025-03-24",
    "OMIMPS": "2025-03-24",
    "Orphanet": "4.6",
    "UMLS": "2024AB",
}


def main(version_mappings: dict):
    """download raw data files for a set of reasources"""
    for prefix in version_mappings:
        pyobo.get_ontology(prefix=prefix, version=version_mappings[prefix])


if __name__ == "__main__":
    main(version_mappings=version_mappings)
