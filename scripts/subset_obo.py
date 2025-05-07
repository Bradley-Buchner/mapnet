"""script for subsetting ontologies in the same way as was done in the landscape analysis and saving the ressults.
Produces the same number of classes as the MESH subset mentioned in the Semra DB Paper, and slightly more classes
for the other reasources, since we are including the ancestor classes as well.
"""

from mapnet.utils import subset_from_obo

## define our subsets
subset_def = {
    # "mesh": {
    #     "version": "2025",
    #     "subset_identifiers": [
    #         "mesh:D007239",
    #         "mesh:D001520",
    #         "mesh:D011579",
    #         "mesh:D001523",
    #         "mesh:D004191",
    #     ],
    # },
    "efo": {"version": "3.76.0", "subset_identifiers": ["efo:0000408"]},
    # "ncit": {"version": "25.03c", "subset_identifiers": ["ncit:C2991"]},
    # "umls": {
    #     "version": "2024AB",
    #     "subset_identifiers": [
    #         "sty:T049",
    #         "sty:T047",
    #         "sty:T191",
    #         "sty:T050",
    #         "sty:T048",
    #     ],
    # },
}


if __name__ == "__main__":
    subset_from_obo(subset_def=subset_def)
