""" "
Re-run the matching of DOID to MESH using Logmap.


"""
from os.path import join
from os import getcwd
from mapnet.utils import convert_onto_format, get_directionial_onto_subset, get_onto_subset
from mapnet.logmap import build_image, run_container
tag = '0.01'
reasources_path = join(getcwd(), "resources")
# target_onto_file = 'mesh_2025_subset.obo'
# target_onto_file = 'mesh_robot_subset.owl'
target_onto_file = 'resources/mesh_direct.obo'
source_onto_file = 'doid.obo'
source_onto_file = 'resources/doid.owl'
# target_onto_file = 'doid.obo'
dataset_def = {
    "mesh": {
        "version": "2025",
        "subset_identifiers": [
            "D007239",
            "D001520",
            "D011579",
            "D001523",
            "D004191",
        ],
    },
    "DOID": {"version": "2025-03-03", "subset_identifiers": []},
}

if __name__ == "__main__":
    get_onto_subset(prefix="mesh", onto_path=target_onto_file, subset_identifiers=dataset_def['mesh']['subset_identifiers'])
    # get_directionial_onto_subset(prefix="mesh", onto_path=target_onto_file, subset_identifiers=dataset_def['mesh']['subset_identifiers'])
    # get_directionial_onto_subset(prefix="mesh",ancestors=True, onto_path=target_onto_file, subset_identifiers=dataset_def['mesh']['subset_identifiers'])
    # convert_onto_format(input_file=target_onto_file, desired_format='obo')
    # convert_onto_format(input_file=source_onto_file, desired_format='obo')
    ## download mesh and doid obo files and subset mesh to only contain disseases
    # subset_from_obo(subset_def=dataset_def)
    ## build image for logmap from docker file 
    # build_image(tag = tag)
    ## run the matching 
    # run_container(tag = tag, target_onto_file = target_onto_file, source_onto_file = source_onto_file, resources_path = reasources_path)
