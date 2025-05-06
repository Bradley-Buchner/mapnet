"""This will download the required ontologies, filter Mesh to primary diseases, and find mappings between the two provided in DOID"""

from mapnet.utils import download_owl
from mapnet.bertmap import filter_non_diseases, filter_supplemental
from deeponto.onto import Ontology
import os


def get_doid_provided_maps():
    """Extract mappings between MESH and DOID provided with the DOID OWL file."""
    doid_path = "resources/doid.owl"
    doid = Ontology(doid_path)
    mesh_iri_base = "http://purl.bioontology.org/ontology/"
    provided_maps = {}
    mesh_map = lambda x: mesh_iri_base + x.replace(":", "/")
    for iri in doid.owl_classes.keys():
        for annotation in doid.get_annotations(iri):
            if annotation.startswith("MESH:"):
                provided_maps[iri] = mesh_map(annotation.split(" ")[0])
    os.makedirs("knownMaps", exist_ok=True)
    with open("knownMaps/doid_to_mesh_provided_maps.tsv", mode="w") as f:
        f.write("SrcEntity\tTgtEntity\tScore\n")
        for doid_iri in provided_maps:
            f.write(f"{doid_iri}\t{provided_maps[doid_iri]}\t1.0\n")


def main():
    """Main method for running this script. Made a function, since may be helpful to produce these artifacts elsewhere"""
    ## download the ontologies.
    download_owl(
        target_ontology_train="mesh",
        source_ontology_train="doid",
        source_ontologies_inference=[],
        target_ontologies_inference=[],
        ontologies_path="resources",
    )
    ## filter the terms
    mesh_path = "resources/mesh.ttl"
    primary_path = "resources/mesh_primary.owl"
    mesh_disease_path = "resources/mesh_primary_disease.owl"
    print("filtering supplemental terms ")
    filter_supplemental(mesh_path=mesh_path, save_path=primary_path)
    print("filtering diseases")
    filter_non_diseases(mesh_path=primary_path, save_path=mesh_disease_path)
    print("getting ontology provided maps")
    get_doid_provided_maps()


if __name__ == "__main__":
    main()
