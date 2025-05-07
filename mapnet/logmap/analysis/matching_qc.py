import polars as pl
from mapnet.utils import format_mappings, get_novel_mappings, get_current_date_ymd
import obonet
from indra.databases import mesh_client

def permissive_map(x):
    """a function to get the name of a given DOID entity, will return 'NO NAME FOUND', if the entity does not exist."""
    try:
        return g.nodes[x]["name"]
    except:
        return "NO NAME FOUND"
    
if __name__ == "__main__":

    g = obonet.read_obo(
        "https://raw.githubusercontent.com/DiseaseOntology/"
        "HumanDiseaseOntology/main/src/ontology/HumanDO.obo"
    )
    maps_path = "mapnet/logmap-matcher/output/logmap2_mappings.tsv"
    raw_maps = pl.read_csv(
        maps_path,
        separator="\t",
        has_header=False,
        new_columns=['SrcEntity', 'TgtEntity', 'Score'],
    )
    predicted_mappings = format_mappings(
        df=raw_maps,
        source_prefix="DOID",
        target_prefix="MESH",
        matching_source="LogMap",
        source_name_func=permissive_map,
        target_name_func=mesh_client.get_mesh_name,
    )
    novel_mappings = get_novel_mappings(
        predicted_mappings=predicted_mappings,
        target_prefix="MESH",
        source_prefix="DOID",
        source_name_func=permissive_map,
        target_name_func=mesh_client.get_mesh_name,
        known_mappings_path="knownMaps/doid_to_mesh_provided_maps.tsv",
    )
    novel_mappings.write_csv(
        f"doid_mesh_mappings_logmap_{get_current_date_ymd()}.tsv", separator="\t"
    )
