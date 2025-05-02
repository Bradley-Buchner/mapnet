"""Method for subsetting ontologies in the same way as was done in the landscape analysis and saving the ressults. 
Produces the same number of classes as the MESH subset mentioned in the Semra DB Paper"""
import pyobo
from pyobo.sources.mesh import get_mesh_category_references
from semra import Reference
import networkx as nx
## define our subsets 

SUBSETS = {
            "mesh": [*get_mesh_category_references("C"), *get_mesh_category_references("F")],
                "efo": [Reference.from_curie("efo:0000408")],
                    "ncit": [Reference.from_curie("ncit:C2991")],
                        "umls": [
                                    # all children of https://uts.nlm.nih.gov/uts/umls/semantic-network/Pathologic%20Function
                                            Reference.from_curie("sty:T049"),  # cell or molecular dysfunction
                                                    Reference.from_curie("sty:T047"),  # disease or syndrome
                                                            Reference.from_curie("sty:T191"),  # neoplastic process
                                                                    Reference.from_curie("sty:T050"),  # experimental model of disease
                                                                            Reference.from_curie("sty:T048"),  # mental or behavioral dysfunction
                                                                                ],
                        }
subset_identifiers = ['mesh:D007239', 'mesh:D001520', 'mesh:D011579', 'mesh:D001523','mesh:D004191',]
ontology = 'mesh'
version = 'disease-subset'
save_path = f'{ontology}_{version}.obo'
## helpder functions 

def get_subset_graph(full_graph, subset_identifiers:list):
    relations = set()
    for ref in subset_identifiers:
        relations = relations | set(nx.ancestors(full_graph, ref)) | {ref} | set(nx.descendants(full_graph, ref)) 
    subset_graph = full_graph.subgraph(relations).copy()
    return subset_graph

def subset_graph_to_obo(subset_graph, ontology, version:str='subset', save_path:str = "mesh_subset.obo"):
    subset_graph.graph['ontology'] = ontology
    subset_obo = pyobo.from_obonet(graph = subset_graph, version = version)
    subset_obo.write_obo(save_path)
    return subset_obo

if __name__ == "__main__":
    obo = pyobo.get_ontology('mesh', version='2025') 
    full_graph = obo.get_graph().get_networkx()
    subset_graph = get_subset_graph(full_graph=full_graph, subset_identifiers=subset_identifiers)
    subset_obo = subset_graph_to_obo(subset_graph=subset_graph, ontology=ontology, version=version, save_path=save_path)
    
    
    

