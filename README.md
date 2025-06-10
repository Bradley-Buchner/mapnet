MapNet
======
Algorithms for semantic mappings between different ontologies.

Funding
-------
The development of MapNet is funded under ARO grant HR00112220036 as part of the DARPA ASKEM and ARPA-H BDF programs.

Usage
----
### BERTMap
- Ontology matching leveraging [BERTMap](https://arxiv.org/abs/2112.02682) models. Implemented using [DeepOnto](https://krr-oxford.github.io/DeepOnto/bertmap/).
- For a usage example see `scripts/bertmap_run.py`.

### LogMap 
- Ontology matching leveraging the [LogMap](https://link.springer.com/chapter/10.1007/978-3-642-25073-6_18) matching system. Leverages java implementation available on Github at [ernestojimenezruiz/logmap-matcher](https://github.com/ernestojimenezruiz/logmap-matcher)
- For usage examples see `scripts/logmap_disease_landscape.py` and `scripts/logmap_doid_to_mesh.py`