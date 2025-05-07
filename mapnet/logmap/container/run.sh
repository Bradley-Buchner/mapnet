#!/bin/sh  
java -jar \
    -Xmx32g \
    --add-opens java.base/java.lang=ALL-UNNAMED \
    /package/logmap/logmap-matcher-4.0.jar \
    MATCHER \
    file:///package/resources/doid.owl \
    file:///package/resources/mesh_primary_disease.owl \
    /package/output/ \
    false ## classify input ontologies along with mapping?

