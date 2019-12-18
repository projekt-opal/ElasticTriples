# ElasticTriples

Elasticsearch powered triple storage.

An import of around 90 million triples can be performed in around 77 minutes.  
(89,902,895 triples; 10.3 GB in Turtle format; 16.3 GB in N-Triples format; import time: 4642.451 seconds)

A search query takes around 2-3 seconds. E.g. extracting one (out of a million) DCAT-dataset with 206 triples uses 2,281 queries inside 3 multi-queries. 

## Credits

[Data Science Group (DICE)](https://dice-research.org/) at [Paderborn University](https://www.uni-paderborn.de/)

This work has been supported by the German Federal Ministry of Transport and Digital Infrastructure (BMVI) in the project [Open Data Portal Germany (OPAL)](http://projekt-opal.de/) (funding code 19F2028A).
