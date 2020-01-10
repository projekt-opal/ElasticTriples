# ElasticTriples

Elasticsearch powered triple storage.


## Preparation: Elasticsearch installation

To use ElasticTriples, you have to install Elasticsearch.
That can by done directly or via Docker:  
 
* [Installing Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html)
* [Install Elasticsearch with Docker](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html)


## Preparation: Big data

If you want to process big files, the data should be available in N-Triples format (instead of e.g. Turtle).
A software for that is [RDF2RDF](https://github.com/knakk/rdf2rdf).
After installing it, use `{GOPATH}/bin/rdf2rdf -in=in.ttl -out=out.nt` to transform you data.


## Import data

An import of around 90 million triples can be performed in around 77 minutes
(89,902,895 triples; 10.3 GB in Turtle format; 16.3 GB in N-Triples format; import time: 4642.451 seconds).
A code example is given in [OpalImport.java](src/main/java/org/dice_research/opal/elastictriples/opal/OpalImport.java).


## Query data

A search query takes around 2-3 seconds. E.g. extracting one (out of a million) DCAT-dataset with 206 triples uses 2,281 queries inside 3 multi-queries. 
A code example is given in [OpalQuery.java](src/main/java/org/dice_research/opal/elastictriples/opal/OpalQuery.java).


## Split data

Splitting data is done by requesting single dataset graphs (each 2-3 seconds) and writing the resulting data no files in N-Triples format.
A code example is given in [OpalSplitter.java](src/main/java/org/dice_research/opal/elastictriples/opal/OpalSplitter.java).


## Filter data

Data can be filtered based on language tags of title literals.
A code example is given in [OpalSplitter.java](src/main/java/org/dice_research/opal/elastictriples/opal/OpalFilter.java).


## Credits

[Data Science Group (DICE)](https://dice-research.org/) at [Paderborn University](https://www.uni-paderborn.de/)

This work has been supported by the German Federal Ministry of Transport and Digital Infrastructure (BMVI) in the project [Open Data Portal Germany (OPAL)](http://projekt-opal.de/) (funding code 19F2028A).
