Elasticsearch/Solr Connector
=======================

**Note**: This connector is still in beta

Setup
-----

```shell
curl -fsSL https://raw.github.com/algolia/elasticsearch-solr-connector/master/dist/elasticsearch-solr-connector.sh > elasticsearch-solr-connector.sh
```

Usage
-----

```shell
iusage: Elasticsearchconnector [option]...
 -d,--debug             Activate the debug mode
 -h,--help              Print help
 -i,--indexName <arg>   Name of the output index.
    --input <arg>       ES or SOLR
 -p,--apiKey <arg>      The api key.
    --params <arg>      Parameters for the input.
 -u,--appID <arg>       The application ID.
```

The elasticsearch connector needs 4 parameters: ```URL|PORT|CLUSTER|INDEX```

The solr connector needs 1 parameter: ```URL```
