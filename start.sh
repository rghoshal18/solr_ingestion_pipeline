docker-compose -f docker-compose-solr-ingestion-pipeline.yml up -d --scale worker=3
#echo $(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' solr_ingestion_pipeline-mysql-1)


## Creating Solr cores for the connections
connections_config="./connections_config.csv"
awk -F',' 'NR > 1 { printf "docker exec solr_ingestion_pipeline-solr-1 solr create -c %s\n", $1, $2 }' "$connections_config" | bash
