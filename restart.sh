docker-compose -f docker-compose-solr-ingestion-pipeline.yml down
docker-compose -f docker-compose-solr-ingestion-pipeline.yml up -d

echo $(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' solr_ingestion_pipeline-mysql-1)