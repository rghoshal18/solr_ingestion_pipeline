echo "Starting Application"
num_workers=$1
docker-compose -f docker-compose-solr-ingestion-pipeline.yml up -d --scale worker=${num_workers}
echo "Application Started"

## Creating Solr cores for the connections
echo "Creating solr cores for connection"
entities_config="./entities_config.csv"
awk -F',' 'NR > 1 { printf "docker exec solr_ingestion_pipeline-solr-1 solr create -c %s\n", $1, $2 }' "$entities_config" | bash
echo "Cores created on solr"