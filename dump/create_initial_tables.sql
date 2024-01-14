USE solr_ingestion_metadata;
CREATE TABLE load_metadata ( 
  id int unsigned AUTO_INCREMENT NOT NULL, 
  llk VARCHAR(50) DEFAULT NULL, 
  `STATUS` VARCHAR(50) DEFAULT 'NOT STARTED',
  PRIMARY KEY (id)
);