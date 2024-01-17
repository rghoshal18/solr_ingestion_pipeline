USE solr_ingestion_metadata;
CREATE TABLE load_metadata ( 
  id int unsigned AUTO_INCREMENT NOT NULL, 
  llk VARCHAR(50) DEFAULT NULL, 
  `STATUS` VARCHAR(50) DEFAULT 'NOT STARTED',
  PRIMARY KEY (id)
);

CREATE TABLE connection_metadata ( 
  id int unsigned AUTO_INCREMENT NOT NULL, 
  load_id int unsigned NOT NULL,
  connection_name VARCHAR(50) DEFAULT NULL, 
  `STATUS` VARCHAR(50) DEFAULT 'NOT STARTED',
  PRIMARY KEY (id),
  FOREIGN KEY (load_id) REFERENCES solr_ingestion_metadata.id
);