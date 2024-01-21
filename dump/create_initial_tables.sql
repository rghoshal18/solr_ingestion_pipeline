-- Use the 'solr_ingestion_metadata' database
USE solr_ingestion_metadata;

-- Create a table for tracking load metadata
CREATE TABLE load_metadata ( 
  id int unsigned AUTO_INCREMENT NOT NULL,         -- Unique identifier for each load
  load_log_key VARCHAR(50) DEFAULT NULL,           -- Key associated with the load log
  `STATUS` VARCHAR(50) DEFAULT 'NOT STARTED',      -- Status of the load process (default: 'NOT STARTED')
  PRIMARY KEY (id)                                 -- Define 'id' as the primary key
);

-- Create a table for entity metadata linked to load metadata
CREATE TABLE entity_metadata (
  id int unsigned AUTO_INCREMENT NOT NULL,           -- Unique identifier for each connection
  load_id int unsigned NOT NULL,                     -- Foreign key linking to load_metadata.id
  entity_name VARCHAR(50) DEFAULT NULL,              -- Name associated with the entity
  `STATUS` VARCHAR(50) DEFAULT 'NOT STARTED',        -- Status of the entity (default: 'NOT STARTED')
  PRIMARY KEY (id),                                  -- Define 'id' as the primary key
  FOREIGN KEY (load_id) REFERENCES load_metadata(id) -- Establish a foreign key relationship with load_metadata.id
);
