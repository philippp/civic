/* Public Record Table */
DROP TABLE IF EXISTS record_to_block_lot;
DROP TABLE IF EXISTS record_to_entity;
DROP TABLE IF EXISTS alias_to_entity;
DROP TABLE IF EXISTS entity;
DROP TABLE IF EXISTS record;

/* A single public record. */
CREATE TABLE record
(
  id CHAR(12) NOT NULL,  /* Document ID */
  record_date DATE NOT NULL,
  record_type VARCHAR(255) NOT NULL,
  grantees_raw VARCHAR(255),
  grantors_raw VARCHAR(255),
  reel_image VARCHAR(255),
  PRIMARY KEY(id)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8;

/* Map of records to block_lots */
CREATE TABLE record_to_block_lot
(
  record_id CHAR(12) NOT NULL,
  block_lot CHAR(10) NOT NULL,
  PRIMARY KEY(record_id, block_lot),
  FOREIGN KEY(record_id) REFERENCES record(id),
  INDEX by_record_id(record_id),
  INDEX by_block_lot(block_lot)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8;

CREATE TABLE entity
(
  id BIGINT NOT NULL AUTO_INCREMENT,  
  best_name VARCHAR(255),
  PRIMARY KEY(id)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8;

CREATE TABLE record_to_entity
(
  record_id CHAR(12) NOT NULL,
  entity_id BIGINT NOT NULL,
  relation ENUM('grantors', 'grantees') NOT NULL,
  PRIMARY KEY(record_id, entity_id, relation),
  INDEX by_record(record_id),
  INDEX by_entity(entity_id),
  FOREIGN KEY(record_id) REFERENCES record(id),
  FOREIGN KEY(entity_id) REFERENCES entity(id)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8;

CREATE TABLE alias_to_entity
(
  entity_id BIGINT NOT NULL,
  alias_name VARCHAR(255),
  FOREIGN KEY(entity_id) REFERENCES entity(id)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8;