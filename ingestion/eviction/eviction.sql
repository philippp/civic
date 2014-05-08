DROP TABLE IF EXISTS eviction;

CREATE TABLE eviction
(
  date_filed DATE NOT NULL,
  petition VARCHAR(255) NOT NULL,
  landlord_names VARCHAR(255),
  address VARCHAR(255) NOT NULL,
  block_lot char(10),
  eviction_type ENUM('ellis', 'omi') NOT NULL,
  senior_disabled_count SMALLINT,
  unit_count SMALLINT,
  PRIMARY KEY(petition)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8;