/* Address Table */
DROP TABLE IF EXISTS eviction_to_blocklot;
CREATE TABLE eviction_to_blocklot
(
  petition VARCHAR(255) NOT NULL,
  block_lot char(10),
  INDEX by_blklot(block_lot),
  PRIMARY KEY (petition, block_lot)
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8;

