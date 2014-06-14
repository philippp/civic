/* Address Table */
DROP TABLE IF EXISTS address;
CREATE TABLE address
(
  addr_n_suffix varchar(10),
  addr_num char(10),
  address varchar(255) NOT NULL,
  block_lot char(10),
  cnn char(10),
  eas_baseid char(10),
  eas_subid char(10),
  longitude double,
  latitude double,
  street_name varchar(255),
  street_type varchar(255),
  unit_num char(10),
  zipcode char(10),
  zoning_sim varchar(10),
  INDEX by_address(address),
  INDEX by_blklot(block_lot)
)
DEFAULT CHARSET=utf8;

