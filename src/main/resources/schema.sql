
DROP TABLE IF EXISTS foo;

CREATE TABLE foo (
  id INT NOT NULL,
  text VARCHAR(50) NOT NULL,
  PRIMARY KEY (id)
);
 
DROP TABLE IF EXISTS foo_seq;

CREATE TABLE foo_seq (
  next_id INT NOT NULL,
  PRIMARY KEY (next_id)
);

INSERT INTO foo_seq (next_id) values(1);

DROP TABLE IF EXISTS bar;

CREATE TABLE bar (
  id INT NOT NULL,
  text VARCHAR(50) NOT NULL,
  PRIMARY KEY (id)
);
 
DROP TABLE IF EXISTS bar_seq;

CREATE TABLE bar_seq (
  name VARCHAR(10) NOT NULL,
  next_id INT NOT NULL,
  PRIMARY KEY (name)
);


DROP TABLE IF EXISTS baz;

CREATE TABLE baz (
  id decimal(9,0) NOT NULL,
  code decimal(3,0) NOT NULL,
  language_code decimal(3,0) NOT NULL,
  text VARCHAR(50) NOT NULL,
  PRIMARY KEY (id, code, language_code)
);
 
DROP TABLE IF EXISTS baz_seq;

CREATE TABLE baz_seq (
  code VARCHAR(3) NOT NULL,
  language_code VARCHAR(3) NOT NULL,
  next_id INT NOT NULL,
  PRIMARY KEY (code, language_code)
);


