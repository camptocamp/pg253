-- Big DB
CREATE DATABASE big;
\c big;
CREATE TABLE data(
  id   serial,
  data text
);

-- Generate a 5Go DB
INSERT INTO data (data) SELECT g.id::text || 'Some more data to fill the DB' FROM generate_series(1, (6000000 / 438) * 5000) AS g (id);

-- Application DB
CREATE DATABASE application;
\c application;


CREATE OR REPLACE FUNCTION create_some_tables(tables_count integer, row_count integer) RETURNS void AS $$
BEGIN
	FOR i IN 1..tables_count LOOP
      EXECUTE 'CREATE TABLE data' || i || '(id serial, data text)';
      EXECUTE 'INSERT INTO data' || i || ' (data) SELECT g.id::text || ''Some more data to fill the DB'' FROM generate_series(1, ' || row_count || ') AS g (id)';
	END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT create_some_tables(1000, 10000);

-- Data DB
CREATE DATABASE data;
\c data;

CREATE TABLE data(
  id   serial,
  data text
);

-- Generate a 500Mo DB
INSERT INTO data (data) SELECT g.id::text || 'Some more data to fill the DB' FROM generate_series(1, (6000000 / 438) * 500) AS g (id);
