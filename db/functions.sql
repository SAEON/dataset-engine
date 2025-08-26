-- Function for querying the ocean dataset cells as mvt (mapbox vector tiles).

CREATE OR REPLACE
    FUNCTION get_ocean_data_tile(z integer, x integer, y integer, query jsonb)
    RETURNS bytea AS $$
DECLARE
  mvt bytea;
  dataset_table_name TEXT;
  sql_query TEXT;
BEGIN
  dataset_table_name := (query ->> 'dataset_id') || '_ocean_dataset_data';

  sql_query := format('
    SELECT ST_AsMVT(tile, ''get_ocean_data_tile'', 4096, ''geom'') FROM (
      SELECT
        ST_AsMVTGeom(
          ST_Transform(cell_points, 3857),
          ST_TileEnvelope($1, $2, $3)
        ) AS geom,
        dataset_id,
        temperature,
        salinity,
        u_velocity,
        v_velocity
      FROM %I
      WHERE
        date_time = $4::timestamp WITH TIME ZONE
        AND depth = $5::float
    ) AS tile
  ', dataset_table_name);

  EXECUTE sql_query
  INTO mvt
  USING z, x, y, (query ->> 'date_time'), (query ->> 'depth');

  RETURN mvt;
END
$$
LANGUAGE plpgsql;

-- ################################################################################################

CREATE OR REPLACE FUNCTION switch_tables(
    current_table_name TEXT,
    temp_table_name TEXT
) RETURNS VOID AS $$
DECLARE
    idx_name TEXT;
    pk_name TEXT;
    seq_name TEXT;
BEGIN
    EXECUTE 'LOCK TABLE ' || current_table_name || ', ' || temp_table_name || ' IN ACCESS EXCLUSIVE MODE';

    EXECUTE 'SELECT c.conname FROM pg_constraint c JOIN pg_class r ON c.conrelid = r.oid WHERE r.relname = ' || quote_literal(temp_table_name) || ' AND c.contype = ''p''' INTO pk_name;

    EXECUTE 'SELECT pg_get_serial_sequence(' || quote_literal(temp_table_name) || ', ' || quote_literal('id') || ')' INTO seq_name;

    EXECUTE 'DROP TABLE IF EXISTS ' || current_table_name || ' CASCADE';

    EXECUTE 'ALTER TABLE ' || temp_table_name || ' RENAME TO ' || current_table_name;

    IF pk_name IS NOT NULL THEN
        EXECUTE 'ALTER TABLE ' || current_table_name || ' RENAME CONSTRAINT ' || pk_name || ' TO ' || current_table_name || '_pkey';
       pk_name := current_table_name || '_pkey';
    END IF;

    FOR idx_name IN
        SELECT indexname
        FROM pg_indexes
        WHERE tablename = current_table_name
        AND indexname != pk_name
    LOOP
        EXECUTE 'ALTER INDEX ' || idx_name || ' RENAME TO ' || replace(idx_name, temp_table_name, current_table_name);
    END LOOP;

    IF seq_name IS NOT NULL THEN
        EXECUTE 'ALTER SEQUENCE ' || seq_name || ' RENAME TO ' || current_table_name || '_id_seq';
    END IF;
END;
$$ LANGUAGE plpgsql;
