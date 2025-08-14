-- Function for querying the ocean dataset cells as mvt (mapbox vector tiles).

CREATE OR REPLACE
    FUNCTION get_ocean_data_tile(z integer, x integer, y integer, query jsonb)
    RETURNS bytea AS $$
DECLARE
  mvt bytea;
BEGIN
  SELECT INTO mvt ST_AsMVT(tile, 'get_ocean_data_tile', 4096, 'geom') FROM (
    SELECT
      ST_AsMVTGeom(
            ST_Transform(cell_points, 3857), -- Transform to Web Mercator (3857)
            ST_TileEnvelope(z, x, y)
        ) AS geom,
      dataset_id,
      temperature,
      salinity,
      u_velocity,
      v_velocity
    FROM
      ocean_dataset_data
    WHERE
      date_time = (query ->> 'date_time')::timestamp WITH TIME ZONE
      AND depth = (query ->> 'depth')::float
  ) as tile;

  RETURN mvt;
END
$$
LANGUAGE plpgsql;