DROP TRIGGER IF EXISTS trigger_flag_transportation ON osm_highway_linestring;
DROP TRIGGER IF EXISTS trigger_refresh ON transportation.updates;

-- Instead of using relations to find out the road names we
-- stitch together the touching ways with the same name
-- to allow for nice label rendering
-- Because this works well for roads that do not have relations as well

-- Improve performance of the sql in transportation/update_route_member.sql
CREATE INDEX IF NOT EXISTS osm_highway_linestring_highway_partial_idx
    ON osm_highway_linestring (highway)
    WHERE highway IN ('motorway', 'trunk');

-- etldoc: osm_highway_linestring ->  osm_transportation_merge_linestring_gen_z11
DROP MATERIALIZED VIEW IF EXISTS osm_transportation_merge_linestring_gen_z11 CASCADE;
CREATE MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z11 AS
(
SELECT (ST_Dump(ST_LineMerge(ST_Collect(ST_Simplify(geometry, ZRes(12)))))).geom AS geometry,
       NULL::bigint AS osm_id,
       highway,
       network,
       construction,
       is_bridge,
       min(z_order) as z_order,
       CASE
           WHEN access IN ('private', 'no') THEN 'no'
           ELSE NULL::text END AS access,
       transportation_tags
         - 'ramp'::text
         - 'oneway'::text
         AS tags
FROM osm_highway_linestring
WHERE (highway IN ('motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'tertiary_link')
   OR construction IN ('motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'tertiary_link'))
AND NOT is_area
AND ST_IsValid(geometry)
GROUP BY highway, network, construction, is_bridge, access, transportation_tags
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z11_geometry_idx
    ON osm_transportation_merge_linestring_gen_z11 USING gist (geometry);

-- etldoc: osm_transportation_merge_linestring_gen_z11 -> osm_transportation_merge_linestring_gen_z10
CREATE MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z10 AS
(
SELECT ST_Simplify(geometry, ZRes(12)) AS geometry,
       osm_id,
       highway,
       network,
       construction,
       is_bridge,
       z_order,
       access,
       tags
FROM osm_transportation_merge_linestring_gen_z11
WHERE highway NOT IN ('tertiary', 'tertiary_link', 'busway')
      AND construction NOT IN ('tertiary', 'tertiary_link', 'busway')
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z10_geometry_idx
    ON osm_transportation_merge_linestring_gen_z10 USING gist (geometry);

-- etldoc: osm_transportation_merge_linestring_gen_z10 -> osm_transportation_merge_linestring_gen_z9
CREATE MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z9 AS
(
SELECT ST_Simplify(geometry, ZRes(11)) AS geometry,
       osm_id,
       highway,
       network,
       construction,
       is_bridge,
       z_order,
       access,
       tags
FROM osm_transportation_merge_linestring_gen_z10
     -- Current view: motorway/primary/secondary, with _link variants and construction 
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z9_geometry_idx
    ON osm_transportation_merge_linestring_gen_z9 USING gist (geometry);

-- etldoc: osm_transportation_merge_linestring_gen_z9 ->  osm_transportation_merge_linestring_gen_z8
CREATE MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z8 AS
(
SELECT ST_Simplify(ST_LineMerge(ST_Collect(geometry)), ZRes(10)) AS geometry,
       NULL::bigint AS osm_id,
       highway,
       network,
       construction,
       is_bridge,
       min(z_order) as z_order,
       tags
         - 'bicycle'::text
         - 'foot'::text
         - 'horse'::text
         - 'layer'::text
         - 'mtb_scale'::text
         - 'toll'::text
         AS tags
FROM osm_transportation_merge_linestring_gen_z9
WHERE (highway IN ('motorway', 'trunk', 'primary') OR
       construction IN ('motorway', 'trunk', 'primary'))
       AND ST_IsValid(geometry)
       AND access IS NULL
GROUP BY highway, network, construction, is_bridge, tags
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z8_geometry_idx
    ON osm_transportation_merge_linestring_gen_z8 USING gist (geometry);

-- etldoc: osm_transportation_merge_linestring_gen_z8 -> osm_transportation_merge_linestring_gen_z7
CREATE MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z7 AS
(
SELECT ST_Simplify(geometry, ZRes(9)) AS geometry,
       osm_id,
       highway,
       network,
       construction,
       is_bridge,
       z_order,
       tags
FROM osm_transportation_merge_linestring_gen_z8
     -- Current view: motorway/trunk/primary
WHERE ST_Length(geometry) > 50
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z7_geometry_idx
    ON osm_transportation_merge_linestring_gen_z7 USING gist (geometry);

-- etldoc: osm_transportation_merge_linestring_gen_z7 -> osm_transportation_merge_linestring_gen_z6
CREATE MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z6 AS
(
SELECT ST_Simplify(geometry, ZRes(8)) AS geometry,
       osm_id,
       highway,
       network,
       construction,
       is_bridge,
       z_order,
       tags
FROM osm_transportation_merge_linestring_gen_z7
WHERE (highway IN ('motorway', 'trunk') OR construction IN ('motorway', 'trunk'))
  AND ST_Length(geometry) > 100
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z6_geometry_idx
    ON osm_transportation_merge_linestring_gen_z6 USING gist (geometry);

-- etldoc: osm_transportation_merge_linestring_gen_z6 -> osm_transportation_merge_linestring_gen_z5
CREATE MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z5 AS
(
SELECT ST_Simplify(geometry, ZRes(7)) AS geometry,
       osm_id,
       highway,
       network,
       construction,
       is_bridge,
       z_order,
       tags
FROM osm_transportation_merge_linestring_gen_z6
WHERE ST_Length(geometry) > 500
     -- Current view: motorway/trunk
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z5_geometry_idx
    ON osm_transportation_merge_linestring_gen_z5 USING gist (geometry);

-- etldoc: osm_transportation_merge_linestring_gen_z5 -> osm_transportation_merge_linestring_gen_z4
CREATE MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z4 AS
(
SELECT ST_Simplify(geometry, ZRes(6)) AS geometry,
       osm_id,
       highway,
       network,
       construction,
       is_bridge,
       z_order,
       tags
FROM osm_transportation_merge_linestring_gen_z5
WHERE (highway = 'motorway' OR construction = 'motorway')
  AND ST_Length(geometry) > 1000
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_transportation_merge_linestring_gen_z4_geometry_idx
    ON osm_transportation_merge_linestring_gen_z4 USING gist (geometry);

-- etldoc: osm_railway_linestring -> osm_railway_linestring_gen_z12
CREATE MATERIALIZED VIEW osm_railway_linestring_gen_z12 AS
(
SELECT (ST_Dump(ST_LineMerge(ST_Collect(ST_Simplify(geometry, ZRes(13)))))).geom AS geometry,
       railway,
       usage,
       is_bridge,
       z_order,
       transportation_tags AS tags
FROM osm_railway_linestring
WHERE railway IN ('rail', 'narrow_gauge', 'light_rail')
  AND service=''
  AND ST_IsValid(geometry)
GROUP BY railway, usage, is_bridge, z_order, transportation_tags
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_railway_linestring_gen_z12_geometry_idx
    ON osm_railway_linestring_gen_z12 USING gist (geometry);

-- etldoc: osm_railway_linestring_gen_z11 -> osm_railway_linestring_gen_z12
CREATE MATERIALIZED VIEW osm_railway_linestring_gen_z11 AS
(
SELECT ST_Simplify(geometry, ZRes(12)) AS geometry,
       railway,
       usage,
       is_bridge,
       z_order,
       tags
FROM osm_railway_linestring_gen_z12
-- Inherited: railway IN ('rail', 'narrow_gauge', 'light_rail')
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_railway_linestring_gen_z11_geometry_idx
    ON osm_railway_linestring_gen_z11 USING gist (geometry);

-- etldoc: osm_railway_linestring_gen_z10 -> osm_railway_linestring_gen_z11
CREATE MATERIALIZED VIEW osm_railway_linestring_gen_z10 AS
(
SELECT ST_Simplify(geometry, ZRes(11)) AS geometry,
       railway,
       usage,
       is_bridge,
       z_order,
       tags
FROM osm_railway_linestring_gen_z11
WHERE railway IN ('rail', 'narrow_gauge')
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_railway_linestring_gen_z10_geometry_idx
    ON osm_railway_linestring_gen_z10 USING gist (geometry);

-- etldoc: osm_railway_linestring_gen_z9 -> osm_railway_linestring_gen_z10
CREATE MATERIALIZED VIEW osm_railway_linestring_gen_z9 AS
(
SELECT ST_Simplify(geometry, ZRes(10)) AS geometry,
       railway,
       usage,
       is_bridge,
       z_order,
       tags
         - 'tunnel'::text
         - 'ford'::text
         - 'ramp'::text
         - 'oneway'::text
         AS tags
FROM osm_railway_linestring_gen_z10
WHERE railway = 'rail'
  AND usage = 'main'
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_railway_linestring_gen_z9_geometry_idx
    ON osm_railway_linestring_gen_z9 USING gist (geometry);

-- etldoc: osm_railway_linestring_gen_z8 -> osm_railway_linestring_gen_z9
CREATE MATERIALIZED VIEW osm_railway_linestring_gen_z8 AS
(
SELECT ST_Simplify(geometry, ZRes(9)) AS geometry,
       railway,
       usage,
       is_bridge,
       z_order,
       tags
         - 'layer'::text
         AS tags
FROM osm_railway_linestring_gen_z9
-- Inherited: WHERE railway = 'rail' AND usage = 'main'
    ) /* DELAY_MATERIALIZED_VIEW_CREATION */;
CREATE INDEX IF NOT EXISTS osm_railway_linestring_gen_z8_geometry_idx
    ON osm_railway_linestring_gen_z8 USING gist (geometry);

-- Handle updates

CREATE SCHEMA IF NOT EXISTS transportation;

CREATE TABLE IF NOT EXISTS transportation.updates
(
    id serial PRIMARY KEY,
    t text,
    UNIQUE (t)
);
CREATE OR REPLACE FUNCTION transportation.flag() RETURNS trigger AS
$$
BEGIN
    INSERT INTO transportation.updates(t) VALUES ('y') ON CONFLICT(t) DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION transportation.refresh() RETURNS trigger AS
$$
DECLARE
    t TIMESTAMP WITH TIME ZONE := clock_timestamp();
BEGIN
    RAISE LOG 'Refresh transportation';
    UPDATE osm_highway_linestring
      SET transportation_tags =
      (
        highway_linestring_tag_base(tags)
        || hstore(ARRAY[
            ['tunnel', is_tunnel::text],
            ['ramp',   is_ramp::text],
            ['ford',   is_ford::text],
            ['oneway', is_oneway::text],
            ['toll',   toll::text],
            ['layer',  layer::text]
        ])
      )
      -- Remove null/default values
      - highway_linestring_tag_discardable()
      WHERE osm_id IN (SELECT id FROM transportation.updates);

    REFRESH MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z11;
    REFRESH MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z10;
    REFRESH MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z9;
    REFRESH MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z8;
    REFRESH MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z7;
    REFRESH MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z6;
    REFRESH MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z5;
    REFRESH MATERIALIZED VIEW osm_transportation_merge_linestring_gen_z4;
    -- noinspection SqlWithoutWhere
    DELETE FROM transportation.updates;

    RAISE LOG 'Refresh transportation done in %', age(clock_timestamp(), t);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_flag_transportation
    AFTER INSERT OR UPDATE OR DELETE
    ON osm_highway_linestring
    FOR EACH STATEMENT
EXECUTE PROCEDURE transportation.flag();

CREATE CONSTRAINT TRIGGER trigger_refresh
    AFTER INSERT
    ON transportation.updates
    INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE transportation.refresh();
