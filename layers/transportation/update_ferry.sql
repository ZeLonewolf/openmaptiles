DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z4;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z5;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z6;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z7;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z8;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z9;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z10;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z11;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z12_skeleton;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_parts;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_isect;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_clustered;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_cluster_coalesced;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_cluster_centroid;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_clustered;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_explode;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_dumppoints;

-- This sequence of tables coalesces colinear ferry sections

-- Step 1: Drop all ferry lines to points
-- etldoc: osm_shipway_linestring_gen_z12 -> osm_shipway_dumppoints
CREATE MATERIALIZED VIEW osm_shipway_dumppoints AS
SELECT 
  osm_id,
  ST_DumpPoints(geometry) AS dp,
  name
FROM osm_shipway_linestring_gen_z12;

-- Step 2: Extract point geometry and point position in the linestring
-- etldoc: osm_shipway_dumppoints -> osm_shipway_explode
CREATE MATERIALIZED VIEW osm_shipway_explode AS
SELECT
  osm_id,
  (dp).geom AS pt,
  (dp).path[1] As ptidx
FROM osm_shipway_dumppoints;

-- Step 3: Cluster groups of nearby points
-- etldoc: osm_shipway_explode -> osm_shipway_clustered
CREATE MATERIALIZED VIEW osm_shipway_clustered AS	
SELECT
  osm_id,
  pt,
  ptidx,
  ST_ClusterDBSCAN(pt, eps := 600, minpoints := 2) over () AS cid
FROM osm_shipway_explode;
  
-- Step 4: Compute center point of each cluster of points
-- etldoc: osm_shipway_clustered -> osm_shipway_cluster_centroid
CREATE MATERIALIZED VIEW osm_shipway_cluster_centroid AS
SELECT
  cid,
  ST_Centroid(ST_Collect(pt)) AS ctr
FROM osm_shipway_clustered
WHERE cid IS NOT NULL
GROUP BY cid;

-- Step 5: Replace all clustered points with a centroid point
-- etldoc: osm_shipway_cluster_centroid -> osm_shipway_cluster_coalesced
CREATE MATERIALIZED VIEW osm_shipway_cluster_coalesced AS
SELECT
  osm_id,
  COALESCE(clctr.ctr, cl.pt) AS pt,
  ptidx
FROM osm_shipway_clustered cl
LEFT OUTER JOIN osm_shipway_cluster_centroid clctr ON cl.cid = clctr.cid;

-- Step 6: Re-assemble linestrings with the new point positions
-- etldoc: osm_shipway_linestring_gen_z12 -> osm_shipway_linestring_clustered
-- etldoc: osm_shipway_cluster_coalesced -> osm_shipway_linestring_clustered
CREATE MATERIALIZED VIEW osm_shipway_linestring_clustered AS
SELECT 
  oscc.osm_id AS osm_id,
  ST_MakeLine(pt order by ptidx) AS geometry
FROM osm_shipway_cluster_coalesced oscc
LEFT OUTER JOIN osm_shipway_linestring_gen_z12 osl
  ON oscc.osm_id = osl.osm_id
GROUP BY oscc.osm_id;

-- Step 7: Iterate through each pair of ferry lines that have shared segments
--   and compute the portion that intersects
-- etldoc: osm_shipway_clustered -> osm_shipway_linestring_isect
-- etldoc: osm_shipway_linestring_clustered -> osm_shipway_linestring_isect
CREATE MATERIALIZED VIEW osm_shipway_linestring_isect AS
SELECT DISTINCT
  ST_Intersection(oslc1.geometry, oslc2.geometry) AS isect,
  oslc1.geometry AS geometry1,
  oslc2.geometry AS geometry2,
  MAX(ST_Length(osl1.geometry)
      ST_Length(osl2.geometry)) AS max_length
FROM osm_shipway_clustered osc1
JOIN osm_shipway_clustered osc2 ON osc1.cid = osc2.cid AND osc1.osm_id < osc2.osm_id
JOIN osm_shipway_linestring_clustered oslc1 ON osc1.osm_id = oslc1.osm_id
JOIN osm_shipway_linestring_clustered oslc2 ON osc2.osm_id = oslc2.osm_id
JOIN osm_shipway_linestring_gen_z12 osl1 ON osl1.osm_id = osc1.osm_id
JOIN osm_shipway_linestring_gen_z12 osl2 ON osl2.osm_id = osc2.osm_id;

-- Step 8: Collect the intersection and difference segments
-- etldoc: osm_shipway_linestring_isect -> osm_shipway_linestring_parts
CREATE MATERIALIZED VIEW osm_shipway_linestring_parts AS
  SELECT DISTINCT geometry_part FROM (
    SELECT
      isect AS geometry_part
    FROM osm_shipway_linestring_isect
    WHERE ST_Length(isect) > 0
    UNION ALL
    SELECT
      ST_Difference(geometry1, isect) AS geometry_part
    FROM osm_shipway_linestring_isect
    WHERE ST_Length(isect) > 0
    UNION ALL
    SELECT
      ST_Difference(geometry2, isect) AS geometry_part
    FROM osm_shipway_linestring_isect
    WHERE ST_Length(isect) > 0
  ) geometry_part_collection;

-- etldoc: osm_shipway_cluster_coalesced -> osm_shipway_linestring_gen_z12_skeleton
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z12_skeleton AS
SELECT
  ST_Collect(geometry_part) AS geometry
FROM osm_shipway_linestring_parts;

-- etldoc: osm_shipway_linestring_gen_z12 -> osm_shipway_linestring_gen_z11
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z11 AS
SELECT
  ST_Simplify(geometry, ZRes(12)) AS geometry
FROM osm_shipway_linestring_gen_z12_skeleton
WHERE ST_Length(geometry) > ZRes(6);

-- etldoc: osm_shipway_linestring_gen_z11 -> osm_shipway_linestring_gen_z10
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z10 AS
SELECT
  ST_Simplify(geometry, ZRes(11)) AS geometry
FROM osm_shipway_linestring_gen_z11
WHERE ST_Length(geometry) > ZRes(5);

-- etldoc: osm_shipway_linestring_gen_z10 -> osm_shipway_linestring_gen_z9
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z9 AS
SELECT
  ST_Simplify(geometry, ZRes(10)) AS geometry
FROM osm_shipway_linestring_gen_z10
WHERE ST_Length(geometry) > ZRes(4);

-- etldoc: osm_shipway_linestring_gen_z9 -> osm_shipway_linestring_gen_z8
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z8 AS
SELECT
  ST_Simplify(geometry, ZRes(9)) AS geometry
FROM osm_shipway_linestring_gen_z9
WHERE ST_Length(geometry) > ZRes(3);

-- etldoc: osm_shipway_linestring_gen_z8 -> osm_shipway_linestring_gen_z7
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z7 AS
SELECT
  ST_Simplify(geometry, ZRes(8)) AS geometry
FROM osm_shipway_linestring_gen_z8
WHERE ST_Length(geometry) > ZRes(2);

-- etldoc: osm_shipway_linestring_gen_z7 -> osm_shipway_linestring_gen_z6
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z6 AS
SELECT
  ST_Simplify(geometry, ZRes(7)) AS geometry
FROM osm_shipway_linestring_gen_z7
WHERE ST_Length(geometry) > ZRes(1);

-- etldoc: osm_shipway_linestring_gen_z6 -> osm_shipway_linestring_gen_z5
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z5 AS
SELECT
  ST_Simplify(geometry, ZRes(6)) AS geometry
FROM osm_shipway_linestring_gen_z6
WHERE ST_Length(geometry) > ZRes(0);

-- etldoc: osm_shipway_linestring_gen_z5 -> osm_shipway_linestring_gen_z4
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z4 AS
SELECT
  ST_Simplify(geometry, ZRes(5)) AS geometry
FROM osm_shipway_linestring_gen_z5
WHERE ST_Length(geometry) > ZRes(-1);
