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
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_multi_isect;
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
  ST_ClusterDBSCAN(pt, eps := 1000, minpoints := 2) over () AS cid
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
  pt,
  MIN(ptidx) as ptidx
FROM
(
  SELECT
    osm_id,
    COALESCE(clctr.ctr, cl.pt) AS pt,
    ptidx
  FROM osm_shipway_clustered cl
  LEFT OUTER JOIN osm_shipway_cluster_centroid clctr ON cl.cid = clctr.cid
) inner_osm_shipway_cluster_coalesced
GROUP BY osm_id, pt;

-- Step 6: Re-assemble linestrings with the new point positions
-- etldoc: osm_shipway_cluster_coalesced -> osm_shipway_linestring_clustered
CREATE MATERIALIZED VIEW osm_shipway_linestring_clustered AS
SELECT 
  osm_id,
  ST_MakeLine(pt order by ptidx) AS geometry
FROM osm_shipway_cluster_coalesced oscc
GROUP BY osm_id;

-- Step 7: Iterate through each pair of ferry lines that have shared segments
--   and compute the portion that intersects
-- etldoc: osm_shipway_clustered -> osm_shipway_linestring_isect
-- etldoc: osm_shipway_linestring_clustered -> osm_shipway_linestring_isect
CREATE MATERIALIZED VIEW osm_shipway_linestring_isect AS
SELECT DISTINCT
  osc1.osm_id AS osm_id1,
  osc2.osm_id AS osm_id2,
  ST_Intersection(oslc1.geometry, oslc2.geometry) AS isect,
  GREATEST(ST_Length(oslc1.geometry),
           ST_Length(oslc2.geometry)) AS max_length
FROM osm_shipway_clustered osc1
JOIN osm_shipway_clustered osc2 ON osc1.cid = osc2.cid AND osc1.osm_id < osc2.osm_id
JOIN osm_shipway_linestring_clustered oslc1 ON osc1.osm_id = oslc1.osm_id
JOIN osm_shipway_linestring_clustered oslc2 ON osc2.osm_id = oslc2.osm_id;

-- Step 8: Get all overlap segments associated with a ferry route
-- etldoc:  osm_shipway_linestring_isect -> osm_shipway_linestring_multi_isect
CREATE MATERIALIZED VIEW osm_shipway_linestring_multi_isect AS
SELECT
  osm_id,
  ST_Union(isect) AS multioverlap
FROM (
  SELECT
    osm_id1 AS osm_id,
    isect
  FROM osm_shipway_linestring_isect
  WHERE ST_Length(isect) > 1 -- 1 meter rounding factor
  UNION ALL
  SELECT
    osm_id2 AS osm_id,
    isect
  FROM osm_shipway_linestring_isect
  WHERE ST_Length(isect) > 1 -- 1 meter rounding factor
  ) overlap_subquery
GROUP BY osm_id;

-- Step 9: Collect the intersection and difference segments
-- etldoc: osm_shipway_linestring_clustered -> osm_shipway_linestring_parts
-- etldoc: osm_shipway_linestring_multi_isect -> osm_shipway_linestring_parts
-- etldoc: osm_shipway_linestring_isect -> osm_shipway_linestring_parts
CREATE MATERIALIZED VIEW osm_shipway_linestring_parts AS
SELECT
  geometry,
  max_length
FROM (
  SELECT
    ST_Difference(geometry, multioverlap) AS geometry,
    ST_Length(geometry) AS max_length
  FROM osm_shipway_linestring_clustered oslc
  JOIN osm_shipway_linestring_multi_isect isect ON oslc.osm_id = isect.osm_id
  UNION ALL
  SELECT
    DISTINCT isect AS geometry,
    max_length
  FROM osm_shipway_linestring_isect
  WHERE max_length > 0
) shipway_parts_bin
WHERE ST_Length(geometry) > 0;

-- etldoc: osm_shipway_cluster_coalesced -> osm_shipway_linestring_gen_z12_skeleton
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z12_skeleton AS
SELECT
  geometry,
  max_length
FROM osm_shipway_linestring_parts;

-- etldoc: osm_shipway_linestring_gen_z12 -> osm_shipway_linestring_gen_z11
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z11 AS
SELECT
  ST_Simplify(geometry, ZRes(12)) AS geometry,
  max_length
FROM osm_shipway_linestring_gen_z12_skeleton
WHERE max_length > ZRes(6);

-- etldoc: osm_shipway_linestring_gen_z11 -> osm_shipway_linestring_gen_z10
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z10 AS
SELECT
  ST_Simplify(geometry, ZRes(11)) AS geometry,
  max_length
FROM osm_shipway_linestring_gen_z11
WHERE max_length > ZRes(5);

-- etldoc: osm_shipway_linestring_gen_z10 -> osm_shipway_linestring_gen_z9
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z9 AS
SELECT
  ST_Simplify(geometry, ZRes(10)) AS geometry,
  max_length
FROM osm_shipway_linestring_gen_z10
WHERE max_length > ZRes(4);

-- etldoc: osm_shipway_linestring_gen_z9 -> osm_shipway_linestring_gen_z8
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z8 AS
SELECT
  ST_Simplify(geometry, ZRes(9)) AS geometry,
  max_length
FROM osm_shipway_linestring_gen_z9
WHERE max_length > ZRes(3);

-- etldoc: osm_shipway_linestring_gen_z8 -> osm_shipway_linestring_gen_z7
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z7 AS
SELECT
  ST_Simplify(geometry, ZRes(8)) AS geometry,
  max_length
FROM osm_shipway_linestring_gen_z8
WHERE max_length > ZRes(2);

-- etldoc: osm_shipway_linestring_gen_z7 -> osm_shipway_linestring_gen_z6
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z6 AS
SELECT
  ST_Simplify(geometry, ZRes(7)) AS geometry,
  max_length
FROM osm_shipway_linestring_gen_z7
WHERE max_length > ZRes(1);

-- etldoc: osm_shipway_linestring_gen_z6 -> osm_shipway_linestring_gen_z5
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z5 AS
SELECT
  ST_Simplify(geometry, ZRes(6)) AS geometry,
  max_length
FROM osm_shipway_linestring_gen_z6
WHERE max_length > ZRes(0);

-- etldoc: osm_shipway_linestring_gen_z5 -> osm_shipway_linestring_gen_z4
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z4 AS
SELECT
  ST_Simplify(geometry, ZRes(5)) AS geometry,
  max_length
FROM osm_shipway_linestring_gen_z5
WHERE max_length > ZRes(-1);
