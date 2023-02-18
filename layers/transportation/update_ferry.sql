DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z4;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z5;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z6;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z7;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z8;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z9;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z10;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z11;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z12_skeleton;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_distinct_segments;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_segment_tuples;
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
CREATE INDEX IF NOT EXISTS osm_shipway_dumppoints_id ON osm_shipway_dumppoints (osm_id);

-- Step 2: Extract point geometry and point position in the linestring
-- etldoc: osm_shipway_dumppoints -> osm_shipway_explode
CREATE MATERIALIZED VIEW osm_shipway_explode AS
SELECT
  osm_id,
  -- Ensure clustering is done in WebMercator
  ST_Transform((dp).geom, 3857) AS pt,
  (dp).path[1] As ptidx
FROM osm_shipway_dumppoints;
CREATE INDEX IF NOT EXISTS osm_shipway_explode_pt ON osm_shipway_explode USING gist (pt);

-- Step 3: Cluster groups of nearby points
-- etldoc: osm_shipway_explode -> osm_shipway_clustered
CREATE MATERIALIZED VIEW osm_shipway_clustered AS	
SELECT
  ose.osm_id AS osm_id,
  pt,
  ptidx,
  ST_ClusterDBSCAN(pt, eps := 200, minpoints := 2) over () AS cid,
  ST_Length(geometry) AS route_length
FROM osm_shipway_explode ose
JOIN osm_shipway_linestring_gen_z12 osl ON ose.osm_id = osl.osm_id;
CREATE INDEX IF NOT EXISTS osm_shipway_clustered_cid ON osm_shipway_clustered (cid);
  
-- Step 4: Compute center point of each cluster of points
-- etldoc: osm_shipway_clustered -> osm_shipway_cluster_centroid
CREATE MATERIALIZED VIEW osm_shipway_cluster_centroid AS
SELECT
  cid,
  ST_Centroid(ST_Collect(pt)) AS ctr
FROM osm_shipway_clustered
WHERE cid IS NOT NULL
GROUP BY cid;
CREATE INDEX IF NOT EXISTS osm_shipway_cluster_centroid ON osm_shipway_cluster_centroid (cid);

-- Step 5: Replace all clustered points with a centroid point
-- etldoc: osm_shipway_cluster_centroid -> osm_shipway_cluster_coalesced
CREATE MATERIALIZED VIEW osm_shipway_cluster_coalesced AS
SELECT
  ioscc.osm_id AS osm_id,
  ioscc.pt AS pt,
  MIN(ioscc.ptidx) AS ptidx
FROM
(
  SELECT
    osm_id,
    COALESCE(clctr.ctr, cl.pt) AS pt,
    ptidx
  FROM osm_shipway_clustered cl
  LEFT OUTER JOIN osm_shipway_cluster_centroid clctr ON cl.cid = clctr.cid
) ioscc
JOIN osm_shipway_clustered osc ON osc.osm_id = ioscc.osm_id
GROUP BY ioscc.osm_id, ioscc.pt;

-- Step 6: Assemble two-point line segments
-- etldoc: osm_shipway_cluster_coalesced -> osm_shipway_segment_tuples
CREATE MATERIALIZED VIEW osm_shipway_segment_tuples AS
SELECT
  osm_id,
  ST_MakeLine(pt1, pt2) AS segment
FROM (
  SELECT
    osm_id, 
    ptidx,
    pt AS pt1, 
    lead(pt) OVER (
      PARTITION BY osm_id
      ORDER BY ptidx
    ) AS pt2
  FROM osm_shipway_cluster_coalesced
) osm_shipway_cluster_segment_endpoints
WHERE pt2 IS NOT NULL;

-- Step 7: Discard duplicate sections belonging to shorter routes
-- etldoc: osm_shipway_segment_tuples -> osm_shipway_distinct_segments
-- etldoc: osm_shipway_clustered -> osm_shipway_distinct_segments
CREATE MATERIALIZED VIEW osm_shipway_distinct_segments AS
SELECT
  segment,
  MAX(osc.route_length) AS route_length
FROM osm_shipway_segment_tuples osst
JOIN osm_shipway_clustered osc ON osst.osm_id = osc.osm_id
GROUP BY segment;

-- Step 8: Re-combine segments that are part of colinear sections
-- etldoc: osm_shipway_distinct_segments -> osm_shipway_linestring_gen_z12_skeleton
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z12_skeleton AS
SELECT
  ST_Union(segment) AS geometry,
  route_length AS max_length
FROM osm_shipway_distinct_segments
GROUP BY route_length;

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
