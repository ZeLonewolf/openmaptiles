DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z4;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z5;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z6;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z7;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z8;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z9;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z10;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z11;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_linestring_gen_z12_clustered;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_cluster_coalesced;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_cluster_centroid;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_clustered;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_explode;
DROP MATERIALIZED VIEW IF EXISTS osm_shipway_dumppoints;

-- This sequence of tables coalesces colinear ferry sections

-- etldoc: osm_shipway_linestring_gen_z12 -> osm_shipway_dumppoints
CREATE MATERIALIZED VIEW osm_shipway_dumppoints AS
SELECT 
  osm_id,
  ST_DumpPoints(geometry) AS dp,
  name
FROM osm_shipway_linestring_gen_z12;

-- etldoc: osm_shipway_dumppoints -> osm_shipway_explode
CREATE MATERIALIZED VIEW osm_shipway_explode AS
SELECT
  osm_id,
  (dp).geom AS pt,
  (dp).path[1] As ptidx
FROM osm_shipway_dumppoints;

-- etldoc: osm_shipway_explode -> osm_shipway_clustered
CREATE MATERIALIZED VIEW osm_shipway_clustered AS	
SELECT
  osm_id,
  pt,
  ptidx,
  ST_ClusterDBSCAN(pt, eps := 400, minpoints := 2) over () AS cid
FROM osm_shipway_explode;
  
-- etldoc: osm_shipway_clustered -> osm_shipway_cluster_centroid
CREATE MATERIALIZED VIEW osm_shipway_cluster_centroid AS
SELECT
  cid,
  ST_Centroid(ST_Collect(pt)) AS ctr
FROM osm_shipway_clustered
WHERE cid IS NOT NULL
GROUP BY cid;

-- etldoc: osm_shipway_cluster_centroid -> osm_shipway_cluster_coalesced
CREATE MATERIALIZED VIEW osm_shipway_cluster_coalesced AS
SELECT
  osm_id,
  COALESCE(clctr.ctr, cl.pt) AS pt,
  ptidx
FROM osm_shipway_clustered cl
LEFT OUTER JOIN osm_shipway_cluster_centroid clctr ON cl.cid = clctr.cid;
  
-- etldoc: osm_shipway_cluster_coalesced -> osm_shipway_linestring_gen_z12
CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z12_clustered AS
SELECT 
  oscc.osm_id AS osm_id,
  ST_MakeLine(pt order by ptidx) AS geometry,
  shipway,
  service_value(service) AS service,
  is_bridge,
  is_tunnel,
  is_ford,
  is_ramp,
  is_oneway,
  layer,
  z_order
FROM osm_shipway_cluster_coalesced oscc
LEFT OUTER JOIN osm_shipway_linestring_gen_z12 osl
  ON oscc.osm_id = osl.osm_id
GROUP BY oscc.osm_id, shipway, service, is_bridge, is_tunnel, is_ford, is_ramp, is_oneway, layer, z_order;

CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z11 AS
SELECT
  osm_id,
  ST_Simplify(geometry, ZRes(12)) AS geometry,
  shipway,
  service,
  is_bridge,
  is_tunnel,
  is_ford,
  is_ramp,
  is_oneway,
  layer,
  z_order
FROM osm_shipway_linestring_gen_z12_clustered
WHERE ST_Length(geometry) > ZRes(6);

CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z10 AS
SELECT
  osm_id,
  ST_Simplify(geometry, ZRes(11)) AS geometry,
  shipway,
  service,
  is_bridge,
  is_tunnel,
  is_ford,
  is_ramp,
  is_oneway,
  layer,
  z_order
FROM osm_shipway_linestring_gen_z11
WHERE ST_Length(geometry) > ZRes(5);

CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z9 AS
SELECT
  osm_id,
  ST_Simplify(geometry, ZRes(10)) AS geometry,
  shipway,
  service,
  is_bridge,
  is_tunnel,
  is_ford,
  is_ramp,
  is_oneway,
  layer,
  z_order
FROM osm_shipway_linestring_gen_z10
WHERE ST_Length(geometry) > ZRes(4);

CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z8 AS
SELECT
  osm_id,
  ST_Simplify(geometry, ZRes(9)) AS geometry,
  shipway,
  service,
  is_bridge,
  is_tunnel,
  is_ford,
  is_ramp,
  is_oneway,
  layer,
  z_order
FROM osm_shipway_linestring_gen_z9
WHERE ST_Length(geometry) > ZRes(3);

CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z7 AS
SELECT
  osm_id,
  ST_Simplify(geometry, ZRes(8)) AS geometry,
  shipway,
  service,
  is_bridge,
  is_tunnel,
  is_ford,
  is_ramp,
  is_oneway,
  layer,
  z_order
FROM osm_shipway_linestring_gen_z8
WHERE ST_Length(geometry) > ZRes(2);

CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z6 AS
SELECT
  osm_id,
  ST_Simplify(geometry, ZRes(7)) AS geometry,
  shipway,
  service,
  is_bridge,
  is_tunnel,
  is_ford,
  is_ramp,
  is_oneway,
  layer,
  z_order
FROM osm_shipway_linestring_gen_z7
WHERE ST_Length(geometry) > ZRes(1);

CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z5 AS
SELECT
  osm_id,
  ST_Simplify(geometry, ZRes(6)) AS geometry,
  shipway,
  service,
  is_bridge,
  is_tunnel,
  is_ford,
  is_ramp,
  is_oneway,
  layer,
  z_order
FROM osm_shipway_linestring_gen_z6
WHERE ST_Length(geometry) > ZRes(0);

CREATE MATERIALIZED VIEW osm_shipway_linestring_gen_z4 AS
SELECT
  osm_id,
  ST_Simplify(geometry, ZRes(5)) AS geometry,
  shipway,
  service,
  is_bridge,
  is_tunnel,
  is_ford,
  is_ramp,
  is_oneway,
  layer,
  z_order
FROM osm_shipway_linestring_gen_z5
WHERE ST_Length(geometry) > ZRes(-1);
