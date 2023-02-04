-- Create bounding windows for country-specific processing

SELECT pg_advisory_lock(1);

-- etldoc: ne_10m_admin_0_countries ->  ne_10m_admin_0_nobuffer
CREATE TABLE IF NOT EXISTS ne_10m_admin_0_nobuffer AS
SELECT iso_a2,
       ST_Buffer(geometry, 10000) AS geometry
FROM ne_10m_admin_0_countries
WHERE iso_a2 IN ('CN', 'CN-TW', 'HK', 'JP', 'KP', 'KR', 'SG', 'MS', 'MO', 'VN');

CREATE INDEX IF NOT EXISTS ne_10m_admin_0_nobuffer_cc_idx ON ne_10m_admin_0_nobuffer(iso_a2);

-- etldoc: ne_10m_admin_0_countries ->  ne_10m_admin_0_buffer
CREATE TABLE IF NOT EXISTS ne_10m_admin_0_buffer AS
SELECT iso_a2,
       geometry
FROM ne_10m_admin_0_countries
WHERE iso_a2 IN ('GB', 'IE');

CREATE INDEX IF NOT EXISTS ne_10m_admin_0_buffer_cc_idx ON ne_10m_admin_0_nobuffer(iso_a2);

-- etldoc: osm_aerodrome_label_point -> osm_aerodrome_label_point
CREATE OR REPLACE FUNCTION han_unification_country_code(g geometry) RETURNS text AS
$$  SELECT CASE
      WHEN ST_Intersects(g, (SELECT geometry FROM ne_10m_admin_0_nobuffer WHERE iso_a2='HK')) THEN 'HK'
      WHEN ST_Intersects(g, (SELECT geometry FROM ne_10m_admin_0_nobuffer WHERE iso_a2='CN-TW')) THEN 'TW'
      WHEN ST_Intersects(g, (SELECT geometry FROM ne_10m_admin_0_nobuffer WHERE iso_a2='CN')) THEN 'CN'
      WHEN ST_Intersects(g, (SELECT geometry FROM ne_10m_admin_0_nobuffer WHERE iso_a2='JP')) THEN 'JP'
      WHEN ST_Intersects(g, (SELECT geometry FROM ne_10m_admin_0_nobuffer WHERE iso_a2='KR')) THEN 'KR'
      WHEN ST_Intersects(g, (SELECT geometry FROM ne_10m_admin_0_nobuffer WHERE iso_a2='VN')) THEN 'VN'
      WHEN ST_Intersects(g, (SELECT geometry FROM ne_10m_admin_0_nobuffer WHERE iso_a2='KP')) THEN 'KP'
      WHEN ST_Intersects(g, (SELECT geometry FROM ne_10m_admin_0_nobuffer WHERE iso_a2='MO')) THEN 'MO'
      WHEN ST_Intersects(g, (SELECT geometry FROM ne_10m_admin_0_nobuffer WHERE iso_a2='MS')) THEN 'MS'
      WHEN ST_Intersects(g, (SELECT geometry FROM ne_10m_admin_0_nobuffer WHERE iso_a2='SG')) THEN 'SG'
    END
$$ LANGUAGE SQL;

SELECT pg_advisory_unlock(1);
