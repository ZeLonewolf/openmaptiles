-- Create bounding windows for country-specific processing

SELECT pg_advisory_lock(1);

-- etldoc: ne_10m_admin_0_countries ->  ne_10m_admin_0_cn
CREATE TABLE IF NOT EXISTS ne_10m_admin_0_cn AS
SELECT geometry
FROM ne_10m_admin_0_countries
WHERE iso_a2 = 'CN';

-- etldoc: ne_10m_admin_0_countries ->  ne_10m_admin_0_gb_buffer
CREATE TABLE IF NOT EXISTS ne_10m_admin_0_gb_buffer AS
SELECT ST_Buffer(geometry, 10000)
FROM ne_10m_admin_0_countries
WHERE iso_a2 = 'GB';

-- etldoc: ne_10m_admin_0_countries ->  ne_10m_admin_0_ie_buffer
CREATE TABLE IF NOT EXISTS ne_10m_admin_0_ie_buffer AS
SELECT ST_Buffer(geometry, 10000)
FROM ne_10m_admin_0_countries
WHERE iso_a2 = 'IE';

-- etldoc: ne_10m_admin_0_countries ->  ne_10m_admin_0_jp_buffer
CREATE TABLE IF NOT EXISTS ne_10m_admin_0_jp_buffer AS
SELECT ST_Buffer(geometry, 10000)
FROM ne_10m_admin_0_countries
WHERE iso_a2 = 'JP';

-- etldoc: ne_10m_admin_0_countries ->  ne_10m_admin_0_kp
CREATE TABLE IF NOT EXISTS ne_10m_admin_0_kp AS
SELECT geometry
FROM ne_10m_admin_0_countries
WHERE iso_a2 = 'KP';

-- etldoc: ne_10m_admin_0_countries ->  ne_10m_admin_0_kr
CREATE TABLE IF NOT EXISTS ne_10m_admin_0_kr AS
SELECT geometry
FROM ne_10m_admin_0_countries
WHERE iso_a2 = 'KR';

-- etldoc: ne_10m_admin_0_countries ->  ne_10m_admin_0_vn
CREATE TABLE IF NOT EXISTS ne_10m_admin_0_vn AS
SELECT geometry
FROM ne_10m_admin_0_countries
WHERE iso_a2 = 'VN';

-- etldoc: osm_aerodrome_label_point -> osm_aerodrome_label_point
CREATE OR REPLACE FUNCTION han_unification_country_code(g geometry) RETURNS text AS
$$  SELECT CASE
      WHEN ST_Intersects(g, (SELECT * FROM ne_10m_admin_0_cn)) THEN 'CN'
      WHEN ST_Intersects(g, (SELECT * FROM ne_10m_admin_0_jp_buffer)) THEN 'JP'
      WHEN ST_Intersects(g, (SELECT * FROM ne_10m_admin_0_kr)) THEN 'KR'
      WHEN ST_Intersects(g, (SELECT * FROM ne_10m_admin_0_vn)) THEN 'VN'
      WHEN ST_Intersects(g, (SELECT * FROM ne_10m_admin_0_kp)) THEN 'KP'
    END
$$ LANGUAGE SQL;

SELECT pg_advisory_unlock(1);
