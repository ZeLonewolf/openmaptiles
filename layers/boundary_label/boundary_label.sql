-- etldoc: layer_boundary_label[shape=record fillcolor=lightpink, style="rounded,filled",
-- etldoc:     label="layer_boundary_label | <z5> z5 | <z6> z6 | <z7> z7 | <z8> z8 |<z9> z9 |<z10> z10 |<z11> z11 |<z12> z12|<z13> z13|<z14_> z14+" ] ;

CREATE OR REPLACE FUNCTION layer_boundary_label(bbox geometry, zoom_level integer)
    RETURNS TABLE
            (
                osm_id      bigint,
                geometry    geometry,
                admin_level int,
                name        text,
                tags        hstore
            )
AS
$$
SELECT osm_id,
       geometry,
       admin_level,
       name,
       tags
FROM (
         -- etldoc: osm_boundary_label_linestring_gen_z5 -> layer_boundary_label:z5
         SELECT osm_id,
                geometry,
                admin_level,
                name,
                tags
         FROM osm_boundary_label_linestring_gen_z5
         WHERE zoom_level = 5
           AND geometry && bbox

         UNION ALL

         SELECT osm_id,
                geometry,
                admin_level,
                name,
                tags
         FROM osm_boundary_label_linestring_gen_z6
         WHERE zoom_level = 6
           AND geometry && bbox

         UNION ALL

         SELECT osm_id,
                geometry,
                admin_level,
                name,
                tags
         FROM osm_boundary_label_linestring_gen_z7
         WHERE zoom_level = 7
           AND geometry && bbox

         UNION ALL

         SELECT osm_id,
                geometry,
                admin_level,
                name,
                tags
         FROM osm_boundary_label_linestring_gen_z8
         WHERE zoom_level = 8
           AND geometry && bbox

         UNION ALL

         SELECT osm_id,
                geometry,
                admin_level,
                name,
                tags
         FROM osm_boundary_label_linestring_gen_z9
         WHERE zoom_level = 9
           AND geometry && bbox

         UNION ALL

         SELECT osm_id,
                geometry,
                admin_level,
                name,
                tags
         FROM osm_boundary_label_linestring_gen_z10
         WHERE zoom_level = 10
           AND geometry && bbox

         UNION ALL

         SELECT osm_id,
                geometry,
                admin_level,
                name,
                tags
         FROM osm_boundary_label_linestring_gen_z11
         WHERE zoom_level = 11
           AND geometry && bbox

         UNION ALL

         SELECT osm_id,
                geometry,
                admin_level,
                name,
                tags
         FROM osm_boundary_label_linestring_gen_z12
         WHERE zoom_level = 12
           AND geometry && bbox

         UNION ALL

         SELECT osm_id,
                geometry,
                admin_level,
                name,
                tags
         FROM osm_boundary_label_linestring_gen_z13
         WHERE zoom_level >= 13
           AND geometry && bbox
     ) AS boundary_label
$$ LANGUAGE SQL STABLE
                PARALLEL SAFE;
