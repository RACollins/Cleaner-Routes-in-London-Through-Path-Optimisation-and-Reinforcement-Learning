DROP SCHEMA IF EXISTS model2;
CREATE SCHEMA model2;

/*
Edges in the graph.
Gamma is the average NO2 pollution of grid cells the edge passes through.
*/
CREATE TABLE model2.edges (
    eid INT,
    gamma NUMERIC,
    distance NUMERIC,
    startnode VARCHAR,
    endnode VARCHAR,
    inpath BOOLEAN
);

CREATE TABLE model2.nodes (
    nid VARCHAR,
    lat NUMERIC,
    lon NUMERIC
);

/* Add a geometry column to edges and nodes tables */
SELECT AddGeometryColumn('model2', 'edges', 'geom', 4326, 'MULTILINESTRING', 4);
SELECT AddGeometryColumn('model2', 'nodes', 'geom', 4326, 'POINT', 4);

/*
Find lines and grid cells that intersect.
Take the average NO2 pollution of a all grid cells a road passes through.

INSERT INTO model2.edges
(eid, gamma, distance, startnode, endnode, geom)
SELECT
    L.gid AS eid,
    AVG(P.value_no2) AS gamma,
    L.length AS distance,
    L.startnode,
    L.endnode,
    ST_Transform(L.geom, 4326) as geom
FROM geog.links L, geog.pollution P
WHERE ST_Intersects(P.geom, ST_Transform(L.geom, 4326))
GROUP BY L.gid;


Find latitude, longitude of nodes in the graph.

INSERT INTO model2.nodes
(nid, lat, lon, geom)
SELECT J.identifier AS nid, ST_Y(ST_TRANSFORM(J.geom, 4326)) AS lat,
ST_X(ST_TRANSFORM(J.geom, 4326)) AS lon, ST_Transform(J.geom,4326) AS geom
FROM model2.edges E, geog.junctions J
WHERE identifier=startnode OR identifier=endnode
GROUP BY J.identifier, J.geom;*/
