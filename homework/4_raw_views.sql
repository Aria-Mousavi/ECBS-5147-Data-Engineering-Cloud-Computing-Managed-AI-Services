-- DROP TABLE IF EXISTS aria.raw_views;

CREATE EXTERNAL TABLE
aria.raw_views (
    title STRING,
    views INT,
    rank INT,
    date DATE,
    retrieved_at STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://aria-wikidata/raw-views/';