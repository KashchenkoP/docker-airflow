DROP TABLE if EXISTS h_panjiva_implicit_importers;

CREATE TABLE h_panjiva_implicit_importers as (
    SELECT
        hashed_id,
        src,
        importer_company_id,
        `date`,
        to_country as `importer_country`,
        volume_usd,
        hscode,
        hskeywords,
        trade_count
    from
        `default.h_conassignments`
    where
        `from_country`='india' and `from_country` is not NULL
);