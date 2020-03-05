DROP TABLE IF EXISTS `default`.dm_toplevel_companies_search;

CREATE TABLE `default`.dm_toplevel_companies_search AS
SELECT
    t1.hashed_id
    ,t1.hscode_on_6
    --,t3.exponential_weghted_total_import_volume
    , ((t1.exponential_weghted_total_import_volume - NVL(t3.exponential_weghted_total_import_volume, 0)) / t2.exponential_weghted_total_import_volume) as  `score`
    ,t4.src, t4.src_id, t4.country, t4.city, t4.stateregion, t4.route, t4.fulladdress, t4.name, t4.phone_number, t4.website, t4.email
FROM
    import_stat_by_hscodes_and_companies t1
LEFT JOIN import_stat_by_hscodes t2
    ON
    t1.hscode_on_6 = t2.hscode_on_6
LEFT JOIN import_stat_from_russia_by_hscodes_and_companies t3
    ON
        t1.hashed_id = t3.hashed_id
    AND
        t1.hscode_on_6 = t3.hscode_on_6
LEFT JOIN h_companies_info t4
    ON
        t1.hashed_id = t4.hashed_id