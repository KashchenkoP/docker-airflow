DROP TABLE IF EXISTS `default.h_companies_info_TMP`;

CREATE TABLE `default.h_companies_info_TMP`(
    `hashed_id` string,
    `src` string,
    `src_id` string,
    `country` string,
    `city` string,
    `stateregion` string,
    `route` string,
    `fulladdress` string,
    `name` string,
    `phone_number` string,
    `website` string,
    `email` string
);

INSERT INTO `default.h_companies_info_TMP` (
    -- China Import Data
    SELECT
        md5('panjiva' || `conpanjivaid`) as `hashed_id`,
        max('panjiva') as `src`,
        max(`conpanjivaid`) as `src_id`,
        max(LOWER(`concountry`)) as `country`,
        max(`concity`) as `city`,
        max(`constateregion`) as `stateregion`,
        max(`conroute`) as `route`,
        max(`confulladdress`) as `fulladdress`,
        max(`conname`) as `name`,
        NULL as `phone`,
        NULL as `website`,
        NULL as `email`
    from `default.h_panjiva_china_import`
    GROUP BY md5('panjiva' || `conpanjivaid`)
    UNION ALL
    -- India Import Data
    SELECT
        md5('panjiva' || `conpanjivaid`) as `hashed_id`,
        max('panjiva') as `src`,
        max(`conpanjivaid`) as `src_id`,
        max(LOWER(`concountry`)) as `country`,
        max(`concity`) as `city`,
        max(`constateregion`) as `stateregion`,
        max(`conroute`) as `route`,
        max(`confulladdress`) as `fulladdress`,
        max(`conname`) as `name`,
        NULL as `phone`,
        NULL as `website`,
        NULL as `email`
    from `default.h_panjiva_india_import`
    group by md5('panjiva' || `conpanjivaid`)
    UNION ALL
    -- Brazil Import Data
    SELECT
        md5('panjiva' || `conpanjivaid`) as `hashed_id`,
        max('panjiva') as `src`,
        max(`conpanjivaid`) as `src_id`,
        max(LOWER(`concountry`)) as `country`,
        max(`concity`) as `city`,
        max(`constateregion`) as `stateregion`,
        max(`conroute`) as `route`,
        max(`confulladdress`) as `fulladdress`,
        max(`conname`) as `name`,
        NULL as `phone`,
        NULL as `website`,
        NULL as `email`
    from `default.h_panjiva_brazil_import`
    GROUP BY md5('panjiva' || `conpanjivaid`)
    UNION ALL
    -- US Import Data
    SELECT
        md5('panjiva' || `conpanjivaid`) as `hashed_id`,
        max('panjiva') as `src`,
        max(`conpanjivaid`) as `src_id`,
        max(LOWER(`concountry`)) as `country`,
        max(`concity`) as `city`,
        max(`constateregion`) as `stateregion`,
        max(`conroute`) as `route`,
        max(`confulladdress`) as `fulladdress`,
        max(`conname`) as `name`,
        NULL as `phone`,
        NULL as `website`,
        NULL as `email`
    from `default.h_panjiva_enhanced_us_import`
    GROUP BY md5('panjiva' || `conpanjivaid`)
    UNION ALL
    -- Implicit importers (based on Indian conassignmetns)
    SELECT
        md5('panjiva' || `importer_company_id`) as `hashed_id`,
        max('panjiva') as `src`,
        max(`importer_company_id`) as `src_id`,
        max(LOWER(`importer_country`)) as `country`,
        NULL as `city`,
        NULL as `stateregion`,
        NULL as `route`,
        NULL as `fulladdress`,
        NULL as `name`,
        NULL as `phone`,
        NULL as `website`,
        NULL as `email`
    from `default.h_panjiva_implicit_importers`
    GROUP BY md5('panjiva' || `importer_company_id`)
    UNION ALL
    -- Russian GTDS Data
    SELECT
        md5('gtds_table' || `opt_recipient_name`) as `hashed_id`,
        max('gtds_table') as `src`,
        max(`opt_recipient_name`) as `src_id`,
        max(LOWER(`to_country`)) as `country`,
        NULL as `city`,
        NULL as `stateregion`,
        NULL as `route`,
        max(`opt_recipient_addr`) as `fulladdress`,
        max(`opt_recipient_name`) as `name`,
        NULL as `phone`,
        NULL as `website`,
        NULL as `email`
    from `default.h_gtds`
    GROUP BY md5('gtds_table' || `opt_recipient_name`)
    UNION ALL
    -- Uzbekistan GTDS data
    SELECT
        md5('uz_gtds_table' || `opt_recipient_name`) as `hashed_id`,
        max('uz_gtds_table') as `src`,
        max(`opt_recipient_name`) as `src_id`,
        max(LOWER(`to_country`)) as `country`,
        NULL as `city`,
        NULL as `stateregion`,
        NULL as `route`,
        max(`opt_recipient_addr`) as `fulladdress`,
        max(`opt_recipient_name`) as `name`,
        NULL as `phone`,
        NULL as `website`,
        NULL as `email`
    from `default.h_uzbek_aggregated`
    GROUP BY md5('uz_gtds_table' || `opt_recipient_name`)
);