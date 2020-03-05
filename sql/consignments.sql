DROP TABLE IF EXISTS `default.h_conassignments`;

CREATE TABLE `default.h_conassignments`(
    `hashed_id` string,
    `src` string,
    `date` date,
    `importer_company_id` string,
    `exporter_company_id` string,
    `from_country` string,
    `to_country` string,
    `volume_usd` string,
    `hscode` string,
    `hskeywords` string,
    `trade_count` string
);

INSERT INTO `default.h_conassignments` (
    -- China Export Data
    SELECT
        md5('panjiva' || `panjivarecordid`) as `hashed_id`,
        'panjiva' as `src`,
        `shpmtmonth` as `date`,
        NULL as `importer_company_id`,
        `shppanjivaid` as `exporter_company_id`,
        LOWER(`shpcountry`) as `from_country`,
        LOWER(`shpmtdestination`) as `to_country`,
        `valueofgoodsusd` as `volume_usd`,
        `hscode` as `hscode`,
        `hscodekeywords` as `hskeywords`,
        NULL as `trade_count`
    from `default.h_panjiva_china_export`
    UNION
    -- India Export Data
    SELECT
        md5('panjiva' || `panjivarecordid`) as `hashed_id`,
        'panjiva' as `src`,
        `departuredate` as `date`,
        `conpanjivaid` as `importer_company_id`,
        `shppanjivaid` as `exporter_company_id`,
        LOWER(`shpcountry`) as `from_country`,
        LOWER(`shpmtdestination`) as `to_country`,
        `itemvalueusd` as `volume_usd`,
        `hscode` as `hscode`,
        `goodsdescribed` as `hskeywords`,
        `itemquantity` as `trade_count`
    from `default.h_panjiva_india_export`
    UNION
    -- Brazil Export Data
    SELECT
        md5('panjiva' || `panjivarecordid`) as `hashed_id`,
        'panjiva' as `src`,
        `shpmtdate` as `date`,
        `conpanjivaid` as `importer_company_id`,
        `shppanjivaid` as `exporter_company_id`,
        LOWER(`shpcountry`) as `from_country`,
        LOWER(`concountry`) as `to_country`,
        `valueofgoodsusd` as `volume_usd`,
        `hscode` as `hscode`,
        NULL as `hskeywords`,
        `grossweightkg` as `trade_count` -- data in kg WTF
    from `default.h_panjiva_brazil_export`
    UNION
    -- US Export Data
    SELECT
        md5('panjiva' || `panjivarecordid`) as `hashed_id`,
        'panjiva' as `src`,
        `shpmtdate` as `date`,
        NULL as `importer_company_id`,
        `shppanjivaid` as `exporter_company_id`,
        LOWER(`shpcountry`) as `from_country`,
        LOWER(`shpmtdestination`) as `to_country`,
        `valueofgoodsusd` as `volume_usd`,
        `hscode` as `hscode`,
        NULL as `hskeywords`,
        `itemquantity` as `trade_count` -- data in kg WTF
    from `default.h_panjiva_enhanced_us_export`
    UNION
    -- China Import Data
    SELECT
        md5('panjiva' || `panjivarecordid`) as `hashed_id`,
        'panjiva' as `src`,
        `shpmtmonth` as `date`,
        `conpanjivaid` as `importer_company_id`,
        NULL as `exporter_company_id`,
        LOWER(`countryofsale`) as `from_country`,
        LOWER(`concountry`) as `to_country`,
        `valueofgoodsusd` as `volume_usd`,
        `hscode` as `hscode`,
        `hscodekeywords` as `hskeywords`,
        NULL as `trade_count`
    from `default.h_panjiva_china_import`
    UNION
    -- India Import Data
    SELECT
        md5('panjiva' || `panjivarecordid`) as `hashed_id`,
        'panjiva' as `src`,
        `departuredate` as `date`,
        `conpanjivaid` as `importer_company_id`,
        `shppanjivaid` as `exporter_company_id`,
        LOWER(`shpcountry`) as `from_country`,
        LOWER(`concountry`) as `to_country`,
        `itemvalueusd` as `volume_usd`,
        `hscode` as `hscode`,
        `goodsdescribed` as `hskeywords`,
        `itemquantity` as `trade_count`
    from `default.h_panjiva_india_import`
    UNION
    -- Brazil Import Data
    SELECT
        md5('panjiva' || `panjivarecordid`) as `hashed_id`,
        'panjiva' as `src`,
        `shpmtdate` as `date`,
        `conpanjivaid` as `importer_company_id`,
        `shppanjivaid` as `exporter_company_id`,
        LOWER(`shpcountry`) as `from_country`,
        LOWER(`concountry`) as `to_country`,
        `valueofgoodsusd` as `volume_usd`,
        `hscode` as `hscode`,
        NULL as `hskeywords`,
        `grossweightkg` as `trade_count`
    from `default.h_panjiva_brazil_import`
    UNION
    -- US Import Data
    SELECT
        md5('panjiva' || `panjivarecordid`) as `hashed_id`,
        'panjiva' as `src`,
        `arrivaldate` as `date`,
        `conpanjivaid` as `importer_company_id`,
        `shppanjivaid` as `exporter_company_id`,
        LOWER(`shpcountry`) as `from_country`,
        LOWER(`concountry`) as `to_country`,
        `valueofgoodsusd` as `volume_usd`,
        `hscode` as `hscode`,
        NULL as `hskeywords`,
        `quantity` as `trade_count`
    from `default.h_panjiva_enhanced_us_import`
    UNION
    -- GTDS DATA
    SELECT
        md5('gtds_table' || `opt_recipient_name`) as `hashed_id`,
        'gtds_table' as `src`,
        `date` as `date`,
        `opt_recipient_name` as `importer_company_id`,
        `exporter_company_id` as `exporter_company_id`,
        LOWER(`from_country`) as `from_country`,
        LOWER(`to_country`) as `to_country`,
        `volume_usd` as `volume_usd`,
        `hscode` as `hscode`,
        `hskeywords` as `hskeywords`,
        `trade_count` as `trade_count`
    from `default.h_gtds`
    UNION
    -- Uzbekistan data
    SELECT
        md5('uz_gtds_table' || `opt_recipient_name`) as `hashed_id`,
        'uz_gtds_table' as `src`,
        `date` as `date`,
        `opt_recipient_name` as `importer_company_id`,
        `exporter_company_id` as `exporter_company_id`,
        LOWER(`from_country`) as `from_country`,
        LOWER(`to_country`) as `to_country`,
        `volume_usd` as `volume_usd`,
        `hscode` as `hscode`,
        `hskeywords` as `hskeywords`,
        `trade_count` as `trade_count`
    from `default.h_uzbek_aggregated`
);