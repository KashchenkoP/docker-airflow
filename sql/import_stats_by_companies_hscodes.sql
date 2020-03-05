DROP TABLE IF EXISTS import_stat_by_hscodes_and_companies;

CREATE TABLE
  import_stat_by_hscodes_and_companies
AS
SELECT
    MD5(concat_ws('', t1.src, t1.importer_company_id)) as hashed_id,
    MAX(t1.importer_company_id) as `importer_company_id`,
    SUBSTR(t1.hscode,1,6) as `hscode_on_6`,
    SUM(t1.volume_usd) as total_import_volume,
    SUM(t1.volume_usd*EXP(-1*(2019 - YEAR(FROM_UNIXTIME(unix_timestamp(t1.`date`, 'yyyy-mm-dd')))))) as exponential_weghted_total_import_volume,
    AVG(t1.volume_usd) avg_import_volume,
    SUM(t1.volume_usd*EXP(-1*(2019 - YEAR(FROM_UNIXTIME(unix_timestamp(t1.`date`, 'yyyy-mm-dd')))))) / count(*) exponential_weghted_avg_import_volume
FROM
    h_conassignments as t1
JOIN
    h_companies_info as t2
ON
    MD5(concat_ws('', t1.src, t1.importer_company_id)) = t2.hashed_id
WHERE
    t1.importer_company_id is not NULL AND t2.hashed_id is not NULL
GROUP BY
    MD5(concat_ws('', t1.src, t1.importer_company_id)), SUBSTR(t1.hscode,1,6);