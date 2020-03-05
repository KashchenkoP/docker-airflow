DROP TABLE if EXISTS import_stat_by_hscodes;

CREATE TABLE
    import_stat_by_hscodes
AS
SELECT
    SUBSTR(hscode,1,6) hscode_on_6
    ,SUM(CAST(volume_usd as float)) total_volume_on_hscode
    ,SUM(CAST(volume_usd as float)*EXP(-1*(2019 - YEAR(FROM_UNIXTIME(unix_timestamp(`date`, 'yyyy-mm-dd')))))) as exponential_weghted_total_import_volume
    ,AVG(CAST(volume_usd as float) / trade_count) avg_unit_value_on_hscode
    ,MAX(CAST(volume_usd as float) / trade_count) max_unit_value_on_hscode
    ,MIN(CAST(volume_usd as float) / trade_count) min_unit_value_on_hscode
FROM
    h_conassignments
GROUP BY SUBSTR(hscode,1,6)