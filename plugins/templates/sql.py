LOAD_HUB_FROM_EXT_TEMP = """
insert into {{ params.hub_name }}
select
    ext.*
from {{ params.hub_name }}_ext ext
left join {{ params.hub_name }} h
    on h.{{ params.pk_name }} = ext.{{ params.pk_name }}
where
      h.{{ params.pk_name }} is null
    and ext.load_dtm::time > {{ params.from_dtm }};
"""

LOAD_LINK_FROM_EXT_TEMP = """

"""

LOAD_SAT_FROM_EXT_TEMP = """
insert into {{ params.sat_name }}
select
   t.*
from
(
    select
        ext.*
        ,md5({% for col in params.hash_diff_cols %}
                    {{ col }} ||
                {% endfor %}
                '') hash_diff
    from {{ params.sat_name }}_ext ext
    where ext.load_dtm::time > '{{ params.from_dtm }}'
) t
left join {{ params.sat_name }} s
    on s.hash_diff = t.hash_diff
where
    s.hash_diff is null
;
"""

UPDATE_COUNTRY_SCORING = """
CREATE OR REPLACE VIEW data_vault.country_scoring_test_data_1 as
    select distinct
            mapp.iso as iso,
            cast('{pd.Timestamp.now()}' as timestamp) as load_dtm,
            mapp.country_eng as country,
            rrm.country as country_ru,
            rrm.tnved,
            rdm.comtrade_feat_1 as import_bulk_volume,
            import_from_russia as import_from_russia,
            rdm.av_tariff as average_tariff, rrm.barrier,
            rdm.common_import_part,
            rrm.exp_good_nisha,
    {{ fts_w_1 }}*fts_feat_1_rate + {{ fts_w_2 }}*fts_feat_2_rate + {{ fts_w_3 }}*fts_feat_3_rate +
    {{ comtrade_w_1 }}*comtrade_feat_1_rate + {{ comtrade_w_1 }}*comtrade_feat_2_rate +
    {{ h_index_w }}*h_index_rate + {{ av_tariff_w }}*av_tariff_rate + {{ pokrytie_merami_w }}*pokrytie_merami_rate +
    {{ transport_w }}*transport_place_rate + {{ pred_gdp_w }}*pred_gdp_rate +{{ pred_imp_w }}*pred_import_rate +
    {{ easy_bus_w }}*easy_doing_business_rate + quota_rate + exp_good_nisha_rate + exp_potential_rate + polit_coef_rate
    as score
    from data_vault.country_scoring_result_rating_dm rrm
    join data_vault.country_scoring_result_dm_1 as rdm
    on rrm.country = rdm.country and rrm.tnved = rdm.tnved
    join data_vault.country_scoring_mapper as mapp
    on lower(mapp.country_ru) = lower(rrm.country)
    where common_import_part_flg != 1;
"""