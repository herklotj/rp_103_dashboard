connection: "echo_aapricing"

# include all the views
include: "*.view"

datagroup: 103_dashboard_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: 103_dashboard_default_datagroup

explore: 103_quotes {}

# explore: aap_claim_status {}

# explore: aap_mta {}

# explore: aap_nb {}

# explore: aap_ren {}

# explore: accident_years {}

# explore: acxiom_poc {}

# explore: breakdown_rpm {}

# explore: broker_motor_sales {}

# explore: broker_nb_sales_update {}

# explore: broker_sales {}

# explore: car_genie_devices {}

# explore: cf_broker_policy_voids {}

# explore: claim_latest_position20200108 {}

# explore: claim_latest_position_20200108 {}

# explore: cma_2018_nb {}

# explore: cma_2018_ren {}

# explore: cma_2018_xq {}

# explore: conf_23to25s {}

# explore: conf_members_2018_breakdowns {}

# explore: conf_members_2018_qts {}

# explore: conf_members_2018_sales {}

# explore: conf_mkt_prem {}

# explore: cue_last_run {}

# explore: dist_102_resv3_vs_aug18 {}

# explore: driving_school_ind {}

# explore: ec {}

# explore: ec_103 {}

# explore: ec_risk_modelling_201907 {}

# explore: ec_risk_modelling_201907_acx {}

# explore: ec_risk_modelling_201907_holdout {}

# explore: ec_risk_modelling_201907_sample {}

# explore: ec_samp {}

# explore: expoclm {}

# explore: expoclm1 {}

# explore: expoclm_aap_bk {}

# explore: expoclm_aap_bk_quarters {}

# explore: expoclm_bk {}

# explore: expoclm_hc {}

# explore: expoclm_quarters {}

# explore: expoclm_quarters_holdout {}

# explore: expoclm_quarters_sample {}

# explore: expoclm_sample {}

# explore: expoclm_vehicle_info {}

# explore: fca_2016_samp_pols {}

# explore: fca_2016_sampling {}

# explore: fca_2017_samp_pols {}

# explore: fca_2017_sampling {}

# explore: fca_2018_samp_pols {}

# explore: fca_2018_sampling {}

# explore: fca_samp_pols {}

# explore: financial_years {}

# explore: g_mod {}

# explore: g_mod2 {}

# explore: home_confused_cover_2019 {}

# explore: ice_20200101_snapshot {}

# explore: ice_20200102_snapshot_summary {}

# explore: ice_canc {}

# explore: ice_dim_claim_experiance {}

# explore: ice_known_missing {}

# explore: ice_mta {}

# explore: ice_nb {}

# explore: ice_policy_slots {}

# explore: ice_ren {}

# explore: ice_trn_claim_experiance {}

# explore: icepoclm_quarters {}

# explore: jack_market_test {}

# explore: jack_radar_export_conf {}

# explore: jack_radar_export_conf_ho {}

# explore: jack_reg_test {}

# explore: lk_h_expoclm_mth {}

# explore: lk_h_expoclm_mth_dl {}

# explore: ln_discrepencies {}

# explore: losstypecode_lookup {}

# explore: lr_dist_102_aug18_scores {}

# explore: lr_dist_102_resv3_scores {}

# explore: mem_prop_for_radar {}

# explore: missing_acx_factors {}

# explore: missing_ins_factors {}

# explore: ml_conf {}

# explore: ml_conf_join {}

# explore: ml_conf_pred_response {}

# explore: model_calibration {}

# explore: model_calibration_aug18 {}

# explore: model_calibration_aug18_20190916 {}

# explore: model_calibration_jul18 {}

# explore: model_calibration_jul18_20190916 {}

# explore: model_calibration_summary {}

# explore: model_calibration_summary_aug18 {}

# explore: model_calibration_summary_aug18_20190916 {}

# explore: model_calibration_summary_jul18 {}

# explore: model_calibration_summary_jul18_20190916 {}

# explore: mot_history {}

# explore: mot_lead {}

# explore: mot_test_item_2016 {}

# explore: mot_test_result_2016 {}

# explore: motor_model_calibrations {}

# explore: motor_model_calibrations_bkup {}

# explore: net_gross_lr_dist_aug18 {}

# explore: occupation_group {}

# explore: pattern_to_ultimate {}

# explore: pattern_to_ultimate_prior {}

# explore: policy_extract_control_temp {}

# explore: postcode_sector_declines {}

# explore: private_lease_all {}

# explore: private_lease_ulez {}

# explore: profile_comp_prelim {}

# explore: profile_comp_prelim_model {}

# explore: qre_numbers {}

# explore: qs_correlation_correction {}

# explore: r_mi_temp {}

# explore: ra_update {}

# explore: radar_channel {}

# explore: radar_exort_conf_aug {}

# explore: radar_exort_conf_oct {}

# explore: radar_exort_conf_sep {}

# explore: radar_export {}

# explore: radar_export_173 {}

# explore: radar_export_173_dedup {}

# explore: radar_export_complaint {}

# explore: radar_export_conf {}

# explore: radar_export_conf_102 {}

# explore: radar_export_conf_103 {}

# explore: radar_export_conf_cue {}

# explore: radar_export_conf_cue_sales {}

# explore: radar_export_conf_homemember_v2 {}

# explore: radar_export_conf_schemetemp {}

# explore: radar_export_confused_sales {}

# explore: radar_export_confused_sales1 {}

# explore: radar_export_conv_aug {}

# explore: radar_export_conv_jul {}

# explore: radar_export_hsls {}

# explore: radar_export_initial_173 {}

# explore: radar_export_initial_173_tmp {}

# explore: radar_export_nb {}

# explore: radar_export_plus_ds_flag {}

# explore: radar_export_plus_ds_flag2 {}

# explore: radar_export_plus_ds_flag3 {}

# explore: radar_export_rehost {}

# explore: radar_export_rehost_sample {}

# explore: radar_export_sales {}

# explore: radar_export_tmp_rehost {}

# explore: radar_ice_export {}

# explore: radar_ice_export_conf {}

# explore: radar_ice_export_conf_102 {}

# explore: radar_ice_export_conf_103 {}

# explore: radar_ice_export_sales {}

# explore: radar_ice_export_tmp {}

# explore: radar_ice_tmp_2 {}

# explore: radar_sales_renewalstest {}

# explore: radar_tmp_2_rehost {}

# explore: rapidminer_residuals {}

# explore: rated_areas_update {}

# explore: rdr_gbm_null_constants {}

# explore: renewal_ice_subset {}

# explore: renewal_ice_subset_102 {}

# explore: renewal_ice_subset_103 {}

# explore: renewal_radar_mi {}

# explore: renewal_radar_mi3 {}

# explore: renewal_subset {}

# explore: renewal_subset_102 {}

# explore: renewal_subset_103 {}

# explore: t20190313_quotes {}

# explore: test2 {}

# explore: today {}

# explore: uncal_temp_to_score {}

# explore: uncalibrated_ice_scores_toscore {}

# explore: uncalibrated_scores {}

# explore: uncalibrated_scores_aug18 {}

# explore: uncalibrated_scores_jul18 {}

# explore: uncalibrated_scores_jul19_cred {}

# explore: uncalibrated_scores_nm {}

# explore: uncalibrated_scores_toscore {}

# explore: uw_years {}

# explore: v_aaihl_bc_data {}

# explore: v_aap_edipol {}

# explore: v_aap_reservebreakdownhistory {}

# explore: v_all_claim_status {}

# explore: v_av_qtd_prem {}

# explore: v_avg_settled_distribution {}

# explore: v_avg_settled_incurred {}

# explore: v_ay_lr_development {}

# explore: v_ay_lr_development_old {}

# explore: v_batch_renewals {}

# explore: v_canc_at_inception {}

# explore: v_canc_development {}

# explore: v_cargenie_makes {}

# explore: v_cf_case_management {}

# explore: v_cf_keeperchange {}

# explore: v_cf_lookup {}

# explore: v_cg_quote_brs {}

# explore: v_claim_freq_wk {}

# explore: v_claim_status {}

# explore: v_claims_cumulative {}

# explore: v_claims_cumulative1 {}

# explore: v_claims_latest_position {}

# explore: v_claims_monthly_trends {}

# explore: v_claims_transactions {}

# explore: v_claims_transactions1 {}

# explore: v_claims_transactions_prelim {}

# explore: v_claims_transactions_prelim1 {}

# explore: v_claims_with_animals {}

# explore: v_clm_bordereaux_102uwy1 {}

# explore: v_clm_bordereaux_102uwy2 {}

# explore: v_clm_bordereaux_102uwy3 {}

# explore: v_clm_bordereaux_102uwy4 {}

# explore: v_clm_bordereaux_103uwy1 {}

# explore: v_clm_bordereaux_103uwy2 {}

# explore: v_clm_bordereaux_last_2m {}

# explore: v_confused_panel_stats {}

# explore: v_confused_ratios {}

# explore: v_conversion {}

# explore: v_conversion_report {}

# explore: v_conversion_wk {}

# explore: v_cue_discrepancies {}

# explore: v_cue_frequency {}

# explore: v_excess_latest_position {}

# explore: v_excess_tri {}

# explore: v_exp_and_earn_prem {}

# explore: v_exp_and_earn_prem_wk {}

# explore: v_experian_failures {}

# explore: v_factor_lrs {}

# explore: v_factor_lrs2 {}

# explore: v_features_with_chire {}

# explore: v_fnol_claims {}

# explore: v_fp_change_201902 {}

# explore: v_fy_lr_development {}

# explore: v_fy_lr_development_old {}

# explore: v_gross_claims_cumulative {}

# explore: v_gross_claims_latest_position {}

# explore: v_home_claims_extract {}

# explore: v_hourly_qts {}

# explore: v_hourly_sales {}

# explore: v_ice_canc_at_inception {}

# explore: v_ice_claim_status {}

# explore: v_ice_claims_cumulative {}

# explore: v_ice_claims_latest_position {}

# explore: v_ice_clm_bordereaux {}

# explore: v_ice_confused_ratios {}

# explore: v_ice_exp_and_earn_prem {}

# explore: v_ice_exp_and_earn_prem_wk {}

# explore: v_ice_extra_ad_reserves {}

# explore: v_ice_inception_strategy {}

# explore: v_ice_inforce_policies {}

# explore: v_ice_mtd_kpis {}

# explore: v_ice_policy_origin {}

# explore: v_ice_prem_earned {}

# explore: v_ice_prem_earned_tm {}

# explore: v_ice_prem_earned_wk {}

# explore: v_ice_reserving_premium_vectors {}

# explore: v_ice_trading_mi {}

# explore: v_ice_trading_stats {}

# explore: v_inception_strategy {}

# explore: v_inforce_policies {}

# explore: v_ln_discrepencies {}

# explore: v_ln_quotes_discrepencies {}

# explore: v_lr_pricing_stategy_ult {}

# explore: v_mi_canc_dev {}

# explore: v_mm_v_conf {}

# explore: v_model_abi_code {}

# explore: v_motor_renewals_yoy_spread {}

# explore: v_mta_by_model {}

# explore: v_mta_numbers {}

# explore: v_mtd_kpis {}

# explore: v_panel_share_stats {}

# explore: v_pmid_prem_earned {}

# explore: v_policy_origin {}

# explore: v_policy_slots_prelim {}

# explore: v_poq_failures_last_week {}

# explore: v_poq_failures_monthly {}

# explore: v_postal_region {}

# explore: v_prem_earned {}

# explore: v_prem_earned_old {}

# explore: v_prem_earned_recon {}

# explore: v_prem_earned_wk {}

# explore: v_prem_transactional {}

# explore: v_prem_written_recon {}

# explore: v_profile_comp {}

# explore: v_profile_comp_model {}

# explore: v_qre_amounts {}

# explore: v_qre_exposure {}

# explore: v_qre_numbers {}

# explore: v_qrt_claim_table {}

# explore: v_recovery_data {}

# explore: v_ren_yoys {}

# explore: v_renewal_batch {}

# explore: v_renewal_competitiveness {}

# explore: v_renewal_cover {}

# explore: v_renewal_shoppers {}

# explore: v_renewal_static_table {}

# explore: v_reserving_amounts {}

# explore: v_reserving_amounts_ice {}

# explore: v_reserving_numbers {}

# explore: v_reserving_numbers_ice {}

# explore: v_reserving_premium_vectors {}

# explore: v_reserving_premium_vectors_old {}

# explore: v_retention_stats {}

# explore: v_rpt_daily_br {}

# explore: v_sales_by_sold_date {}

# explore: v_same_day_conversion {}

# explore: v_sap_claim_pol {}

# explore: v_sap_claim_pol_home {}

# explore: v_sap_policy_uwy {}

# explore: v_sap_policy_uwy_home {}

# explore: v_sboc_history {}

# explore: v_scheme {}

# explore: v_scheme_detailed {}

# explore: v_slrs_aug18 {}

# explore: v_slrs_new_rm {}

# explore: v_slrs_nmjul18 {}

# explore: v_slrs_resv3 {}

# explore: v_strat_conv {}

# explore: v_strategy_kpis {}

# explore: v_trading_dash {}

# explore: v_trading_mi {}

# explore: v_trading_stats {}

# explore: v_uwy_lr_development {}

# explore: v_uwy_lr_development_old {}

# explore: v_vehicle_file_nd_update {}

# explore: v_vehicle_file_nd_update_uncleansed {}

# explore: v_xq_stats {}

# explore: vl_flag_manual_test {}

# explore: xq_ice_subset {}

# explore: xq_subset {}
