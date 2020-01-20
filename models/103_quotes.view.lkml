view: 103_quotes {
  derived_table: {
     sql: SELECT drv.quote_id,
       TIMESTAMPDIFF(YEAR,drv.birth_dt,cov.cover_start_dt) AS age,
       MIN(TIMESTAMPDIFF (YEAR,drv.birth_dt,cov.cover_start_dt)) OVER (PARTITION BY drv.quote_id) AS min_age,
       drv.driver_id,
       ncb_years,
       rct_mi_12,
       vl.manufacturer,
       vehicle_model,
       vehicle_engine_size,
       ra.ad_rated_area,
       ra.ot_rated_area,
       ra.tp_rated_area,
       ra.ws_rated_area,
       ra.pi_rated_area,
       veh.abi_code,
       drv.no_claims,
       drv.no_convictions,
       rad.radar_no_bus_rules_failed,
       rad.rct_br047_strategic,
       rad.rct_br012_driverage,
       expc.ecos1_pvd1_s1_ndvalscore1 AS credit_score,
       cov.quotedpremium_in_notinclipt,
       rad.rct_member_score_unbanded,
       riskpremium_ap,
       riskpremium_an,
       mi.rct_mi_6 AS market_premium,
       drv.quote_dttm
FROM (SELECT quote_id,
             cover_start_dt,
             quotedpremium_in_notinclipt,
             risk_postcode,
             riskpremium_ap,
             riskpremium_an
      FROM qs_cover
      WHERE TIMESTAMPDIFF(DAY,quote_dttm,SYSDATE) <= 7) cov
  INNER JOIN qs_drivers drv ON cov.quote_id = drv.quote_id AND drv.driver_id = 0
  INNER JOIN qs_mi_outputs mi ON cov.quote_id = mi.quote_id
  INNER JOIN qs_vehicles veh ON cov.quote_id = veh.quote_id
  INNER JOIN rated_areas ra ON REPLACE (cov.risk_postcode,' ','') = ra.postcode
  LEFT JOIN vl_vehicle_data vl ON veh.abi_code = vl.abi_code
  INNER JOIN qs_experian_consumer_all expc ON cov.quote_id = expc.quote_id AND expc.driver_id = 0
  INNER JOIN qs_radar_return rad ON cov.quote_id = rad.quote_id
WHERE rct_mi_12 IS NOT NULL
AND   rct_mi_13 = '103';;
   }

  dimension: driver_id {
    description: "driver id"
    type: number
    sql: ${TABLE}.driver_id ;;
  }

   dimension: age {
     description: "Driver age"
     type: number
     sql: ${TABLE}.age ;;
   }
  dimension: would_accept {
    description: "Would accept quote but for strategic rule"
    type:  number
    sql: CASE WHEN (${TABLE}.radar_no_bus_rules_failed = 1 AND ${TABLE}.rct_br047_strategic = 1) OR ${TABLE}.radar_no_bus_rules_failed = 0 THEN 1 ELSE 0 END ;;
  }

  dimension: would_accept_24 {
    description: "Would accept quote 24 year old but for age rule"
    type:  number
    sql: CASE WHEN (${TABLE}.radar_no_bus_rules_failed = 1 AND ${TABLE}.rct_br012_driverage = 1) OR ${TABLE}.radar_no_bus_rules_failed = 0 THEN 1 ELSE 0 END ;;
  }

   dimension: min_age {
     description: "min age on policy"
     type: number
     sql: ${TABLE}.min_age ;;
   }

  dimension: credit_score {
    description: ""
    type: tier
    style:  interval
    tiers: [0,50,100,150,200,250,300,350,400,450,500,550,600,650,700,750,800,850,900,950,1000]
    sql: ${TABLE}.credit_score ;;
  }

  dimension: NCD {
    description: "NCD"
    type: string
    sql: CASE WHEN ${TABLE}.ncb_years > 9 THEN '9+' ELSE ${TABLE}.ncb_years END;;
  }

  dimension: Breakdown_Membership_Propensity_Banded {
    type: tier
    style: interval
    tiers: [0.005,0.010,0.015,0.02,0.025,0.03,0.035,0.04,0.045,0.05,0.055,0.06,0.065,0.07,0.075,0.08,0.085,0.09,0.095,0.1]
    sql: ${TABLE}.rct_mi_12 ;;
  }

  dimension: Breakdown_Membership_Propensity {
    description: "Membership propensity score"
    type: number
    sql: ${TABLE}.rct_mi_12 ;;
  }

  dimension: manufacturer {
    description: "vehicle manufacturer"
    type: string
    drill_fields: [model]
    sql: ${TABLE}.manufacturer ;;
  }
  dimension: model {
    description: "vehicle model"
    type: string
    drill_fields: [engine_size]
    sql: ${TABLE}.vehicle_model ;;
  }
  dimension: engine_size {
    description: "vehicle engine size"
    type: number
    sql: ${TABLE}.vehicle_engine_size ;;
  }
  dimension: ad_rated_area {
    description: "ad_rated_area"
    type: number
    sql: ${TABLE}.ad_rated_area ;;
  }
  dimension: ot_rated_area {
    description: "ot_rated_area"
    type: number
    sql: ${TABLE}.ot_rated_area ;;
  }
  dimension: tp_rated_area {
    description: "tp_rated_area"
    type: number
    sql: ${TABLE}.tp_rated_area ;;
  }
  dimension: ws_rated_area {
    description: "ws_rated_area"
    type: number
    sql: ${TABLE}.ws_rated_area ;;
  }
  dimension: pi_rated_area {
    description: "pi_rated_area"
    type: number
    sql: ${TABLE}.pi_rated_area ;;
  }
  dimension: claims {
    description: "number of claims"
    type: number
    sql: ${TABLE}.no_claims ;;
  }
  dimension: convictions {
    description: "number of convctions"
    type: number
    sql: ${TABLE}.no_convictions ;;
  }
  dimension: bus_rules_failed {
    description: "number of business_rules_failed"
    type: number
    sql: ${TABLE}.radar_no_bus_rules_failed ;;
  }
  dimension: strategic {
    description: "strategic rule failed"
    type: number
    sql: ${TABLE}.rct_br047_strategic ;;
  }
  dimension: age_rule {
    description: "age rule failed"
    type: number
    sql: ${TABLE}.rct_br012_driverage ;;
  }
  dimension: member_score_unbanded {
    description: "member score"
    type: number
    sql: ${TABLE}.rct_member_score_unbanded ;;
  }

   dimension_group: quote {
     description: "quote dttm"
     type: time
     timeframes: [date, week, month, year]
     sql: ${TABLE}.quote_dttm ;;
   }

  measure: av_membership_propensity {
    type: average
    sql: ${TABLE}.rct_mi_12 ;;
    value_format: "#.00;($#.00)"
  }

  measure: av_quoted_premium {
    type: average
    sql: ${TABLE}.quotedpremium_in_notinclipt ;;
    value_format: "#.00;($#.00)"
  }
  measure: av_risk_premium_ap {
    type: average
    sql: ${TABLE}.riskpremium_ap ;;
    value_format: "#.00;($#.00)"
  }
  measure: av_risk_premium_an {
    type: average
    sql: ${TABLE}.riskpremium_an ;;
    value_format: "#.00;($#.00)"
  }
  measure: av_market_premium {
    type: average
    sql: ${TABLE}.market_premium ;;
    value_format: "#.00;($#.00)"
  }
  measure: no_quotes {
    type:  count_distinct
    sql: ${TABLE}.quote_id ;;
    value_format: "#.00;($#.00)"
  }
 }
