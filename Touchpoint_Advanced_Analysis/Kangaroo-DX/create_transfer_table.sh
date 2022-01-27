#!/bin/bash
hive -e "
DROP  TABLE IF EXISTS marketing_modeling.app_mk_attribution_report_t;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_mk_attribution_report_t (
  touchpoint_id STRING COMMENT '触点id',
  touchpoint_name STRING COMMENT '触点名称',
  attribution STRING COMMENT '马尔可夫贡献度结果',
  brand STRING COMMENT '品牌, MG or RW',
  attribution_tp STRING COMMENT '归因触点',
  pt STRING COMMENT '分区键，yyyymm 格式的月份，训练样本区间下限'
) STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_mk_attribution_report_t'
;


DROP  TABLE IF EXISTS marketing_modeling.app_ml_attribution_report_t;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_ml_attribution_report_t (
  touchpoint_id STRING COMMENT '触点id',
  touchpoint_name STRING COMMENT '触点名称',
  attribution STRING COMMENT '机器学习贡献度',
  brand STRING COMMENT '品牌, MG or RW',
  attribution_tp STRING COMMENT '归因触点',
  pt STRING COMMENT '分区键，yyyymm 格式的月份，训练样本区间下限'
) STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_ml_attribution_report_t'
;



DROP  TABLE IF EXISTS marketing_modeling.app_attribution_report_t;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_attribution_report_t (
    touchpoint_id STRING COMMENT '触点id',
    touchpoint_name STRING COMMENT '触点名称',
    instore_mk_attribution STRING,
    trial_mk_attribution STRING,
    deal_mk_attribution STRING,
    instore_ml_attribution STRING,
    trial_ml_attribution STRING,
    deal_ml_attribution STRING,
    pt STRING COMMENT '分区键，yyyymm 格式的月份，训练样本区间下限',
    brand STRING
) STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_attribution_report_t'
;


DROP TABLE IF EXISTS marketing_modeling.app_fir_contact_conversion_report_monthly_a_t;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_fir_contact_conversion_report_monthly_a_t (
  mac_code STRING COMMENT '大区代码',
  rfs_code STRING COMMENT '小区代码',
  area STRING COMMENT '首触省份',
  is_sec_net STRING COMMENT '是否二网 1:是 0：否',
  activity_name STRING COMMENT '首触活动名称',
  fir_contact_tp_id STRING COMMENT '线索来源触点ID',
  fir_contact_series STRING COMMENT '首触车系',
  fir_contact_month STRING COMMENT '首触所属月份',
  cust_vol BIGINT COMMENT '总人数，按电话号码去重',
  instore_vol BIGINT COMMENT '到店人数，按电话号码去重',
  trial_vol BIGINT COMMENT '试驾人数，按电话号码去重',
  consume_vol BIGINT COMMENT '订单人数(含交现车)，按电话号码去重',
  deliver_vol BIGINT COMMENT '交车人数，按电话号码去重',
  brand STRING COMMENT '品牌',
  pt STRING COMMENT '分区键，yyyymm 格式的日期') STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_fir_contact_conversion_report_monthly_a_t'
;

DROP TABLE IF EXISTS marketing_modeling.app_fir_contact_conversion_report_weekly_a_t;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_fir_contact_conversion_report_weekly_a_t (
  mac_code STRING COMMENT '大区代码',
  rfs_code STRING COMMENT '小区代码',
  area STRING COMMENT '首触省份',
  is_sec_net STRING COMMENT '是否二网 1:是 0：否',
  activity_name STRING COMMENT '首触活动名称',
  fir_contact_tp_id STRING COMMENT '线索来源触点ID',
  fir_contact_series STRING COMMENT '首触车系',
  fir_contact_week STRING COMMENT '首触所属周',
  cust_vol BIGINT COMMENT '总人数，按电话号码去重',
  instore_vol BIGINT COMMENT '到店人数，按电话号码去重',
  trial_vol BIGINT COMMENT '试驾人数，按电话号码去重',
  consume_vol BIGINT COMMENT '订单人数(含交现车)，按电话号码去重',
  deliver_vol BIGINT COMMENT '交车人数，按电话号码去重',
  brand STRING COMMENT '品牌',
  pt STRING COMMENT '分区键，yyyyww 格式的日期') STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_fir_contact_conversion_report_weekly_a_t'
;



DROP TABLE IF EXISTS marketing_modeling.app_touchpoints_profile_monthly_t;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_touchpoints_profile_monthly_t (
  mobile STRING COMMENT '电话号码',
  fir_contact_month STRING COMMENT '首触月份',
  fir_contact_date STRING COMMENT '首触时间',
  fir_contact_series STRING COMMENT '首触车系',
  mac_code STRING COMMENT '首触大区',
  rfs_code STRING COMMENT '首触小区代码',
  area STRING COMMENT '首触省份',
  is_sec_net STRING COMMENT '是否二网 1:是 0：否',
  activity_name STRING COMMENT '首触活动名称',
  fir_contact_tp_id STRING COMMENT '首触触点id',
  touchpoint_level string,
  level_1_tp_id string,
  brand STRING COMMENT '品牌',
  pt STRING COMMENT 'month used by partition, format: yyyymm'
) STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_touchpoints_profile_monthly_t'
;

DROP TABLE IF EXISTS marketing_modeling.app_touchpoints_profile_weekly_t;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_touchpoints_profile_weekly_t (
  mobile STRING COMMENT '电话号码',
  fir_contact_week STRING COMMENT '首触周',
  fir_contact_date STRING COMMENT '首触时间',
  fir_contact_series STRING COMMENT '首触车系',
  mac_code STRING COMMENT '首触大区',
  rfs_code STRING COMMENT '首触小区代码',
  area STRING COMMENT '首触省份',
  is_sec_net STRING COMMENT '是否二网 1:是 0：否',
  activity_name STRING COMMENT '首触活动名称',
  fir_contact_tp_id STRING COMMENT '首触触点id',
  brand STRING COMMENT '品牌',
  pt STRING COMMENT 'week used by partition, format: yyyyww'
) STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_touchpoints_profile_weekly_t'
;


DROP TABLE IF EXISTS marketing_modeling.app_tp_asset_report_a_t;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_tp_asset_report_a_t (
	  touchpoint_level INT COMMENT '触点等级',
	  touchpoint_id STRING COMMENT '触点码值',
	  fir_contact_month STRING COMMENT '首触月份',
	  fir_contact_tp_id STRING COMMENT '线索来源触点',
	  fir_contact_series STRING COMMENT '首触车系',
	  mac_code STRING COMMENT '大区代码',
	  rfs_code STRING COMMENT '小区代码',
	  area STRING COMMENT '省份',
	  tp_pv BIGINT COMMENT '触点覆盖人次',
	  tp_uv BIGINT COMMENT '触点覆盖独立用户数',
	  instore_vol BIGINT COMMENT '到店人数',
	  trial_vol BIGINT COMMENT '试驾人数',
	  consume_vol BIGINT COMMENT '成交人数（含订单和交车）',
	  cust_vol BIGINT COMMENT '首触用户数',
	  exit_pv BIGINT COMMENT '未成交用户中的停止人数，即未成交用户且最后一个触点是该触点的人数',
	  undeal_vol BIGINT COMMENT '未成交用户人数',
	  tp_coverage DOUBLE COMMENT '触点覆盖度',
	  tp_avg_times DOUBLE COMMENT '触点平均互动次数',
	  exit_rate DOUBLE COMMENT '触点停止率',
	  tp_instore_rate DOUBLE COMMENT '触点到店转化率',
	  tp_trial_rate DOUBLE COMMENT '触点试驾转化率',
	  tp_deal_rate DOUBLE COMMENT '触点成交转化率',
  pt STRING COMMENT '分区键，yyyymmdd格式的日期，数据生成日期',
  brand STRING COMMENT '分区键，品牌，MG/RW') STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_tp_asset_report_a_t'
  ;


DROP TABLE IF EXISTS marketing_modeling.cdm_customer_touchpoints_profile_a_t;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.cdm_customer_touchpoints_profile_a_t (
  mobile STRING COMMENT '电话号码',
  last_fir_contact_date_brand STRING COMMENT '首触时间',
  mac_code STRING COMMENT '首触大区代码',
  rfs_code STRING COMMENT '首触小区代码',
  area STRING COMMENT '首触省份',
  is_sec_net STRING COMMENT '是否二网 1:是 0：否',
  activity_name STRING COMMENT '首触活动名称',
  touchpoint_id STRING COMMENT '触点id',
  fir_contact_fir_sour_brand STRING COMMENT '一级线索来源',
  fir_contact_sec_sour_brand STRING COMMENT '二级线索来源',
  fir_contact_series_brand STRING COMMENT '首触车系',
  brand STRING COMMENT '品牌, MG or RW',
  pt STRING COMMENT 'date used by partition, format: yyyymmdd'
) STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/cdm_customer_touchpoints_profile_a_t'
;



DROP  TABLE IF EXISTS marketing_modeling.app_dim_area_t;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_dim_area_t (
  area STRING COMMENT '省份',
  brand STRING COMMENT '品牌, MG or RW'
) STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_dim_area_t'
;

DROP  TABLE IF EXISTS marketing_modeling.app_dim_activity_name_t;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_dim_activity_name_t (
  activity_name STRING COMMENT '活动名称',
  brand STRING COMMENT '品牌, MG or RW'
) STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_dim_activity_name_t'
;


DROP  TABLE IF EXISTS marketing_modeling.app_dim_car_series_t;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_dim_car_series_t (
  fir_contact_series STRING COMMENT '首触车系',
  brand STRING COMMENT '品牌, MG or RW',
  fir_contact_series_id string
) STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_dim_car_series_t'
;


DROP  TABLE IF EXISTS marketing_modeling.app_dim_tree_big_small_area_t;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_dim_tree_big_small_area_t (
  rfs_code STRING COMMENT '小区代码',
  mac_code STRING COMMENT '大区代码',
  rfs_name STRING COMMENT '小区中文名',
  mac_name STRING COMMENT '大区中文名',
  brand STRING COMMENT '品牌 RW,MG'
) STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_dim_tree_big_small_area_t'
;

DROP TABLE IF EXISTS marketing_modeling.app_month_map_week_t;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_month_map_week_t (
  month_key STRING COMMENT '月',
  clndr_wk_desc STRING COMMENT '周'
) STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_month_map_week_t';


DROP TABLE IF EXISTS marketing_modeling.cdm_dim_dealer_employee_info;
CREATE EXTERNAL TABLE marketing_modeling.cdm_dim_dealer_employee_info(
id bigint
,username varchar(100)
,realname varchar(100)
,cardno varchar(100)
,checkstatus varchar(100)
,brand_id varchar(100)
,chinese_name varchar(100)
,saic varchar(6)
,sales_code varchar(9)
,sales_name varchar(100)
,rfs_code varchar(9)
,rfs_name varchar(200)
,mac_code varchar(9)
,mac_name varchar(200)
,showroom_code varchar(100)
,dealer_code varchar(9)
,dealer_shortname varchar(100)
,dealer_level varchar(20)
,area varchar(100)
,city_name varchar(100)
,address varchar(255)
,postcode varchar(100)
,mobile varchar(100)
,email varchar(100)
,gender varchar(10)
,member_type_id tinyint
,status varchar(10)
,is_on_job varchar(10)
,birth date
,org_code varchar(100)
,entrytime timestamp
,leavetime timestamp
,create_date timestamp
,login_date timestamp
,is_sis tinyint
,roleid varchar(255)
,rolename varchar(255)
,labelname varchar(255)
,is_deleted tinyint
)
STORED AS orc
location '/user/hive/warehouse/marketing_modeling.db/cdm_dim_dealer_employee_info';



DROP TABLE IF EXISTS marketing_modeling.app_undeal_report_a_t;
create EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_undeal_report_a_t (
mobile string,
fir_contact_month STRING,
fir_contact_tp_id STRING,
tp_id string,
fir_contact_series STRING,
mac_code STRING,
rfs_code STRING,
area STRING,
undeal_vol int,
pt STRING COMMENT '分区键，yyyymmdd格式的日期，数据生成日期',
brand STRING COMMENT '分区键，品牌，MG/RW')
STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_undeal_report_a_t';



"

