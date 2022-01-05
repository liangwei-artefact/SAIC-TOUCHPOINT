DROP TABLE IF EXISTS marketing_modeling.app_month_map_week;
CREATE EXTERNAL TABLE IF NOT EXISTS marketing_modeling.app_month_map_week (
  month_key STRING COMMENT '大区代码',
  clndr_wk_desc STRING COMMENT '小区代码'
) STORED AS ORC;