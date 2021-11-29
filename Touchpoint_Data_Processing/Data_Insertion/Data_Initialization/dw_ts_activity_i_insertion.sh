pt1=$3
pt2=$4
pt1_date=$(date -d "-0 day $pt1 " +'%Y-%m-%d')
pt2_date=$(date -d "-0 day $pt2 " +'%Y-%m-%d')
queue_name=marketing_modeling


hive --hivevar pt1_date=$pt1_date --hivevar pt2_date=$pt2_date  -e "
DROP TABLE IF EXISTS marketing_modeling.tmp_dw_ts_activity_i;
CREATE table IF NOT EXISTS marketing_modeling.tmp_dw_ts_activity_i 
AS
SELECT 
    b.mobile as mobile,
    a.attend_time as act_time,
    c.saic_name as activity_name,
    c.saic_activity_codename as saic_type,
    CASE WHEN b.brand_id = 101 THEN 'RW' ELSE 'MG' END AS brand
FROM 
(
    SELECT cust_id, attend_time, activity_id 
    FROM dtwarehouse.ods_dlm_t_cust_activity 
    WHERE pt = ${pt2}
    AND attend_time >= '${pt1_date}' AND attend_time <= '${pt2_date}'
) a
LEFT JOIN
(
    SELECT 
        id, mobile, brand_id
    FROM
    (SELECT id, mobile, dealer_id FROM dtwarehouse.ods_dlm_t_cust_base WHERE pt = ${pt2}) a
    LEFT JOIN (SELECT dlm_org_id, brand_id FROM dtwarehouse.ods_rdp_v_sales_region_dealer WHERE pt = ${pt2}) b
    ON a.dealer_id = b.dlm_org_id
)b
ON a.cust_id = b.id
LEFT JOIN 
(SELECT * FROM dtwarehouse.ods_activity_saic_activity WHERE pt = ${pt2}) c
ON a.activity_id = c.saic_activityid
WHERE b.mobile IS NOT NULL AND a.attend_time IS NOT NULL
"

spark-submit --master yarn  \
--driver-memory 5G  \
--num-executors 10 \
--executor-cores 10 \
--executor-memory 32G \
--conf "spark.excutor.memoryOverhead=10G"  \
--queue $queuename \
dw_ts_activity_processing.py
