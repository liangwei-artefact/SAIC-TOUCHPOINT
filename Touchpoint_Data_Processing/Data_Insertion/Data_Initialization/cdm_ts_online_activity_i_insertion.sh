#!/bin/bash
pt2=$3
pre_day=$4
pt1=$(date -d "${pt2} -$pre_day day" '+%Y%m%d')
cd $(dirname $(readlink -f $0))
queue_name=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`
hive --hivevar pt1=$pt1 --hivevar pt2=$pt2 --hivevar queue_name=${queue_name} -e "
set tez.queue.name=${queue_name};
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;

insert overwrite table marketing_modeling.cdm_ts_online_activity_i partition (pt,brand)
select
mobile,
action_time,
touchpoint_id,
cast(pt as string) pt,
cast(brand as string) brand
from ( select
phone mobile,
ts action_time,
touchpoint_id,
brand,
pt
from
(
select
phone,
to_utc_timestamp(detail['ts'],'yyyy-MM-dd HH:mm:ss'),
case
when detail['pagetype'] = '金融产品介绍页' and detail['applicationname'] = 'MGAPP' then '002010001000_tp' -- APP金融产品介绍页点击
when detail['pagetype'] = '展厅首页' and detail['applicationname'] = 'MGAPP' then '003001004000_tp' -- 展厅首页点击
when detail['pagetype'] = '车型列表页' and detail['applicationname'] = 'MGAPP' then '003001006000_tp' -- 车型列表页点击
when detail['pagetype'] = '试驾首页' and detail['applicationname'] = 'MGAPP' then '003001007000_tp' -- 试驾首页点击
when detail['pagetype'] = '配置详情页' and detail['applicationname'] = 'MGAPP' then '003001008000_tp' -- 配置详情页点击
when detail['pagetype'] = '虚拟车控首页' and detail['applicationname'] = 'MGAPP' then '003001009000_tp' -- 虚拟车控首页点击
when detail['pagetype'] = '整车商城订购首页' and detail['applicationname'] = 'MGAPP' then '003001010000_tp' -- 整车商城订购首页点击
when detail['pagetype'] = '整车商城支付页' and detail['applicationname'] = 'MGAPP' then '003001011000_tp' -- 整车商城支付页点击
when detail['pagetype'] = '车型介绍页' and detail['motorcycletype_var'] = '943' and detail['applicationname'] = 'MGAPP' then '003001005001_tp' -- 点击全新MG5
when detail['pagetype'] = '车型介绍页' and detail['motorcycletype_var'] = '963' and detail['applicationname'] = 'MGAPP' then '003001005002_tp' -- 点击MG领航
when detail['pagetype'] = '车型介绍页' and detail['motorcycletype_var'] = '983' and detail['applicationname'] = 'MGAPP' then '003001005003_tp' -- 点击MG领航PHEV
when detail['pagetype'] = '车型介绍页' and detail['motorcycletype_var'] = '643' and detail['applicationname'] = 'MGAPP' then '003001005004_tp' -- 点击MGHS
when detail['pagetype'] = '车型介绍页' and detail['motorcycletype_var'] = '703' and detail['applicationname'] = 'MGAPP' then '003001005005_tp' -- 点击MGZS纯电动
when detail['pagetype'] = '车型介绍页' and detail['motorcycletype_var'] = '763' and detail['applicationname'] = 'MGAPP' then '003001005006_tp' -- 点击eMGHS
when detail['pagetype'] = '车型介绍页' and detail['motorcycletype_var'] = '1123' and detail['applicationname'] = 'MGAPP' then '003001005007_tp' -- 点击MG6 Xpower
when detail['pagetype'] = '车型介绍页' and detail['motorcycletype_var'] = '146' and detail['applicationname'] = 'MGAPP' then '003001005008_tp' -- 点击MG3
when detail['pagetype'] = '车型介绍页' and detail['motorcycletype_var'] = '163' and detail['applicationname'] = 'MGAPP' then '003001005009_tp' -- 点击MG6
when detail['pagemodule_pvar'] = '消息中心' and detail['pagetype'] = '消息中心首页' and detail['applicationname'] = 'MGAPP' then '002008003001_tp' -- 消息中心首页访问
when detail['pagemodule_pvar'] = '消息中心' and detail['pagetype'] = '消息中心赞首页' and detail['applicationname'] = 'MGAPP' then '002008003002_tp' -- 消息中心赞首页访问
when detail['pagemodule_pvar'] = '消息中心' and detail['pagetype'] = '消息中心评论首页' and detail['applicationname'] = 'MGAPP' then '002008003003_tp' -- 消息中心评论首页访问
when detail['pagemodule_pvar'] = '消息中心' and detail['pagetype'] = '消息中心订单消息首页' and detail['applicationname'] = 'MGAPP' then '002008003004_tp' -- 消息中心订单消息首页访问
when detail['pagemodule_pvar'] = '消息中心' and detail['pagetype'] = '消息中心活动消息首页' and detail['applicationname'] = 'MGAPP' then '002008003005_tp' -- 消息中心活动消息首页访问
when detail['pagemodule_pvar'] = '消息中心' and detail['pagetype'] = '消息中心系统消息首页' and detail['applicationname'] = 'MGAPP' then '002008003006_tp' -- 消息中心系统消息首页访问
when detail['pagemodule_pvar'] = '每日福利' and detail['pagetype'] = '每日福利首页' and detail['applicationname'] = 'MGAPP' then '002009003002_tp' -- 每日福利首页浏览
when detail['pagemodule_pvar'] = '每日福利' and detail['pagetype'] = '积分明细首页' and detail['applicationname'] = 'MGAPP' then '002009003003_tp' -- 积分明细首页浏览
when detail['pagemodule_pvar'] = '每日福利' and detail['pagetype'] = '积分明细-即将过期积分页' and detail['applicationname'] = 'MGAPP' then '002009003004_tp' -- 积分明细-即将过期积分页浏览
when detail['pagemodule_pvar'] = '每日福利' and detail['pagetype'] = '积分明细-积分指南页' and detail['applicationname'] = 'MGAPP' then '002009003005_tp' -- 积分明细-积分指南页浏览
when detail['pagemodule_pvar'] = '服务' and detail['pagetype'] = '服务首页' and detail['applicationname'] = 'MGAPP' then '002009004001_tp' -- 服务首页访问
when detail['pagemodule_pvar'] = '服务' and detail['pagetype'] = '权益包领取页' and detail['applicationname'] = 'MGAPP' then '002009004002_tp' -- 权益包领取页访问
when detail['pagemodule_pvar'] = '服务' and detail['pagetype'] = '优选网点首页' and detail['applicationname'] = 'MGAPP' then '002009004003_tp' -- 优选网点首页访问
when detail['pagemodule_pvar'] = '服务' and detail['pagetype'] = '流量服务首页' and detail['applicationname'] = 'MGAPP' then '002009004004_tp' -- 流量服务首页访问
when detail['pagemodule_pvar'] = '服务' and detail['pagetype'] = '滴滴代驾首页' and detail['applicationname'] = 'MGAPP' then '002009004005_tp' -- 滴滴代驾首页访问
when detail['pagemodule_pvar'] = '服务' and detail['pagetype'] = '救援服务首页' and detail['applicationname'] = 'MGAPP' then '002009004006_tp' -- 救援服务首页访问
when detail['pagemodule_pvar'] = '服务' and detail['pagetype'] = '充电服务首页' and detail['applicationname'] = 'MGAPP' then '002009004007_tp' -- 充电服务首页访问
when detail['pagetype'] = '整车分期选择页' and detail['applicationname'] = 'RWAPP' then '002004001000_rw' -- 金融服务页面点击浏览
when detail['pagetype'] = '预约试驾' and detail['applicationname'] = 'RWAPP' then '007001001001_rw' -- 试驾首页浏览
when detail['pagetype'] = '添加驾驶证' and detail['applicationname'] = 'RWAPP' then '007001001002_rw' -- 试驾添加驾驶证
when detail['pagetype'] = '挑选经销商' and detail['applicationname'] = 'RWAPP' then '007001001003_rw' -- 试驾挑选经销商
when detail['pagetype'] = '预约试驾' and detail['applicationname'] = 'RWAPP' then '007001002002_rw' -- 试驾首页浏览
when detail['pagetype'] = '服务首页' and detail['applicationname'] = 'RWAPP' then '016001007000_rw' -- 服务首页访问
else NULL end as touchpoint_id,
case
when detail['applicationname'] = 'MGAPP' then 'MG'
when detail['applicationname'] = 'RWAPP' then 'RW'
else NULL
end as brand,
regexp_replace(to_date(to_utc_timestamp(detail['ts'],'yyyy-MM-dd HH:mm:ss')), "-", "") as pt,
to_utc_timestamp(detail['ts'],'yyyy-MM-dd HH:mm:ss')  ts
from
cdp.cdm_cdp_customer_behavior_detail
where
type='contactor' and pt between '${pt1}' and '${pt2}'
and regexp_replace(to_date(to_utc_timestamp(detail['ts'],'yyyy-MM-dd HH:mm:ss')), "-", "") >= '${pt1}' and regexp_replace(to_date(to_utc_timestamp(detail['ts'],'yyyy-MM-dd HH:mm:ss')), "-", "") <= '${pt2}'
) a
) t1
where
mobile regexp '^[1][3-9][0-9]{9}$'
AND action_time IS NOT NULL
AND touchpoint_id IS NOT NULL
AND brand IS NOT NULL
"