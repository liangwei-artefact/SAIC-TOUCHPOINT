pt=$3
hive --hivevar pt=$pt -e "
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;

insert overwrite table marketing_modeling.dw_ts_online_activity_i partition (pt，brand)
select * from
(
      select
            phone as mobile,
            ts as action_time,
            touchpoint_id,
            pt,
            brand
	from
      (
            select
                  loginuserid,
                  ts,
                  case
                  when pagevariable['pagetype_pvar'] = '金融产品介绍页' and applicationname = 'MGAPP' then '002010001000_tp' -- APP金融产品介绍页点击
                  when pagevariable['pagetype_pvar'] = '展厅首页' and applicationname = 'MGAPP' then '003001004000_tp' -- 展厅首页点击
                  when pagevariable['pagetype_pvar'] = '车型列表页' and applicationname = 'MGAPP' then '003001006000_tp' -- 车型列表页点击
                  when pagevariable['pagetype_pvar'] = '试驾首页' and applicationname = 'MGAPP' then '003001007000_tp' -- 试驾首页点击
                  when pagevariable['pagetype_pvar'] = '配置详情页' and applicationname = 'MGAPP' then '003001008000_tp' -- 配置详情页点击
                  when pagevariable['pagetype_pvar'] = '虚拟车控首页' and applicationname = 'MGAPP' then '003001009000_tp' -- 虚拟车控首页点击
                  when pagevariable['pagetype_pvar'] = '整车商城订购首页' and applicationname = 'MGAPP' then '003001010000_tp' -- 整车商城订购首页点击
                  when pagevariable['pagetype_pvar'] = '整车商城支付页' and applicationname = 'MGAPP' then '003001011000_tp' -- 整车商城支付页点击
                  when pagevariable['pagetype_pvar'] = '车型介绍页' and pagevariable['motorcycletype_pvar'] = '943' and applicationname = 'MGAPP' then '003001005001_tp' -- 点击全新MG5
                  when pagevariable['pagetype_pvar'] = '车型介绍页' and pagevariable['motorcycletype_pvar'] = '963' and applicationname = 'MGAPP' then '003001005002_tp' -- 点击MG领航
                  when pagevariable['pagetype_pvar'] = '车型介绍页' and pagevariable['motorcycletype_pvar'] = '983' and applicationname = 'MGAPP' then '003001005003_tp' -- 点击MG领航PHEV
                  when pagevariable['pagetype_pvar'] = '车型介绍页' and pagevariable['motorcycletype_pvar'] = '643' and applicationname = 'MGAPP' then '003001005004_tp' -- 点击MGHS
                  when pagevariable['pagetype_pvar'] = '车型介绍页' and pagevariable['motorcycletype_pvar'] = '703' and applicationname = 'MGAPP' then '003001005005_tp' -- 点击MGZS纯电动
                  when pagevariable['pagetype_pvar'] = '车型介绍页' and pagevariable['motorcycletype_pvar'] = '763' and applicationname = 'MGAPP' then '003001005006_tp' -- 点击eMGHS
                  when pagevariable['pagetype_pvar'] = '车型介绍页' and pagevariable['motorcycletype_pvar'] = '1123' and applicationname = 'MGAPP' then '003001005007_tp' -- 点击MG6 Xpower
                  when pagevariable['pagetype_pvar'] = '车型介绍页' and pagevariable['motorcycletype_pvar'] = '146' and applicationname = 'MGAPP' then '003001005008_tp' -- 点击MG3
                  when pagevariable['pagetype_pvar'] = '车型介绍页' and pagevariable['motorcycletype_pvar'] = '163' and applicationname = 'MGAPP' then '003001005009_tp' -- 点击MG6
                  when pagevariable['pagemodule_pvar'] = '消息中心' and pagevariable['pagetype_pvar'] = '消息中心首页' and applicationname = 'MGAPP' then '002008003001_tp' -- 消息中心首页访问
                  when pagevariable['pagemodule_pvar'] = '消息中心' and pagevariable['pagetype_pvar'] = '消息中心赞首页' and applicationname = 'MGAPP' then '002008003002_tp' -- 消息中心赞首页访问
                  when pagevariable['pagemodule_pvar'] = '消息中心' and pagevariable['pagetype_pvar'] = '消息中心评论首页' and applicationname = 'MGAPP' then '002008003003_tp' -- 消息中心评论首页访问
                  when pagevariable['pagemodule_pvar'] = '消息中心' and pagevariable['pagetype_pvar'] = '消息中心订单消息首页' and applicationname = 'MGAPP' then '002008003004_tp' -- 消息中心订单消息首页访问
                  when pagevariable['pagemodule_pvar'] = '消息中心' and pagevariable['pagetype_pvar'] = '消息中心活动消息首页' and applicationname = 'MGAPP' then '002008003005_tp' -- 消息中心活动消息首页访问
                  when pagevariable['pagemodule_pvar'] = '消息中心' and pagevariable['pagetype_pvar'] = '消息中心系统消息首页' and applicationname = 'MGAPP' then '002008003006_tp' -- 消息中心系统消息首页访问
                  when pagevariable['pagemodule_pvar'] = '每日福利' and pagevariable['pagetype_pvar'] = '每日福利首页' and applicationname = 'MGAPP' then '002009003002_tp' -- 每日福利首页浏览
                  when pagevariable['pagemodule_pvar'] = '每日福利' and pagevariable['pagetype_pvar'] = '积分明细首页' and applicationname = 'MGAPP' then '002009003003_tp' -- 积分明细首页浏览
                  when pagevariable['pagemodule_pvar'] = '每日福利' and pagevariable['pagetype_pvar'] = '积分明细-即将过期积分页' and applicationname = 'MGAPP' then '002009003004_tp' -- 积分明细-即将过期积分页浏览
                  when pagevariable['pagemodule_pvar'] = '每日福利' and pagevariable['pagetype_pvar'] = '积分明细-积分指南页' and applicationname = 'MGAPP' then '002009003005_tp' -- 积分明细-积分指南页浏览
                  when pagevariable['pagemodule_pvar'] = '服务' and pagevariable['pagetype_pvar'] = '服务首页' and applicationname = 'MGAPP' then '002009004001_tp' -- 服务首页访问
                  when pagevariable['pagemodule_pvar'] = '服务' and pagevariable['pagetype_pvar'] = '权益包领取页' and applicationname = 'MGAPP' then '002009004002_tp' -- 权益包领取页访问
                  when pagevariable['pagemodule_pvar'] = '服务' and pagevariable['pagetype_pvar'] = '优选网点首页' and applicationname = 'MGAPP' then '002009004003_tp' -- 优选网点首页访问
                  when pagevariable['pagemodule_pvar'] = '服务' and pagevariable['pagetype_pvar'] = '流量服务首页' and applicationname = 'MGAPP' then '002009004004_tp' -- 流量服务首页访问
                  when pagevariable['pagemodule_pvar'] = '服务' and pagevariable['pagetype_pvar'] = '滴滴代驾首页' and applicationname = 'MGAPP' then '002009004005_tp' -- 滴滴代驾首页访问
                  when pagevariable['pagemodule_pvar'] = '服务' and pagevariable['pagetype_pvar'] = '救援服务首页' and applicationname = 'MGAPP' then '002009004006_tp' -- 救援服务首页访问
                  when pagevariable['pagemodule_pvar'] = '服务' and pagevariable['pagetype_pvar'] = '充电服务首页' and applicationname = 'MGAPP' then '002009004007_tp' -- 充电服务首页访问

                  when pagevariable['pagetype_pvar'] = '整车分期选择页' and applicationname = 'RWAPP' then '002004001000_rw' -- 金融服务页面点击浏览
                  when pagevariable['pagetype_pvar'] = '预约试驾' and applicationname = 'RWAPP' then '007001001001_rw' -- 试驾首页浏览
                  when pagevariable['pagetype_pvar'] = '添加驾驶证' and applicationname = 'RWAPP' then '007001001002_rw' -- 试驾添加驾驶证
                  when pagevariable['pagetype_pvar'] = '挑选经销商' and applicationname = 'RWAPP' then '007001001003_rw' -- 试驾挑选经销商
                  when pagevariable['pagetype_pvar'] = '预约试驾' and applicationname = 'RWAPP' then '007001002002_rw' -- 试驾首页浏览
                  when pagevariable['pagetype_pvar'] = '服务首页' and applicationname = 'RWAPP' then '016001007000_rw' -- 服务首页访问
                  else NULL end as touchpoint_id,
                  case
                        when applicationname = 'MGAPP' then 'MG'
                        when applicationname = 'RWAPP' then 'RW'
                        else NULL
                  end as brand,
                  pt
            from dtwarehouse.cdm_growingio_activity_hma
            where
                  pt >= '${pt}'
      ) a
      join
      (
            select
                  cellphone as phone,
                  uid
            from
            (
                  select
                        cellphone,
                        uid,
                        Row_Number() OVER (partition by uid ORDER BY regist_date) rank_num
                  from dtwarehouse.ods_ccm_member
                  where pt = '${pt}'
            ) b0
            where rank_num = 1
      ) b
      on a.loginuserid = b.uid
) t1
where 
	mobile regexp '^[1][3-9][0-9]{9}$'
	AND action_time IS NOT NULL
	AND touchpoint_id IS NOT NULL
	AND brand IS NOT NULL
"