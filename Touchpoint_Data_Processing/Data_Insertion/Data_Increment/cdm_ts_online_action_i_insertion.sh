#!/bin/bash
#/*********************************************************************
#*模块: /Touchpoint_Data_Processing/Data_Increment/
#*程序: cdm_ts_online_action_i_insertion.sh
#*功能: 基于dtwarehouse.cdm_growingio_action_hma加工的一方触点相关触点
#*开发人: Junhai MA
#*开发日期: 2021-06-05
#*修改记录: 
#*          
#*********************************************************************/

pt=$3
hive --hivevar pt=$pt -e "
set hive.exec.dynamic.partition.mode=nonstrict;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=8192;
SET hive.exec.max.dynamic.partitions=2048;
SET hive.exec.max.dynamic.partitions.pernode=1000;

INSERT OVERWRITE TABLE marketing_modeling.cdm_ts_online_action_i PARTITION(pt,brand)

SELECT
	mobile,
	action_time,
	touchpoint_id,
	pt,
	brand
FROM
(
    SELECT
        register_login_phone_number AS mobile,
        ts AS action_time,
        CASE
		    WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'cardclick' THEN '002002003000_tp' -- 社区文章卡片点击
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'poster' THEN '002006003001_tp' -- 海报分享成功
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroom_poster' THEN '002006003002_tp' -- 展厅海报分享成功            
			WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'livedetailsplitclick' AND eventvariable['pitname_var'] = '领券' THEN '008002002004_tp' --直播详情页点击领券
			WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'livedetailsplitclick' AND eventvariable['pitname_var'] = '转发' THEN '008002002005_tp' -- 直播详情页点击评论
			WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'livedetailsplitclick' AND eventvariable['pitname_var'] = '点赞' THEN '008002002006_tp' -- 直播详情页点击点赞
			WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'livedetailsplitclick' AND eventvariable['pitname_var'] = '转发' THEN '008002002007_tp' -- 直播详情页点击转发
			WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'livedetailsplitclick' AND eventvariable['pitname_var'] = '评论' THEN '008002002008_tp' -- 直播详情页点击打赏			
			WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'osmessageclick' THEN '002008002002_tp' -- OS消息点击
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mallPlatformPitClick' THEN '002009002001_tp' -- 商城平台点击
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'fittingmallPitClick' THEN '002009002002_tp' -- 配件商城点击
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'shoppingmallpitclick' THEN '002009002003_tp' -- 积分商城首页点击
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'productlistclick' THEN '002009002004_tp' -- 积分商城商品列表区点击
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'productshare' THEN '002009002005_tp' -- 积分商城商品详情分享成功
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'benefitpitclick' THEN '002009003001_tp' -- 福利访问点击
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'pictureswip' THEN '003001001000_tp' -- 展厅首页_图片滑动
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroomlistclick' THEN '003001002000_tp' -- 展厅列表页按钮点击
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'carpictureswip' THEN '003002001000_tp' -- 展厅首页访问
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'carfeatureclick' THEN '003002002000_tp' -- 展厅车辆详情访问
            WHEN applicationname = 'MGAPP' AND act_name = 'reservedrive' THEN '007001001001_tp' -- 预约试驾成功
            WHEN domain = 'wx4916ea5d69638c01' AND act_name = 'reservedrive' THEN '007001002001_tp' -- 预约试乘试驾成功
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_pc_testdrive_submit' THEN '007001003001_tp' -- 名爵PC官网试乘试驾页-提交预约（https://m.saicmg.com/purchase/test-drive.html）
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_pc_mghs_testdrive_submit' THEN '007001003002_tp' -- 名爵PC官网HS车系页底部-提交预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_pc_mgehs_testdrive_submit' THEN '007001003003_tp' -- 名爵PC官网eHS车系页底部-提交预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_pc_newzc_testdrive_submit' THEN '007001003004_tp' -- 名爵PC官网newZS车系页底部-提交预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_pc_mgzs_testdrive_submit' THEN '007001003005_tp' -- 名爵PC官网ZS车系页底部-提交预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_pc_EZS_testdrive_submit' THEN '007001003006_tp' -- 名爵PC官网EZS车系页底部-提交预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_pc_mg6_testdrive_submit' THEN '007001003007_tp' -- 名爵PC官网620T车系页底部-提交预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_pc_emg6_testdrive_submit' THEN '007001003008_tp' -- 名爵PC官网650T车系页底部-提交预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_pc_mggs_testdrive_submit' THEN '007001003009_tp' -- 名爵PC官网GS车系页底部-提交预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_wap_testdrive_submit' THEN '007001003010_tp' -- 名爵WAP官网试乘试驾页-提交预约（https://www.saicmg.com/purchase/test-drive.html
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_wap_mghs_testdrive_submit' THEN '007001003011_tp' -- 名爵WAP官网HS车系页底部-提交预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_wap_mgehs_testdrive_submit' THEN '007001003012_tp' -- 名爵WAP官网eHS车系页底部-提交预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_wap_newzs_testdrive_submit' THEN '007001003013_tp' -- 名爵WAP官网newZS车系页底部-提交预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_wap_mgzs_testdrive_submit' THEN '007001003014_tp' -- 名爵WAP官网ZS车系页底部-提交预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_wap_mgezs_testdrive_submit' THEN '007001003015_tp' -- 名爵WAP官网EZS车系页底部-提交预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_wap_mg6_testdrive_submit' THEN '007001003016_tp' -- 名爵WAP官网620T车系页底部-提交预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_wap_emg6_testdrive_submit' THEN '007001003017_tp' -- 名爵WAP官网650T车系页底部-提交预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mg_wap_mggs_testdrive_submit' THEN '007001003018_tp' -- 名爵WAP官网GS车系页底部-提交预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'livelistpitclick' THEN '008002002001_tp' -- 直播列表页点击
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'livescreenclick' THEN '008002002002_tp' -- 直播列表页筛选
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'livedetailspitclick' THEN '008002002003_tp' -- 直播详情页点击
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'recommendActivity_entranceClick' THEN '008002003001_tp' -- 访问推荐注册活动
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'recommendActivityPage_shareClick' THEN '008002003002_tp' -- 分享推荐注册活动
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'recommendActivitySharePage_click' THEN '008002003003_tp' -- 填写推荐注册活动页信息
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'share_activity_gifthave' AND eventvariable['pitname_var'] = '邀请注册' THEN '008002004001_tp' -- 邀请注册
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'share_activity_gifthave' AND eventvariable['pitname_var'] = '邀请试驾' THEN '008002004002_tp' -- 邀请试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'share_activity_gifthave' AND eventvariable['pitname_var'] = '邀请购车' THEN '008002004003_tp' -- 邀请购车
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'gameactivity_buttonclick' THEN '008002005001_tp' -- 游戏点击
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'ask_drawback' THEN '013002001000_tp' -- 申请退款
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'drawback_done' THEN '013002002000_tp' -- 退款成功
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'mailclick' THEN '002008002001_tp' -- 站内信消息点击
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroompitclick' AND eventvariable['pitname_var'] ='试乘试驾' THEN '003001003001_tp' -- 展厅首页 - 试乘试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroompitclick' AND eventvariable['pitname_var'] ='虚拟体验' THEN '003001003002_tp' -- 展厅首页 - 虚拟体验
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroompitclick' AND eventvariable['pitname_var'] ='订购爱车' THEN '003001003003_tp' -- 展厅首页 - 订购爱车
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroompitclick' AND eventvariable['pitname_var'] ='客服' THEN '003001003004_tp' -- 展厅首页 - 客服
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroompitclick' AND eventvariable['pitname_var'] ='宣传图区' THEN '003001003005_tp' -- 展厅首页 - 宣传图区
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroompitclick' AND eventvariable['pitname_var'] ='优选网点-一键导航' THEN '003001003006_tp' -- 展厅首页 - 优选网点-一键导航
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroompitclick' AND eventvariable['pitname_var'] ='优选网点-预约试驾' THEN '003001003007_tp' -- 展厅首页 - 优选网点-预约试驾
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroompitclick' AND eventvariable['pitname_var'] ='优选网点-查看更多' THEN '003001003008_tp' -- 展厅首页 - 优选网点-查看更多
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroompitclick' AND eventvariable['pitname_var'] ='优选网点-电话' THEN '003001003009_tp' -- 展厅首页 - 优选网点-电话
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroompitclick' AND eventvariable['pitname_var'] ='金融服务' THEN '003001003010_tp' -- 展厅首页 - 金融服务
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroompitclick' AND eventvariable['pitname_var'] ='订购入口' THEN '003001003011_tp' -- 展厅首页 - 订购入口
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroompitclick' AND eventvariable['pitname_var'] ='最近活动' THEN '003001003012_tp' -- 展厅首页 - 最近活动
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroompitclick' AND eventvariable['pitname_var'] ='车型文字区' THEN '003001003013_tp' -- 展厅首页 - 车型文字区
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroompitclick' AND eventvariable['pitname_var'] ='车体图区' THEN '003001003014_tp' -- 展厅首页 - 车体图区
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroompitclick' AND eventvariable['pitname_var'] ='视频播放' THEN '003001003015_tp' -- 展厅首页 - 视频播放
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'showroompitclick' AND eventvariable['pitname_var'] ='视频播放' THEN '003001003015_tp' -- 展厅首页 - 视频播放
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'carfeatureclick' AND eventvariable['motorcycletype_var'] = '第三代MG6' THEN '003002002001_tp' -- 访问第三代MG6详情
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'carfeatureclick' AND eventvariable['motorcycletype_var'] = '全新一代MG ZS' THEN '003002002002_tp' -- 访问全新一代MG ZS详情
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'carfeatureclick' AND eventvariable['motorcycletype_var'] = 'MG eHS' THEN '003002002003_tp' -- 访问MG eHS详情
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'carfeatureclick' AND eventvariable['motorcycletype_var'] = '第三代MG6 PHEV' THEN '003002002004_tp' -- 访问第三代MG6 PHEV详情
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'carfeatureclick' AND eventvariable['motorcycletype_var'] = '纯电动MG EZS' THEN '003002002005_tp' -- 访问纯电动MG EZS详情
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'carfeatureclick' AND eventvariable['motorcycletype_var'] = 'MG HS' THEN '003002002006_tp' -- 访问MG HS详情
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'carfeatureclick' AND eventvariable['motorcycletype_var'] = 'MG eHS' THEN '003002002007_tp' -- 访问MG eHS详情
            WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'minepitclick' AND eventvariable['pitname_var'] = '帖子' THEN '002002004000_tp' -- 帖子浏览            
			WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'servicepitclick' THEN '016001001000_tp' -- 服务首页访问及点击
			WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'pick_upanddeliverServiceEntranceClick' THEN '016002001000_tp' -- 申请取送车（一般发生在维保）
			WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'second_handCarPageClick' THEN '016003001000_tp' -- 二手车服务访问
			WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'change_carPageSubmitClick' THEN '016003002000_tp' -- 二手车置换提交成功
			WHEN (applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01') AND act_name = 'find_dealerPageClick' THEN '016003003000_tp' -- 二手车经销商查询	 
			WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'poster' THEN '002003003001_rw' -- 海报分享成功
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'onShareAppMessage' THEN '002003003002_rw' -- 转发分享按钮点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'showroompitclick' THEN '003002001000_rw' -- 展厅首页核心按钮点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar'] IN ('预约试驾','试乘试驾') THEN '003002001001_rw' -- 预约试乘试驾
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar'] IN ('立即订购', '立即定购') THEN '003002001002_rw' -- 立即订购
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar'] IN ('立即预订', '立即预定' ,'立即预定6') THEN '003002001003_rw' -- 立即预订
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar'] IN ('盲订2', '盲定') THEN '003002001004_rw' -- 盲订
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar'] = '定制版订购' THEN '003002001005_rw' -- 定制版订购
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar'] IN ('九宫格抽奖','抽奖2') THEN '003002001007_rw' -- 抽奖
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar'] = '斗地主' THEN '003002001008_rw' -- 斗地主
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'vehicledetailmessage_pitclick' THEN '003002002001_rw' -- 车型详情页资讯点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'carfeatureswip' THEN '003002002002_rw' -- 展厅_车辆详情特点图片滑动
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'carpictureswip' THEN '003002002003_rw' -- 展厅_车型图片滑动
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'adviserbtnclick' THEN '003002004000_rw' -- 点击联系销售顾问
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'special_testdrivepage_testdriveclick' THEN '007001001004_rw' -- 预约特色试驾按钮点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'special_testdrive_entranceclick' THEN '007001001005_rw' -- 预约特色试驾入口点击
            WHEN applicationname = 'RWAPP' AND act_name = 'reservedrive' THEN '007001001006_rw' -- 预约试乘试驾成功
            WHEN domain = 'wx9425e8d1bc083073' AND act_name = 'reservedrive' THEN '007001002001_rw' -- 预约试乘试驾成功
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_pc_360PLUS_testdrive_submit' THEN '007001003001_rw' -- 上汽荣威_PC_360PLUS车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_pc_e950_testdrive_submit' THEN '007001003002_rw' -- 上汽荣威_PC_e950车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_pc_ei5_testdrive_submit' THEN '007001003003_rw' -- 上汽荣威_PC_Ei5车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_pc_ei6PLUS_testdrive_submit' THEN '007001003004_rw' -- 上汽荣威_PC_ei6PLUS车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_pc_erx5_testdrive_submit' THEN '007001003005_rw' -- 上汽荣威_PC_eRX5车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_pc_cerx5_testdrive_submit' THEN '007001003006_rw' -- 上汽荣威_PC_ERX5车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_pc_i5GL_testdrive_submit' THEN '007001003007_rw' -- 上汽荣威_PC_i5GL车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_pc_i5_testdrive_submit' THEN '007001003008_rw' -- 上汽荣威_PC_i5车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_pc_i6PLUS_testdrive_submit' THEN '007001003009_rw' -- 上汽荣威_PC_i6PLUS车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_pc_rx3_testdrive_submit' THEN '007001003010_rw' -- 上汽荣威_PC_rx3车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_pc_rx5emax_testdrive_submit' THEN '007001003011_rw' -- 上汽荣威_PC_RX5eMAX车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_pc_rx5max_testdrive_submit' THEN '007001003012_rw' -- 上汽荣威_PC_rx5max车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_pc_rx5_testdrive_submit' THEN '007001003013_rw' -- 上汽荣威_PC_rx5车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_pc_rx8_testdrive_submit' THEN '007001003014_rw' -- 上汽荣威_PC_rx8车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_pc_testdrive_submit' THEN '007001003015_rw' -- 上汽荣威_PC_预约试乘试驾页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_wap_ei5_testdrive_submit' THEN '007001003016_rw' -- 上汽荣威_WAP_Ei5车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_wap_i5GL_testdrive_submit' THEN '007001003017_rw' -- 上汽荣威_WAP_i5GL车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_wap_i6PLUS_testdrive_submit' THEN '007001003018_rw' -- 上汽荣威_WAP_i6PLUS车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_wap_rx3_testdrive_submit' THEN '007001003019_rw' -- 上汽荣威_WAP_rx3车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_wap_rx5emax_testdrive_submit' THEN '007001003020_rw' -- 上汽荣威_WAP_RX5eMAX车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_wap_rx5max_testdrive_submit' THEN '007001003021_rw' -- 上汽荣威_WAP_rx5max车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_wap_rx5_testdrive_submit' THEN '007001003022_rw' -- 上汽荣威_WAP_rx5车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_wap_rx8_testdrive_submit' THEN '007001003023_rw' -- 上汽荣威_WAP_rx8车系页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'rw_wap_testdrive_submit' THEN '007001003024_rw' -- 上汽荣威_WAP_预约试乘试驾页_预约试乘试驾提交
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'homepage_moduleclick' THEN '002002001000_rw' -- 首页推荐点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar']='展厅' THEN '002002002001_rw' -- APP
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar']='我的' THEN '002002002002_rw' -- APP
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar']='威社区' THEN '002002002003_rw' -- APP
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar']='爱车' THEN '002002002004_rw' -- APP
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar']='无忧服务' THEN '002002002005_rw' -- APP
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar']='优品' THEN '002002002006_rw' -- APP
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar']='签到' THEN '002002002007_rw' -- APP
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar']='消息中心' THEN '002002002008_rw' -- APP
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar']='搜索' THEN '002002002009_rw' -- APP
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar']='车友圈' THEN '002002002010_rw' -- APP
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar']='服务' THEN '002002002011_rw' -- APP
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar']='车信' THEN '002002002012_rw' -- APP
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar']='探索' THEN '002002002013_rw' -- APP
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar']='推荐' THEN '002002002014_rw' -- APP
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar']='合伙人计划' THEN '002002002015_rw' -- APP
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND eventvariable['pitnamevar']='搜索框' THEN '002002002016_rw' -- APP
            WHEN applicationname IN ('RWAPP') AND act_name = 'playvideo' THEN '002002005000_rw' -- APP视频播放
            WHEN applicationname IN ('RWAPP') AND act_name = 'share' THEN '002002010000_rw' --APP分享/转发
            WHEN applicationname IN ('RWAPP') AND act_name = 'searchresultclick' THEN '002002011001_rw' --APP_搜索结果点击
            WHEN applicationname IN ('RWAPP') AND act_name = 'searchsuccess' THEN '002002011002_rw' --APP_完成搜索结果展示
            WHEN applicationname IN ('RWAPP') AND act_name = 'search' THEN '002002011003_rw' --APP_搜索
            WHEN applicationname IN ('RWAPP') AND act_name = 'IM_entranceclick' THEN '002002012001_rw' --APP消息使用_IM入口点击
            WHEN applicationname IN ('RWAPP') AND act_name = 'msgpush_pitclick' THEN '002002012002_rw' --APP消息使用_消息推送弹窗点击
            WHEN applicationname IN ('RWAPP') AND act_name = 'msgcenterpage_pitclick' THEN '002002012003_rw' --APP消息使用_消息中心页面中的各按钮点击
            WHEN applicationname IN ('RWAPP') AND act_name = 'IM_changebind_submitclick' THEN '002002012004_rw' --APP消息使用_IM换绑提交按钮点击
            WHEN applicationname IN ('RWAPP') AND act_name = 'IM_carmessagepage_salesclick' THEN '002002012006_rw' --APP消息使用_IM车信首页销售顾问按钮点击
            WHEN applicationname IN ('RWAPP') AND act_name = 'IM_call' THEN '002002012007_rw' --APP消息使用_IM通话成功
            WHEN applicationname IN ('RWAPP') AND act_name = 'IM_sentmessage' THEN '002002012008_rw' --APP消息使用_IM发送消息成功
            WHEN applicationname IN ('RWAPP') AND act_name = 'IM_chatwindowpitclick' THEN '002002012009_rw' --APP消息使用_IM聊天窗口页坑位点击
            WHEN applicationname IN ('RWAPP') AND act_name = 'fittingmallpitclick' THEN '002002014001_rw' --APP配件商城_荣威配件商城首页核心按钮点击
            WHEN domain = 'wx9425e8d1bc083073' AND act_name = 'searchresultclick' THEN '002003001001_rw' --小程序_搜索结果点击
            WHEN domain = 'wx9425e8d1bc083073' AND act_name = 'search' THEN '002003001002_rw' --小程序_完成搜索
            WHEN domain = 'wx9425e8d1bc083073' AND act_name = 'IM_changebind_submitclick' THEN '002003002001_rw' --小程序_IM换绑提交按钮点击
            WHEN domain = 'wx9425e8d1bc083073' AND act_name = 'IM_login' THEN '002003002002_rw' --小程序_IM环信登录成功
            WHEN domain = 'wx9425e8d1bc083073' AND act_name = 'IM_carmessagepage_salesclick' THEN '002003002003_rw' --小程序_IM车信首页销售顾问按钮点击
            WHEN domain = 'wx9425e8d1bc083073' AND act_name = 'IM_call' THEN '002003002004_rw' --小程序_IM通话成功
            WHEN domain = 'wx9425e8d1bc083073' AND act_name = 'IM_sentmessage' THEN '002003002005_rw' --小程序_IM发送消息成功
            WHEN domain = 'wx9425e8d1bc083073' AND act_name = 'IM_chatwindowpitclick' THEN '002003002006_rw' --小程序_IM聊天窗口页坑位点击
            WHEN domain = 'wx9425e8d1bc083073' AND act_name = 'IM_entranceclick' THEN '002003002007_rw' --小程序_IM入口点击
            WHEN domain = 'wx9425e8d1bc083073' AND act_name = 'poster' THEN '002003003001_rw' --小程序_海报分享成功
            WHEN domain = 'wx9425e8d1bc083073' AND act_name = 'onShareAppMessage' THEN '002003003002_rw' --小程序_转发分享按钮点击
            WHEN domain = 'wx9425e8d1bc083073' AND act_name = 'playvideo' THEN '002003004000_rw' --小程序视频播放
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'PC_roewe_financial_calculator_policy_apply_click' THEN '002004003001_rw' --上汽荣威_PC_金融服务页_计算器_金融政策_立即申请_点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'PC_roewe_financial_calculator_qiqiudai_apply_click' THEN '002004003002_rw' --上汽荣威_保险计算器查询页_子险选择
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'PC_roewe_financial_loan_submit_click' THEN '002004003003_rw' --上汽荣威_PC_金融服务页_我要贷款_提交_点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'favoritecarpitclick' THEN '003001001001_rw' --App展厅访问爱车_首页核心坑位点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'cardesignhighlight' THEN '003001002001_rw' --车型浏览_车型详情亮点
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'pictureswip' THEN '003001002002_rw' --车型浏览_爱车_图片滑动
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'share_activity_gifthave' THEN '008002002002_rw' --合伙人邀请点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'key_click_gifthave' THEN '008002002003_rw' --合伙人二期核心坑位点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'activitybuttonclick_11' THEN '008002002004_rw' --双十一活动商品按钮点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'serviceDrawPage_rulesClick' THEN '008002002005_rw' --抽奖页面-活动规则点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'serviceActivityPage_drawnowClick' THEN '008002002006_rw' --抽奖活动详情页-立即抽奖按钮点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'serviceJulyEverydaydrawPage_rulesClick' THEN '008002002007_rw' --售后7月天天抽奖页面-活动规则点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'serviceJulyEverydaydrawPage_drawClick' THEN '008002002008_rw' --售后7月天天抽奖页面-抽奖按钮点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'air_conditionerEverydaydrawPage_rulesClick' THEN '008002002009_rw' --空调养护服务天天抽奖页面-活动规则点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'air_conditionerEverydaydrawPage_drawClick' THEN '008002002010_rw' --空调养护服务天天抽奖页面-抽奖按钮点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'leads_33yi' THEN '008002001001_rw' --33亿促销活动留资
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'registrationactivities' THEN '008002002011_rw' --报名活动
            WHEN applicationname IN ('RWAPP') AND act_name = 'fightTheLandlord_buttonclick' THEN '008002003001_rw' --APP游戏参与_斗地主活动按钮点击
            WHEN domain = 'wx9425e8d1bc083073' AND act_name = 'fightTheLandlord_buttonclick' THEN '008002005001_rw' --小程序游戏参与_斗地主活动按钮点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'ask_drawback' THEN '013002001000_rw' --小程序退款_申请退款
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'servicepageicon' THEN '016001002000_rw' --服务首页icon点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'pick_upanddeliverServiceEntranceClick' THEN '016001003000_rw' --申请取送车服务入口点击情况
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'washcarsearchresultclick' THEN '016001004000_rw' --洗车服务搜索结果点击
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'washcarsearch' THEN '016001005000_rw' --洗车服务完成搜索
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'shoppingindex' THEN '016001006000_rw' --服务首页优品商城轮播位点击    
            WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'cardclick' THEN '002002003002_rw' -- 社区文章卡片点击
			WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'minepitclick' and eventvariable['pitname_var'] = '帖子' THEN '002002003003_rw' -- 帖子浏览
			WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'cc_view' THEN '002002003004_rw' -- 知识库内容阅读
			WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'forumpageclick' THEN '002002004001_rw' -- 论坛列表页模块点击
			WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'topicarticleclick' THEN '002002004002_rw' -- 资讯专题页文章点击
			WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'recomaticleclick' THEN '002002004003_rw' -- 社区_推荐文章点击
			WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'topicbattleinbbsclick' THEN '002002004004_rw' -- 话题对垒论坛入口点击
			WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'topicbattleinmrflclick' THEN '002002004005_rw' -- 话题对垒每日福利入口点击
			WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'tbattlecommsclick' THEN '002002004006_rw' -- 话题对垒评论点击
			WHEN (applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073') AND act_name = 'cardclick' THEN '002002004007_rw' -- 社区_卡片点击
            ELSE NULL
        END AS touchpoint_id,
        CASE
            WHEN applicationname IN ('MGAPP', 'MG官网') OR domain = 'wx4916ea5d69638c01' THEN 'MG'
            WHEN applicationname IN ('RWAPP', 'RW官网') OR domain = 'wx9425e8d1bc083073' THEN 'RW'
            ELSE NULL END AS brand,
        pt
    FROM dtwarehouse.cdm_growingio_action_hma
    WHERE
        pt >= '${pt}'
        AND register_login_phone_number regexp '^[1][3-9][0-9]{9}$'

    UNION ALL

    SELECT
        cellphone AS mobile,
        create_date AS action_time,
        '002009002006_tp' AS touchpoint_id, -- 商城下单
        brand,
        pt
    FROM
    (
        SELECT
            buyer_id,
            create_date,
            vehicle_brand AS brand,
            REGEXP_REPLACE(TO_DATE(create_date), '-', '') AS pt
        FROM linkflow.ods_dbuom_uom_order
        WHERE pt = '${pt}'  -- 全量表取最新pt再切取
            AND REGEXP_REPLACE(TO_DATE(create_date), '-', '') >= '${pt}'
			AND vehicle_brand = 'MG'
    ) b0
    join
    (
        SELECT
            cellphone,
            uid
        FROM
        (
            SELECT
                cellphone,
                uid,
                ROW_NUMBER() OVER (PARTITION BY uid ORDER BY regist_date) rank_num
            FROM dtwarehouse.ods_ccm_member
            WHERE
                -- pt = '20210531'
                pt = '${pt}' -- 生产环境下用这一行
        ) b0
        WHERE rank_num = 1
    ) b1
    ON b0.buyer_id = b1.uid
    WHERE
        cellphone regexp '^[1][3-9][0-9]{9}$'
        AND create_date IS NOT NULL
) a
WHERE
    touchpoint_id IS NOT NULL
    AND action_time IS NOT NULL
"