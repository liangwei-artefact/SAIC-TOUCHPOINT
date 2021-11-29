#/*********************************************************************
#*模块: /Touchpoint_Advanced_Analysis/Touchpoint_profile
#*程序: touchpoint_profile.sh
#*功能: 计算首触用户的属性的Shell script
#*开发人: Boyan XU
#*开发日: 2021-09-05
#*修改记录:  
#*    
#*********************************************************************/

pt=$3
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  ../../config/config.ini`

spark-submit --master yarn  \
--driver-memory 5G  \
--num-executors 10 \
--executor-cores 10 \
--executor-memory 32G \
--conf "spark.excutor.memoryOverhead=10G"  \
--queue $queuename \
touchpoint_profile.py $pt
