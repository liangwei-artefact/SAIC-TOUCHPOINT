#/*********************************************************************
#*模块: /Touchpoint_Advanced_Analysis/Contribution_report
#*程序: contri_report_compute_result.sh
#*功能: 计算首触用户的属性的Shell script
#*开发人: Liangwei Chen
#*开发日: 2021-12-26
#*修改记录:
#*
#*********************************************************************/
cd $(dirname $(readlink -f $0))
# MG / RW
brand=$1
queuename=`awk -F '=' '/\[HIVE\]/{a=1}a==1&&$1~/queue/{print $2;exit}'  config.ini`
echo "queuename"_${queuename}
spark-submit --master yarn  \
--driver-memory 5G  \
--num-executors 8 \
--executor-cores 8 \
--executor-memory 20G \
--conf "spark.excutor.memoryOverhead=10G"  \
--queue $queuename \
contri_report_compute_result.py $brand
