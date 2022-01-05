#!/usr/bin/python
# -*- coding: utf-8 -*-
# /*********************************************************************
# *模块: /Toichpoing_Advanced_Analysis/Attribution_report/
# *程序: attri_reprot_model_running.py
# *功能: 计算马尔可夫贡献度 & 机器学习贡献度
# *开发人:  Qian Wang & Xiaofeng XU
# *开发日: 2021-09-11
# *修改记录:
#
# *********************************************************************/

import re
import datetime
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings('ignore')
import sys
import logging
import yaml

from attri_report_data_processing import *

from pychattr.channel_attribution import MarkovModel
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

# ---【设置日志配置】---
logger = logging.getLogger("Attribution_Model_Score_Generation")
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler("Logs/attribution_model.log")
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
fh.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)

# ---【加载参数】---

## 需要进行计算的品牌，取值可以是"MG"或"RW"
brand = sys.argv[2]
cur_month_start = sys.argv[3]
cur_month_start = datetime.datetime.strptime(cur_month_start, "%Y%m%d")

## 归因对象 touchpoint_id, a list of tp id
## 定义触点的后缀，MG触点后缀为"tp"，RW为"rw"
suffix = {'MG': 'tp', 'RW': 'rw'}
if sys.argv[1] == 'instore':
    attribute_tp_ids = ['006000000000_{0}'.format(suffix[brand])]
elif sys.argv[1] == 'trial':
    attribute_tp_ids = ['007003000000_{0}'.format(suffix[brand]), '007004000000_{0}'.format(suffix[brand])]
elif sys.argv[1] == 'deal':
    attribute_tp_ids = ['011001000000_{0}'.format(suffix[brand]), '011002000000_{0}'.format(suffix[brand])]
logger.info("[{0} - {1}] attri_report_data_processing.py started".format(brand, sys.argv[1]))

# ---【加载Config文件】---
with open('removal_tp.yaml') as config_file:
    config = yaml.full_load(config_file)

## 获取不想用于计算贡献度的无效节点
rm_list = config['{0}_rm_list'.format(brand.lower())]


# ---【计算贡献度】---
def fetch_data():
    '''
    方法描述：读取触点数据和触点码表，并进行数据预处理

    参数说明：
      无

    返回值：处理好的触点行为数据dataframe和触点码表dataframe
    '''
    # 加载触点行为数据和触点码表
    df = pd.read_csv('{0}_tp_analysis_base.csv'.format(brand.lower()), sep='\t',
                     names=["mobile", "action_time", "touchpoint_id"])
    id_mapping = pd.read_csv('id_mapping.csv', sep='\t', names=["touchpoint_id", "touchpoint_name"])
    logger.info('[{0} - {1}] loaded dataset successfully'.format(brand, sys.argv[1]))
    logger.info('Fetch Data - Loaded {0} touchpoint data!'.format(str(df.shape)))
    logger.info('Fetch Data - Loaded {0} id_mapping data!'.format(str(id_mapping.shape)))

    # 数据预处理
    df_paths, touchpoint_list = preprocessing(df, rm_list, attribute_tp_ids, cur_month_start)
    logger.info('[{0} - {1}] preprocessed dataset successfully'.format(brand, sys.argv[1]))

    return df_paths, id_mapping, touchpoint_list


# 计算马尔可夫贡献度
def run_mk(df_paths):
    '''
    方法描述：计算马尔可夫贡献度, 并将结果存储到本地

    参数说明：
        df_paths: dataframe
        ）存储用户的触点行为数据的 pandas dataframe

    返回值：马尔可夫贡献度结果存储到本地
    '''
    try:
        ## 用逗号连接用户路径
        df_paths['path'] = df_paths['touchpoint_id'].apply(lambda x: ",".join(x))
        df_paths = df_paths[['mobile', 'path', 'conversion']]

        data = pd.DataFrame({
            "path": df_paths.path.values.tolist(),
            "conversions": df_paths.conversion.values.tolist()
        })

        # 马尔可夫模型训练
        logger.info('[{0} - {1}] MK model training started'.format(brand, sys.argv[1]))

        mm = MarkovModel(
            path_feature="path",
            conversion_feature="conversions",
            null_feature=None,
            separator=",",
            k_order=1,
            n_simulations=10000,
            max_steps=None,
            return_transition_probs=True,
            random_state=26
        )
        mm.fit(data)
        logger.info('[{0} - {1}] MK model training ended'.format(brand, sys.argv[1]))

        remove_effects = mm.removal_effects_.merge(
            id_mapping[['touchpoint_id', 'touchpoint_name']], \
            left_on='channel_name', \
            right_on='touchpoint_id').sort_values(by='removal_effect', ascending=False) \
            [['touchpoint_id', 'touchpoint_name', 'removal_effect']]

        # 计算贡献度的share
        remove_effects['share_in_result'] = remove_effects['removal_effect'] / remove_effects.removal_effect.sum()
        remove_effects['brand'] = brand

        # 将结果存储在本地
        remove_effects.to_csv('{0}_mk_attribution_report.csv'.format(brand.lower()), encoding='utf-8', index=False,mode='a')
        logger.info('[{0} - {1}] MK model result saved'.format(brand, sys.argv[1]))
        logger.info('[{0} - {1}] MK model terminated'.format(brand, sys.argv[1]))

    except:
        logger.error('\nError Reason:\n' + str(traceback.format_exc()))


# 计算机器学习贡献度
def run_ml(df_paths, touchpoint_list):
    '''
    方法描述：计算机器学习贡献度, 并将结果存储到本地

    参数说明：
        df_paths: dataframe
        ）存储用户的触点行为数据的 pandas dataframe

    返回值：机器学习贡献度结果存储到本地
    '''
    try:
        # 为每个触点产生是否存在的特征
        for i in touchpoint_list:
            tmp = '{}'.format(i)
            df_paths[tmp] = df_paths['touchpoint_id'].apply(lambda x: if_has_endpoint([i], x))

        X = df_paths.drop(columns=['mobile', 'touchpoint_id', 'conversion'])
        y = df_paths['conversion']

        X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y, random_state=20)
        clf = RandomForestClassifier(n_estimators=20, verbose=3, n_jobs=-1, random_state=20)
        logger.info('[{0} - {1}] ML model training started'.format(brand, sys.argv[1]))
        clf.fit(X_train, y_train)
        logger.info('[{0} - {1}] ML model training ended'.format(brand, sys.argv[1]))
        y_pred = clf.predict(X_test)

        data = {'feature_names': X_train.columns, 'feature_importance': clf.feature_importances_}
        fi_importance = pd.DataFrame(data).sort_values(by=['feature_importance'], ascending=False)

        fi_importance = fi_importance.merge(id_mapping[['touchpoint_id', 'touchpoint_name']], left_on='feature_names',
                                            right_on='touchpoint_id')
        fi_importance = fi_importance[['touchpoint_id', 'touchpoint_name', 'feature_importance']]
        fi_importance['brand'] = brand

        # 将结果存储在本地
        fi_importance.to_csv('{0}_ml_attribution_report.csv'.format(brand.lower()), encoding='utf-8', index=False,mode='a')
        logger.info('[{0} - {1}] result saved'.format(brand, sys.argv[1]))
        logger.info('[{0} - {1}] ML model terminated'.format(brand, sys.argv[1]))
    except:
        logger.error('\nError Reason:\n' + str(traceback.format_exc()))


if __name__ == "__main__":
    df_paths, id_mapping, touchpoint_list = fetch_data()
    run_ml(df_paths, touchpoint_list)
    run_mk(df_paths)
