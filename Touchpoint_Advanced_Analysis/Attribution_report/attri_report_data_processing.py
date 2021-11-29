#!/usr/bin/python
# -*- coding: utf-8 -*-
# /*********************************************************************
# *模块: /Toichpoing_Advanced_Analysis/Attribution_report/
# *程序: attri_reprot_ml_model.py
# *功能: 计算机器学习贡献度
# *开发人: Qian Wang & Boyan Xu
# *开发日: 2021-06-20
# *修改记录:
# *********************************************************************/

import re
import sys
import datetime
import pandas as pd
import numpy as np


def rm_unwanted_tp(df, rm_list):
    '''
    方法描述：去除数据集中的一些不想用于计算贡献度的无效节点

    参数说明：
        df: dataframe
        ）存储日期列的Pandas数据框
        rv_list: list
        ）第一个日期的列名

    返回值：清除了无效节点后的触点数据集
    '''
    df.dropna(inplace=True)
    df = df[df.touchpoint_id.isin(rm_list) == False]
    return df


def if_has_endpoint(touchpoint_ids, list1):
    '''
    方法描述：用于判断一串行为数据中是否含有寻找的目标节点

    参数说明：
        touchpoint_ids: list
        ）每个用户的触点行为数据
        list1: list
        ）存储目标节点的list，因为目标节点可能不只有一个

    返回值：如果list touchpoint_ids中含有任一list1中的触点，则返回1，否则返回0
    '''
    for touchpoint_id in touchpoint_ids:
        if touchpoint_id in list1:
            return 1
    return 0


def exclude_endpoint(touchpoint_ids, list1):
    '''
    方法描述：节选出发生在节点之前的所有触点行为序列，用于计算贡献度

    参数说明：
        touchpoint_ids: list
        ）每个用户的触点行为数据
        list1: list
        ）存储目标节点的list，因为目标节点可能不只有一个

    返回值：list touchpoint_ids中位于寻找的目标节点前的所有触点id组成的list
    '''
    for touchpoint_id in touchpoint_ids:
        if touchpoint_id in list1:
            list1 = list1[:list1.index(touchpoint_id)]
    return list1


def preprocessing(df, rm_list, attribute_tp_ids, cur_month_start):
    '''
    方法描述：串联用户的触点行为旅程，并对串联后的触点行为数据进行清洗

    参数说明：
        df: dataframe
        ）存储用户的触点行为数据的 pandas dataframe

    返回值：通过电话号码聚合了行为的dataframe
    '''
    df = rm_unwanted_tp(df, rm_list)
    df['action_time'] = pd.to_datetime(df['action_time'])

    # 去掉在当月之前6个月内发生过目标节点行为的用户
    rm_users = df[(df.touchpoint_id.isin(attribute_tp_ids)) & (df.action_time <= cur_month_start)]
    df = df[df.mobile.isin(rm_users) == False]
    df.mobile = df.mobile.astype(int)

    # 串联用户的触点行为旅程
    df = df.sort_values(['mobile', 'action_time'], ascending=[False, True])
    df_paths = df.groupby('mobile')['touchpoint_id'].aggregate(lambda x: x.tolist()).reset_index()

    ## 产生标签，即判断用户的触点行为旅程中是否含有我们搜寻的目标节点
    df_paths['conversion'] = df_paths['touchpoint_id'].apply(lambda x: if_has_endpoint(attribute_tp_ids, x))

    ## 删除空路径并且只保留目标节点之前的路径
    df_paths.loc[df_paths.conversion == 1, 'touchpoint_id'] = df_paths.loc[df_paths.conversion == 1, 'touchpoint_id'].apply(lambda x: exclude_endpoint(attribute_tp_ids, x))
    df_paths = df_paths[df_paths['touchpoint_id'].str.len() > 0]

    touchpoint_list = df.touchpoint_id.unique()
    return df_paths, touchpoint_list