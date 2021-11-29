#!/usr/bin/env python
# coding: utf-8

'''
Utils packages for touchpoint id processing
Created on Thursday May 27 2021
@author: Xiaofeng XU
'''

def get_tp_mapping(data,tp_id,target_lv):
    '''
    给定一个触点ID，以及想要搜索该触点的上下级别差，即可得到该触点ID的目标级别触点ID
    data(dataframe): 触点体系数据
    tp_id(string): 给定的触点ID
    target_lv(int)：目标搜索等级范围偏差，如1表示搜索给定触点ID的下一级别触点，-1表示搜索给定触点ID的上一级别触点
    '''
    tp_id_lv = int(data[data.touchpoint_id == tp_id].touchpoint_level)
    search_lv = tp_id_lv + target_lv
    try:
        if target_lv < 0:
            id_result =  tp_id[:search_lv * 3] + '0' * (4 - search_lv) * 3  + '_tp'
            if id_result in data.touchpoint_id.tolist():
                return id_result
            else:
                return np.nan        
        else:
            id_result = data[(data.touchpoint_id.str.startswith(tp_id[:tp_id_lv * 3])) &\
                        (data.touchpoint_level == search_lv)].touchpoint_id.tolist()
            if len(id_result) == 0:
                return np.nan
            else:
                return id_result
    except:
        return np.nan
