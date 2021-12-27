#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2021/12/25 1:43 PM
# @Author  : Liangwei Chen
# @FileName: attri_report_load_data.py
# @Software: PyCharm


import sys
import datetime
import numpy as np
import calendar

def get_n_month_start(stamp, n):
    year = stamp.year + (stamp.month + n) / 12
    if ((stamp.month + n) % 12 == 0):
        month = stamp.month
    else:
        month = (stamp.month + n) % 12
    bf_month_end_stamp = datetime.datetime(year=year, month=month, day=1)
    return bf_month_end_stamp


# 获得月初
def get_month_start(stamp):
    cur_month_start_stamp = get_n_month_start(stamp, 0)
    return cur_month_start_stamp


# 获取多个月之后的月末
def get_add_n_month_end(stamp, n):
    year = stamp.year + (stamp.month + n) / 12
    if ((stamp.month + n) % 12 == 0):
        month = stamp.month
    else:
        month = (stamp.month + n) % 12
    a, b = calendar.monthrange(year, month)
    af_month_end_stamp = datetime.datetime(year=year, month=month, day=b)
    return af_month_end_stamp


# 获取月末
def get_month_end(stamp):
    cur_month_end_stamp = get_add_n_month_end(stamp, 0)
    return cur_month_end_stamp
# pt转化
input_pt = '20211228'
pt_stamp = datetime.datetime.strptime(input_pt, '%Y%m%d')
cur_month_start_stamp = pt_stamp.replace(day=1)
cur_month_end_stamp = get_month_end(pt_stamp)

pt = datetime.datetime.strftime(pt_stamp, "%Y%m%d")
pt_month = datetime.datetime.strftime(pt_stamp, "%Y%m")
this_month_start = datetime.datetime.strftime(cur_month_start_stamp, "%Y%m%d")
this_month_end = datetime.datetime.strftime(cur_month_end_stamp, "%Y%m%d")