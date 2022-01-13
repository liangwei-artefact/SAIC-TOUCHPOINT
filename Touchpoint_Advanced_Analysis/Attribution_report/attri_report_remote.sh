#!/bin/bash
pt=$3
ssh -p 22 mmodel@10.129.170.23 sh /home/mmodel/touchpoint/Attribution_report/attri_report_pipeline.sh $pt

