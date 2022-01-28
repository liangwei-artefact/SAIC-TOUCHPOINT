#!/bin/bash

hive -e "


drop table marketing_modeling.app_other_fir_contact_tp;
 create  TABLE IF NOT EXISTS marketing_modeling.app_other_fir_contact_tp (
fir_contact_fir_sour_brand STRING,
fir_contact_sec_sour_brand STRING,
brand string)
STORED AS ORC
location '/user/hive/warehouse/marketing_modeling.db/app_other_fir_contact_tp';
"
