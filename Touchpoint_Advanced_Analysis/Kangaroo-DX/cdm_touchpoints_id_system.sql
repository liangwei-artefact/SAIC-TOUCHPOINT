insert into app_touchpoints_id_system
(
touchpoint_id
,touchpoint_name
,touchpoint_level
,level_1_tp_id
,level_2_tp_id
,level_3_tp_id
,level_4_tp_id
,brand
)
values
(
?
,?
,?
,?
,?
,?
,?
,?
)
ON DUPLICATE KEY UPDATE
touchpoint_id=values(touchpoint_id)
,touchpoint_name=values(touchpoint_name)
,touchpoint_level=values(touchpoint_level)
,level_1_tp_id=values(level_1_tp_id)
,level_2_tp_id=values(level_2_tp_id)
,level_3_tp_id=values(level_3_tp_id)
,level_4_tp_id=values(level_4_tp_id)
,brand=values(brand);

delete from app_touchpoints_id_system
where touchpoint_id=?
  and touchpoint_name=?
  and touchpoint_level=?
  and level_1_tp_id=?
and level_2_tp_id=?
and level_3_tp_id=?
and level_4_tp_id =?
and brand=?