insert into app_dim_tree_big_small_area(rfs_code,mac_code,rfs_name,mac_name,brand) values (?,?,?,?,?)
ON DUPLICATE KEY UPDATE
rfs_code=values(rfs_code)
,mac_code=values(mac_code)
,rfs_name=values(rfs_name)
,mac_name=values(mac_name)
,brand=values(brand)

