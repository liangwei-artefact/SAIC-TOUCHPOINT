insert into app_dim_activity_name(activity_name,brand) values (?,?)
ON DUPLICATE KEY UPDATE
activity_name=values(activity_name),
brand=values(brand)