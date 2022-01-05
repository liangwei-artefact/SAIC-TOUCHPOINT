insert into app_dim_area(area,brand) values (?,?)
ON DUPLICATE KEY UPDATE
area=values(area),
brand=values(brand)