insert into app_dim_car_series(fir_contact_series,brand) values (?,?)
ON DUPLICATE KEY UPDATE
fir_contact_series=values(fir_contact_series),
brand=values(brand)