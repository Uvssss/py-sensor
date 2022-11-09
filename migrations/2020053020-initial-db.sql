create table daily (
	`day` date not null ,
    max_temp float,
    min_temp float,
    average_temp float,
    average_humid int,
    min_humid int,
    max_humid int,
    sensor_id int not null,
    foreign key(sensor_id) references sensor(id)
)