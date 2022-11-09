create table hourly (
	`hour` datetime not null,
    temp float,
    humid int,
    sensor_id int not null ,
    foreign key(sensor_id) references sensor(id)
)