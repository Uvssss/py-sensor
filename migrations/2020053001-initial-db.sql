create table currently(
	`time` timestamp not null,
    humid int,
    temp float,
    sensor_id int,
    foreign key(sensor_id) references sensor(id)
)