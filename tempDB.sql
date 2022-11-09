create database temp;
use temp;


create table sensor (
	id int not null auto_increment,
    sensor varchar(100) not null,
    location varchar(100) not null,
    primary key(id)
);
create table currently(
	`time` timestamp not null,
    humid int,
    temp float,
    sensor_id int,
    primary key(`time`),
    foreign key(sensor_id) references sensor(id)
);

create table hourly (
	`hour` datetime not null,
    max_temp float,
    min_temp float,
    average_temp float,
    average_humid int,
    min_humid int,
    max_humid int,
    sensor_id int,
    foreign key(sensor_id) references sensor(id),
    primary key(`hour`)
);
create table daily (
	`day` date  unique,
    max_temp float,
    min_temp float,
    average_temp float,
    average_humid int,
    min_humid int,
    max_humid int,
    sensor_id int,
    foreign key(sensor_id) references sensor(id),
    primary key (`day`)
);

create table weekly(
	`week` date not null,
	max_temp float,
    min_temp float,
    average_temp float,
    average_humid int,
    min_humid int,
    max_humid int,
    sensor_id int,
    foreign key(sensor_id) references sensor(id),
    primary key (`week`)
);

create table monthly(
	`month` varchar(7)  unique,
	max_temp float,
    min_temp float ,
    average_temp float ,
    average_humid int ,
    min_humid int ,
    max_humid int,
    sensor_id int,
    foreign key(sensor_id) references sensor(id),
    primary key (`month`)
);

/* trigger auto updates the rest for the tables
*/

delimiter //
create trigger check_data before insert on currently for each row
begin
	if new.humid not between 10 and 100
    then 
		signal sqlstate '45000' set message_text = "not believeable data";
    end if;
    if new.temp not between -30 and 40
    then 
		signal sqlstate '45000' set message_text = "not believeable data";
    end if;
end//

delimiter ;

delimiter //
create trigger update_hourly after insert on currently for each row
		begin
			set @sensor1=(select sensor_id from currently order by `time` DESC limit 1);
			set @sensor2=(select sensor_id from hourly order by `hour` desc limit 1);
			set @times=(select left(`time`,13) from currently where @sensor1=sensor_id order by `time` DESC limit 1);
			set @hours=(select left(`hour`,13) from hourly where @sensor1=sensor_id order by `hour` DESC limit 1);
   			set @mintemp=(select min(temp) from currently where @times=left(`time`,13) and @sensor1=sensor_id);
    		set @maxtemp=(select max(temp) from currently where @times=left(`time`,13) and @sensor1=sensor_id);
			set @averagetemp=(select avg(temp) from currently where @times=left(`time`,13) and @sensor1=sensor_id);
    		set @averagehumid=(select avg(humid) from currently where @times=left(`time`,13) and @sensor1=sensor_id);
    		set @maxhumid=(select max(humid) from currently where @times=left(`time`,13) and @sensor1=sensor_id);
    		set @minhumid=(select min(humid) from currently where @times=left(`time`,13) and @sensor1=sensor_id);
    		if @hours=@times and @sensor1=@sensor2 then
				update hourly
				set 
					max_temp= @maxtemp,
					min_temp=@mintemp,
					average_temp=@averagetemp,
					average_humid=@averagehumid,
					min_humid=@minhumid,
					max_humid=@maxhumid where `hour`=@times and sensor_id=@sensor2;
			else
            INSERT INTO `temp`.`hourly`
				(`hour`,`max_temp`,`min_temp`,`average_temp`,`average_humid`,`min_humid`,`max_humid`,`sensor_id`)
			VALUES
				(@times,@maxtemp, @mintemp, @averagetemp, @averagehumid, @minhumid,@maxhumid,@sensor1);
			end if;
end//


delimiter //
create trigger update_daily after insert on currently for each row
		begin
			set @sensor1=(select sensor_id from currently order by `time` DESC limit 1);
			set @sensor2=(select sensor_id from daily  order by `day` desc limit 1);
			set @times=(select left(`time`,10) from currently where @sensor1=sensor_id order by `time` DESC limit 1);
			set @days=(select left(`day`,10) from daily where @sensor1=sensor_id order by `day` DESC limit 1);
   			set @mintemp=(select min(temp) from currently where @times=left(`time`,10) and @sensor1=sensor_id);
    		set @maxtemp=(select max(temp) from currently where @times=left(`time`,10) and @sensor1=sensor_id);
			set @averagetemp=(select avg(temp) from currently where @times=left(`time`,10) and @sensor1=sensor_id);
    		set @averagehumid=(select avg(humid) from currently where @times=left(`time`,10) and @sensor1=sensor_id);
    		set @maxhumid=(select max(humid) from currently where @times=left(`time`,10) and @sensor1=sensor_id);
    		set @minhumid=(select min(humid) from currently where @times=left(`time`,10) and @sensor1=sensor_id);
    		if @times=@days and @sensor1=@sensor2 then
				update daily
				set 
					max_temp= @maxtemp,
					min_temp=@mintemp,
					average_temp=@averagetemp,
					average_humid=@averagehumid,
					min_humid=@minhumid,
					max_humid=@maxhumid where `day`=@times and sensor_id=@sensor1;
			else
				INSERT INTO `daily`
				VALUES
					(@times, @maxtemp, @mintemp, @averagetemp, @averagehumid, @minhumid,@maxhumid,@sensor1);
			end if;
end//

Delimiter //
create trigger update_weekly after insert on currently for each row
begin
		set @sensor1=(select sensor_id from currently order by `time` DESC limit 1);
		set @sensor2=(select sensor_id from weekly order by `week` desc limit 1);
		set @days=(select left(`time`,10) from currently where @sensor1=sensor_id order by `time` DESC limit 1 );
		set @weeks=(select `week` from weekly where @sensor2=sensor_id order by `week` DESC limit 1);
		set @mintemp=(select min(temp) from currently where left(`time`,10)>= date_add(@days,interval -7 day)and @sensor1=sensor_id);
		set @maxtemp=(select max(temp) from currently where left(`time`,10)>= date_add(@days,interval -7 day)and @sensor1=sensor_id);
		set @averagetemp=(select avg(temp) from currently  where left(`time`,10)>= date_add(@days,interval -7 day)and @sensor1=sensor_id );
		set @averagehumid=(select avg(humid) from currently  where left(`time`,10)>= date_add(@days,interval -7 day)and @sensor1=sensor_id );
		set @maxhumid=(select max(humid) from currently where left(`time`,10)>= date_add(@days,interval -7 day)and @sensor1=sensor_id);
		set @minhumid=(select min(humid) from currently where left(`time`,10)>= date_add(@days,interval -7 day) and @sensor1=sensor_id);
		if datediff(@weeks,@days)<=7  and @sensor1=@sensor2 then
			update weekly
			set 
				max_temp= @maxtemp,
				min_temp=@mintemp,
				average_temp=@averagetemp,
				average_humid=@averagehumid,
				min_humid=@minhumid,
				max_humid=@maxhumid where `week`=@days and @sensor2=sensor_id;
		else
			INSERT INTO `weekly`
			VALUES
				(DATE_ADD(@days,interval 7 day), @maxtemp, @mintemp, @averagetemp, @averagehumid,@minhumid,@maxhumid,@sensor1);
		end if;
end//

delimiter //
create trigger update_monthly after insert on currently for each row
		begin
			set @sensor1=(select sensor_id from currently order by `time` DESC limit 1);
			set @sensor2=(select sensor_id from monthly order by `month` desc limit 1);
			set @days=(select left(`time`,7) from currently where @sensor1=sensor_id order by `time` DESC limit 1 );
			set @months=(select `month` from monthly where @sensor2=sensor_id order by `month` DESC limit 1 );
    		set @mintemp=(select min(temp) from currently where @days=left(`time`,7) );
    		set @maxtemp=(select max(temp) from currently where @days=left(`time`,7));
			set @averagetemp=(select avg(temp) from currently where @days=left(`time`,7) );
    		set @averagehumid=(select avg(humid) from currently where @days=left(`time`,7) );
		    set @maxhumid=(select max(humid) from currently where @days=left(`time`,7));
		    set @minhumid=(select min(humid) from currently where @days=left(`time`,7));
    		if @days=@months and @sensor1=@sensor2 then
				update monthly
				set 
					max_temp= @maxtemp,
					min_temp=@mintemp,
					average_temp=@averagetemp,
					average_humid=@averagehumid,
					min_humid=@minhumid,
					max_humid=@maxhumid where `month`=@days and @sensor2=sensor_id;
			else
				INSERT INTO `monthly`
				VALUES
					(@days, @maxtemp, @mintemp, @averagetemp, @averagehumid, @minhumid,@maxhumid,@sensor1);
			end if;
		end//

delimiter ;
select * from sensor;
select * from currently;
select * from hourly;
select * from daily;
select * from weekly;
select * from monthly;
select average_temp,average_humid from weekly where sensor_id=1;