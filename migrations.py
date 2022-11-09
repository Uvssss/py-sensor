import logging
import logging.config
import mysql.connector
import os
import time
from datetime import datetime
import yaml


from configparser import ConfigParser
from mysql.connector import Error

# Loading logging configuration
with open('./log_migrate_db.yaml', 'r') as stream:
    config = yaml.safe_load(stream)

logging.config.dictConfig(config)

# Creating logger
logger = logging.getLogger('root')

logger.info('DB migration service')

# Initiating and reading config values
logger.info('Loading configuration from file')

try:
	config = ConfigParser()
	config.read('config.ini')

	mysql_config_mysql_host = config.get('mysql_config', 'mysql_host')
	mysql_config_mysql_db = config.get('mysql_config', 'mysql_db')
	mysql_config_mysql_user = config.get('mysql_config', 'mysql_user')
	mysql_config_mysql_pass = config.get('mysql_config', 'mysql_pass')

except:
	logger.exception('')
logger.info('DONE')

connection = None
connected = False

def init_db():
	global connection
	connection = mysql.connector.connect(host=mysql_config_mysql_host, database=mysql_config_mysql_db, user=mysql_config_mysql_user, password=mysql_config_mysql_pass)

init_db()

def get_cursor():
	global connection
	try:
		connection.ping(reconnect=True, attempts=1, delay=0)
		connection.commit()
	except mysql.connector.Error as err:
		logger.error("No connection to db " + str(err))
		connection = init_db()
		connection.commit()
	return connection.cursor()

# Opening connection to mysql DB
logger.info('Connecting to MySQL DB')
try:
	# connection = mysql.connector.connect(host=mysql_config_mysql_host, database=mysql_config_mysql_db, user=mysql_config_mysql_user, password=mysql_config_mysql_pass)
	cursor = get_cursor()
	if connection.is_connected():
		db_Info = connection.get_server_info()
		logger.info('Connected to MySQL database. MySQL Server version on ' + str(db_Info))
		cursor = connection.cursor()
		cursor.execute("select database();")
		record = cursor.fetchone()
		logger.debug('Your connected to - ' + str(record))
		connection.commit()
except Error as e :
	logger.error('Error while connecting to MySQL' + str(e))


# Check if table exists
def mysql_check_if_table_exists(table_name):
	records = []
	cursor = get_cursor()
	try:
		cursor = connection.cursor()
		result  = cursor.execute("SHOW TABLES LIKE '" + str(table_name) + "'")
		records = cursor.fetchall()
		connection.commit()
	except Error as e :
		logger.error("query: " + "SHOW TABLES LIKE '" + str(table_name) + "'")
		logger.error('Problem checking if table exists: ' + str(e))
		pass
	return records

# Create migrations table
def mysql_create_migrations_table():
	cursor = get_cursor()
	result = []
	try:
		cursor = connection.cursor()
		result  = cursor.execute( "CREATE TABLE `migration` ( `id` INT NOT NULL AUTO_INCREMENT, `name` VARCHAR(255), `exec_ts` INT(10), `exec_dt` varchar(20), PRIMARY KEY (`id`))" )
		connection.commit()
	except Error as e :
		logger.error( "CREATE TABLE `migration` ( `id` INT NOT NULL AUTO_INCREMENT, `name` VARCHAR(255), `exec_ts` INT(10), `exec_dt` varchar(20), PRIMARY KEY (`id`))" )
		logger.error('Problem creating migration table in DB: ' + str(e))
		pass
	return result

# Check if table exists
def mysql_check_if_migration_exists(migration_f_name):
	records = []
	cursor = get_cursor()
	try:
		cursor = connection.cursor()
		result  = cursor.execute("SELECT count(*) FROM migration WHERE `name` ='" + str(migration_f_name) + "'")
		records = cursor.fetchall()
		connection.commit()
	except Error as e :
		logger.error("SELECT count(*) FROM migration WHERE `name` ='" + str(migration_f_name) + "'")
		logger.error('Problem checking if migration exists: ' + str(e))
		pass
	return records[0][0]

# Exec any sql on DB
def mysql_exec_any_sql(sql_query):
	cursor = get_cursor()
	status = 0
	try:
		cursor = connection.cursor()
		result  = cursor.execute( sql_query )
		logger.info(result)
		connection.commit()
	except Error as e :
		logger.error( sql_query )
		logger.error('Problem executing sql query on DB: ' + str(e))
		status = 1
		pass
	return status

def update_hourly_trigger():
    try:
        cursor = connection.cursor()
        mySql_insert_query = """ create trigger update_hourly after insert on currently for each row
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
		end"""       
        cursor.execute(mySql_insert_query)
        connection.commit()
        logger.info("Trigger created")

    except mysql.connector.Error as error:
        logger.error("Failed to create trigger {}".format(error))

def update_daily_trigger():
    try:
        cursor = connection.cursor()
        mySql_insert_query = """create trigger update_daily after insert on currently for each row
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
					max_humid=@maxhumid where `day`=@times and sensor_id=@sensor2;
			else
				INSERT INTO `daily`
				VALUES
					(@times, @maxtemp, @mintemp, @averagetemp, @averagehumid, @minhumid,@maxhumid,@sensor1);
			end if;
		end"""       
        cursor.execute(mySql_insert_query)
        connection.commit()
        logger.info("Trigger created")

    except mysql.connector.Error as error:
        logger.error("Failed to create trigger {}".format(error))
def update_weekly_trigger():
    try:
        cursor = connection.cursor()
        mySql_insert_query = """create trigger update_weekly after insert on currently for each row
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
		end"""       
        cursor.execute(mySql_insert_query)
        connection.commit()
        logger.info("Trigger created")

    except mysql.connector.Error as error:
       logger.error("Failed to create trigger {}".format(error))
def update_monthly_trigger():
    try:
        cursor = connection.cursor()
        mySql_insert_query = """create trigger update_monthly after insert on currently for each row
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
		end"""       
        cursor.execute(mySql_insert_query)
        connection.commit()
        logger.info("Trigger created")

    except mysql.connector.Error as error:
        logger.error("Failed to create trigger {}".format(error))
def check_data():
    try:
        cursor = connection.cursor()
        mySql_insert_query = """ create trigger check_data before insert on currently for each row
			begin
				if new.humid not between 10 and 100 then 
					signal sqlstate '45000' set message_text = "not believeable";
    			end if;
    			if new.temp not between -30 and 40 then 
					signal sqlstate '45000' set message_text = "not believeable data";
    			end if;
			end"""       
        cursor.execute(mySql_insert_query)
        connection.commit()
        logger.info("Trigger created")

    except mysql.connector.Error as error:
        logger.error("Failed to create trigger {}".format(error))

# Migration value insert
def mysql_migration_value_insert(name, exec_ts, exec_dt):
	cursor = get_cursor()
	try:
		cursor = connection.cursor()
		result  = cursor.execute( "INSERT INTO `migrations` (`name`, `exec_ts`, `exec_dt`) VALUES ('" + str(name) + "', '" + str(exec_ts) + "', '" + str(exec_dt) + "')")
		connection.commit()
	except Error as e :
		logger.error( "INSERT INTO `migrations` (`name`, `exec_ts`, `exec_dt`) VALUES ('" + str(name) + "', '" + str(exec_ts) + "', '" + str(exec_dt) + "')")
		logger.error('Problem inserting migration values into DB: ' + str(e))
		pass

if mysql_check_if_table_exists("migrations") == []:
	mysql_create_migrations_table()
else:
	logger.info("Migrations table exists")

migrations_list = []
# Reading all migration file names into an array
cur_dir = os. getcwd()
migrations_files_list = os.listdir(cur_dir + "/migrations/")
for f_name in migrations_files_list:
	if f_name.endswith('.sql'):
		migrations_list.append(f_name)

# Sorting list to be processed in the correct order
migrations_list.sort(reverse=False)

counter = 0
for migration in migrations_list:
	if mysql_check_if_migration_exists(migration) == 0:
		with open(cur_dir + "/migrations/" + migration,'r') as file:
			migration_sql = file.read()
			logger.debug(migration_sql)
			logger.info("Executing: " + str(migration))
			if mysql_exec_any_sql(migration_sql) == 0:
				mig_exec_ts = int(time.time())
				mig_exec_dt = datetime.utcfromtimestamp(mig_exec_ts).strftime('%Y-%m-%d %H:%M:%S')
				mysql_migration_value_insert(migration, mig_exec_ts, mig_exec_dt)
				logger.info("OK")
				counter += 1
			else:
				logger.error("Problem applying migration. Aborting")
				break
if counter == 0:
	logger.info("No migrations to execute")	
check_data()
update_daily_trigger()
update_hourly_trigger()
update_weekly_trigger()
update_monthly_trigger()
