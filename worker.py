from email.utils import format_datetime
import re
from urllib import response
from main import *
from datetime import datetime
config = ConfigParser()
config.read('config.ini')
mysql_config_mysql_host = config.get('mysql_config', 'mysql_host')
mysql_config_mysql_db = config.get('mysql_config', 'mysql_db')
mysql_config_mysql_user = config.get('mysql_config', 'mysql_user')
mysql_config_mysql_pass = config.get('mysql_config', 'mysql_pass')
connection = mysql.connector.connect(host=mysql_config_mysql_host, database=mysql_config_mysql_db, user=mysql_config_mysql_user, password=mysql_config_mysql_pass)
# Loading logging configuration
with open('./log_worker.yaml', 'r') as stream:
	log_config = yaml.safe_load(stream)

logging.config.dictConfig(log_config)

	# Creating logger
logger = logging.getLogger('root')

	# Initiating and reading config values
logger.info('Loading configuration from file')
def Insert_into_current(humid,temp,sensor_id):
    try:
        unix_time=time.time()
        date_time = datetime.fromtimestamp(unix_time)
        times=date_time.strftime("%Y-%m-%d %H:%M:%S")
        cursor = connection.cursor()
        mySql_insert_query = """INSERT INTO currently (`time`,humid,temp,sensor_id) 
	                                            VALUES (%s, %s, %s,%s) """       
        record = (times,humid,temp,sensor_id)
        cursor.execute(mySql_insert_query, record)
        connection.commit()
        logger.info(" inserted successfully in current")    

    except mysql.connector.Error as error:
        logger.error("Failed to insert into MySQL table {}".format(error))
def select_hourly():
    try:
        id=return_sensor(str(input("Input sensor name ")),str(input("Input sensor location ")))
        sql_select_Query = "select * from hourly where sensor_id= %s"
        cursor = connection.cursor()
        cursor.execute(sql_select_Query,(id,))
            # get all records
        records = cursor.fetchall()
        print("Total number of rows in table: ", cursor.rowcount)

        print("\nPrinting each row from Hourly")
        for row in records:
            print("Date = ", row[0], )
            print("Max Temperature = ", row[1])
            print("Min Temperature = ", row[2])
            print("Average Temperature = ", row[3])
            print("Average Humidity = ", row[4])
            print("Min Humidity = ", row[5])
            print("Max Humidity  = ", row[6])
            print("Sensor_id  = ", row[7], "\n")

    except mysql.connector.Error as e:
        logger.error("Error using select_hourly", e)
def select_daily():
    try:
        id=return_sensor(str(input("Input sensor name ")),str(input("Input sensor location ")))
        sql_select_Query = "select * from daily where sensor_id=%s"
        cursor = connection.cursor()
        cursor.execute(sql_select_Query,(id,))
            # get all records
        records = cursor.fetchall()
        print("Total number of rows in table: ", cursor.rowcount)

        print("\nPrinting each row from daily")
        for row in records:
            print("Date = ", row[0], )
            print("Max Temperature = ", row[1])
            print("Min Temperature = ", row[2])
            print("Average Temperature = ", row[3])
            print("Average Humidity = ", row[4])
            print("Min Humidity = ", row[5])
            print("Max Humidity  = ", row[6])
            print("Sensor_id  = ", row[7], "\n")

    except mysql.connector.Error as e:
        logger.error("Error using select_daily", e)

def select_weekly():
    try:
        id=return_sensor(str(input("Input sensor name ")),str(input("Input sensor location ")))
        sql_select_Query = "select * from weekly where sensor_id=%s"
        cursor = connection.cursor()
        cursor.execute(sql_select_Query,(id,))
            # get all records
        records = cursor.fetchall()
        print("Total number of rows in table: ", cursor.rowcount)

        print("\nPrinting each row from weekly")
        for row in records:
            print("Date = ", row[0], )
            print("Max Temperature = ", row[1])
            print("Min Temperature = ", row[2])
            print("Average Temperature = ", row[3])
            print("Average Humidity = ", row[4])
            print("Min Humidity = ", row[5])
            print("Max Humidity  = ", row[6])
            print("Sensor_id  = ", row[7], "\n")

    except mysql.connector.Error as e:
        logger.error("Error using select_weekly", e)
def select_monthly():
    try:
        id=return_sensor(str(input("Input sensor name ")),str(input("Input sensor location ")))
        sql_select_Query = "select * from monthly where sensor_id=%s"
        cursor = connection.cursor()
        cursor.execute(sql_select_Query,(id,))
            # get all records
        records = cursor.fetchall()
        print("Total number of rows in table: ", cursor.rowcount)

        print("\nPrinting each row from monthly")
        for row in records:
            print("Date = ", row[0], )
            print("Max Temperature = ", row[1])
            print("Min Temperature = ", row[2])
            print("Average Temperature = ", row[3])
            print("Average Humidity = ", row[4])
            print("Min Humidity = ", row[5])
            print("Max Humidity  = ", row[6])
            print("Sensor_id  = ", row[7], "\n")

    except mysql.connector.Error as e:
        logger.error("Error using select_monthly", e)
def insert_sensor(name,location):
    try:
        cursor = connection.cursor()
        mySql_insert_query = """INSERT INTO sensor (sensor,location) 
	                                            VALUES (%s, %s) """       
        record = (name,location)
        cursor.execute(mySql_insert_query, record)
        connection.commit()
        logger.info(" inserted successfully in sensor")
    except mysql.connector.Error as e:
        logger.error("Error while inserting data from MySQL table", e)
def select_sensor():
    try:
        sql_select_Query = "select * from sensor"
        cursor = connection.cursor()
        cursor.execute(sql_select_Query)
            # get all records
        records = cursor.fetchall()
        print("Total number of rows in table: ", cursor.rowcount)

        print("\nPrinting each row from Sensor")
        for row in records:
            print("id = ", row[0], )
            print("Sensor = ", row[1])
            print("Location  = ", row[2], "\n")
    except mysql.connector.Error as e:
        logger.error("Error using select_sensor", e)
def select_closest_hour(date):
    try:
        id=return_sensor(str(input("Input sensor name ")),str(input("Input sensor location ")))
        Query = "select * from hourly  where sensor_id=%s ORDER BY ABS(timestampdiff(hour, %s ,`hour`)) limit 1;"
        cursor = connection.cursor()
        record=(id,date)
        cursor.execute(Query,record)
               # get all records
        records = cursor.fetchall()
        print("\nPrinting from Hourly")
        for row in records:
            print("Date = ", row[0], )
            print("Max Temperature = ", row[1])
            print("Min Temperature = ", row[2])
            print("Average Temperature = ", row[3])
            print("Average Humidity = ", row[4])
            print("Min Humidity = ", row[5])
            print("Max Humidity  = ", row[6])
            print("Sensor_id  = ", row[7],"/n")
    except mysql.connector.Error as e:
        logger.error("Error using select_closest_hour", e)


def return_sensor(name,location):
    try:
        Query = "select id from sensor where sensor = %s and location= %s limit 1;"
        cursor = connection.cursor()
        record=(name,location)
        cursor.execute(Query,record)
        id = cursor.fetchone()[0]
        return id
    except mysql.connector.Error as e:
        logger.error(e)
        
        