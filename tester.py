from migrations import *
from main import *
from worker import *

import os.path

print("checking config.ini")
file_exists = os.path.exists('config.ini')
assert file_exists == True
print(" ---------------- ")


print(" checking config data")
config = ConfigParser()
config.read('config.ini')
mysql_config_mysql_host = config.get('mysql_config', 'mysql_host')
mysql_config_mysql_db = config.get('mysql_config', 'mysql_db')
mysql_config_mysql_user = config.get('mysql_config', 'mysql_user')
mysql_config_mysql_pass = config.get('mysql_config', 'mysql_pass')
assert mysql_config_mysql_host == "localhost"
assert mysql_config_mysql_db == "temp"
assert mysql_config_mysql_user == "root"
assert mysql_config_mysql_pass == "kaka123"
print("OK")

print("Checking if DB migration component log config file exists log_migrate_db.yaml -->")
assert os.path.isfile("log_migrate_db.yaml") == True
print("OK")
print("----------")

print("Checking if  main component log config file exists log_main.yaml -->")
assert os.path.isfile("log_worker.yaml") == True
print("OK")
print("----------")

print("Checking if log destination directory exists -->")
assert os.path.isdir("log") == True
print("OK")
print("----------")

print("Checking if migration source directory exists -->")
assert os.path.isdir("migrations") == True
print("OK")
print("----------")

print("Checking if main migragtion sql file exists")
assert os.path.isfile("migrations/2020053000-initial-db.sql")== True
assert os.path.isfile("migrations/2020053010-initial-db.sql")== True
assert os.path.isfile("migrations/2020053020-initial-db.sql")== True
assert os.path.isfile("migrations/2020053030-initial-db.sql")== True
assert os.path.isfile("migrations/2020053040-initial-db.sql")== True

print("---------")
print("OK")
print("Configuration file test DONE -> ALL OK")
print("----------------------------------------")
print("Checking if json folder exists")
assert os.path.isdir("json")== True
print("OK")
print("------------")
print("Checking if Main .py files exist")
assert os.path.isfile("main.py") == True
assert os.path.isfile("worker.py") == True
assert os.path.isfile("migrations.py") == True


print("OK")
print("Checking connection to DB")
connection = mysql.connector.connect(host=mysql_config_mysql_host, database=mysql_config_mysql_db, user=mysql_config_mysql_user, password=mysql_config_mysql_pass)
assert connection.is_connected() == True

print("OK")
print("----------")
print("Checking all functions")
try:
    sensor=input("Input sensor name")
    location=input("Input location")
    humid=random.randrange(10,50)
    temp=random.randrange(-10,30)
    insert_sensor(sensor,location)
    Insert_into_current(humid,temp,1)
    select_hourly()
    select_daily()
    select_weekly()
    select_monthly()
    select_sensor()
    sql_query="delete from sensor"
    sql_query1="delete from hourly"
    sql_query2="delete from daily"
    sql_query3="delete from weekly"
    sql_query4="delete from monthly"
    sql_query5="delete from currently"
    cursor = connection.cursor()
    cursor.execute(sql_query)
    connection.commit()
    cursor.execute(sql_query1)
    connection.commit()
    cursor.execute(sql_query2)
    connection.commit()
    cursor.execute(sql_query3)
    connection.commit()
    cursor.execute(sql_query4)
    connection.commit()
    cursor.execute(sql_query5)
    connection.commit()
    print("OK")
    print("Rollback complete")
except:
    assert False
print("OK")
print("All OK")