from main import *
from worker import *


#CURRENTLY DATA JSON
sql_select_Query = "select * from currently"
cursor = connection.cursor()
cursor.execute(sql_select_Query)
records = cursor.fetchall()

with open("json/currently.json", "w") as f:
    json.dump(records,f , indent=4, default=str)



sql_select_Query = "select * from hourly"
cursor = connection.cursor()
cursor.execute(sql_select_Query)
records = cursor.fetchall()

with open("json/hourly.json", "w") as f:
    json.dump(records,f , indent=4, default=str)



sql_select_Query = "select * from daily"
cursor = connection.cursor()
cursor.execute(sql_select_Query)
records = cursor.fetchall()

with open("json/daily.json", "w") as f:
    json.dump(records,f , indent=4, default=str)




sql_select_Query = "select * from weekly"
cursor = connection.cursor()
cursor.execute(sql_select_Query)
records = cursor.fetchall()

with open("json/weekly.json", "w") as f:
    json.dump(records,f , indent=4, default=str)



sql_select_Query = "select * from monthly"
cursor = connection.cursor()
cursor.execute(sql_select_Query)
records = cursor.fetchall()

with open("json/monthly.json", "w") as f:
    json.dump(records,f , indent=4, default=str)
