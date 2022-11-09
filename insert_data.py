from main import *
from worker import *
from datetime import date
from datetime import datetime
"""
select_hourly()
select_daily()
select_weekly()
select_monthly()
"""


print("Choose 1,2,3")
choose=int(input("1 - Insert Sensor, 2 - Insert Data, 3 - Insert data loop "))
if choose==3:
    select_sensor()
    id=return_sensor(str(input("Input sensor name ")),str(input("Input sensor location ")))
    while True:
        humid=random.randrange(20,50)
        temp=random.randrange(-20,20)
        Insert_into_current(humid,temp,id)
        time.sleep(1)
if choose==2:
    select_sensor()
    id=return_sensor(str(input("Input sensor name ")),str(input("Input sensor location ")))
    humid=random.randrange(20,50)
    temp=random.randrange(-20,20)
    Insert_into_current(humid,temp,id)
if choose==1:
    location = input("Input Location ")
    sensor = input("Input sensor name ")
    insert_sensor(sensor,location)
