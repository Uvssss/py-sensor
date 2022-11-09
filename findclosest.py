from main import *
from worker import *
from datetime import date
from datetime import datetime
year=int(input("Input Year "))
month=int(input("Input Month "))
day=int(input("Input Day "))
hour=int(input("Input Hours "))
minute=int(input("Input Minutes "))

a=[year,month,day,hour,minute]
newdate = str(datetime(*map(int, a)))
select_closest_hour(newdate)
