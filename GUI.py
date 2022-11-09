from main import *
from worker import *
import tkinter as tk
from tkinter import ttk
config = ConfigParser()
config.read('config.ini')
mysql_config_mysql_host = config.get('mysql_config', 'mysql_host')
mysql_config_mysql_db = config.get('mysql_config', 'mysql_db')
mysql_config_mysql_user = config.get('mysql_config', 'mysql_user')
mysql_config_mysql_pass = config.get('mysql_config', 'mysql_pass')
connection = mysql.connector.connect(host=mysql_config_mysql_host, database=mysql_config_mysql_db, user=mysql_config_mysql_user, password=mysql_config_mysql_pass)


with open('./log_GUIworker.yaml', 'r') as stream:
	log_config = yaml.safe_load(stream)

logging.config.dictConfig(log_config)

	# Creating logger
logger = logging.getLogger('root')

	# Initiating and reading config values
logger.info('Loading configuration from file')
def get_index(event):
        global index
        index=choosen.current()
        print(index)
        return index

def average():
        if index==0:
                humid=random.randrange(20,50)
                temp=random.randrange(-20,20)
                while True:
                        humidint=random.randrange(-1,2)
                        tempint=random.randrange(-1,2)
                        humid= humid + humidint
                        temp=temp + tempint
                        print("Humidity - ",humid)
                        print("Temperature - ",temp)
                        time.sleep(1)
        if index==1:
                x1 = entry1.get()
                x2 = entry2.get()
                sensor_id=return_sensor(x1,x2)
                query="""select average_temp,average_humid from hourly where sensor_id=%s"""
                cursor = connection.cursor()
                cursor.execute(query,(sensor_id,))
                records = cursor.fetchall()
                for row in records:
                        ltemp=ttk.Label(window, text = "Average temperature:", font = ("Times New Roman", 10))
                        lhumid=ttk.Label(window, text = "Average Humidity:", font = ("Times New Roman", 10))
                        aver_temp=ttk.Label(window, text = row[0], font = ("Times New Roman", 10))
                        aver_humid=ttk.Label(window, text = row[1], font = ("Times New Roman", 10))
                        ltemp.grid(column=0, row=4, sticky=tk.W, padx=5, pady=5)
                        lhumid.grid(column=0, row=5, sticky=tk.W, padx=5, pady=5)
                        aver_temp.grid(column=1, row=4, sticky=tk.W, padx=5, pady=5)
                        aver_humid.grid(column=1, row=5, sticky=tk.W, padx=5, pady=5)
                
        if index==2:
                x1 = entry1.get()
                x2 = entry2.get()
                sensor_id=return_sensor(x1,x2)
                query="""select average_temp,average_humid from daily where sensor_id=%s"""
                cursor = connection.cursor()
                cursor.execute(query,(sensor_id,))
                records = cursor.fetchall()
                for row in records:
                        ltemp=ttk.Label(window, text = "Average temperature:", font = ("Times New Roman", 10))
                        lhumid=ttk.Label(window, text = "Average Humidity:", font = ("Times New Roman", 10))
                        aver_temp=ttk.Label(window, text = row[0], font = ("Times New Roman", 10))
                        aver_humid=ttk.Label(window, text = row[1], font = ("Times New Roman", 10))
                        ltemp.grid(column=0, row=4, sticky=tk.W, padx=5, pady=5)
                        lhumid.grid(column=0, row=5, sticky=tk.W, padx=5, pady=5)
                        aver_temp.grid(column=1, row=4, sticky=tk.W, padx=5, pady=5)
                        aver_humid.grid(column=1, row=5, sticky=tk.W, padx=5, pady=5)
        if index==3:
                x1 = entry1.get()
                x2 = entry2.get()
                sensor_id=return_sensor(x1,x2)
                query="""select average_temp,average_humid from weekly where sensor_id=%s"""
                cursor = connection.cursor()
                cursor.execute(query,(sensor_id,))
                records = cursor.fetchall()
                for row in records:
                        ltemp=ttk.Label(window, text = "Average temperature:", font = ("Times New Roman", 10))
                        lhumid=ttk.Label(window, text = "Average Humidity:", font = ("Times New Roman", 10))
                        aver_temp=ttk.Label(window, text = row[0], font = ("Times New Roman", 10))
                        aver_humid=ttk.Label(window, text = row[1], font = ("Times New Roman", 10))
                        ltemp.grid(column=0, row=4, sticky=tk.W, padx=5, pady=5)
                        lhumid.grid(column=0, row=5, sticky=tk.W, padx=5, pady=5)
                        aver_temp.grid(column=1, row=4, sticky=tk.W, padx=5, pady=5)
                        aver_humid.grid(column=1, row=5, sticky=tk.W, padx=5, pady=5)
                
        if index==4:
                x1 = entry1.get()
                x2 = entry2.get()
                sensor_id=return_sensor(x1,x2)
                query="""select average_temp,average_humid from monthly where sensor_id=%s"""
                cursor = connection.cursor()
                cursor.execute(query,(sensor_id,))
                records = cursor.fetchall()
                for row in records:
                        ltemp=ttk.Label(window, text = "Average temperature:", font = ("Times New Roman", 10))
                        lhumid=ttk.Label(window, text = "Average Humidity:", font = ("Times New Roman", 10))
                        aver_temp=ttk.Label(window, text = row[0], font = ("Times New Roman", 10))
                        aver_humid=ttk.Label(window, text = row[1], font = ("Times New Roman", 10))
                        ltemp.grid(column=0, row=4, sticky=tk.W, padx=5, pady=5)
                        lhumid.grid(column=0, row=5, sticky=tk.W, padx=5, pady=5)
                        aver_temp.grid(column=1, row=4, sticky=tk.W, padx=5, pady=5)
                        aver_humid.grid(column=1, row=5, sticky=tk.W, padx=5, pady=5)

window = tk.Tk()
window.geometry("500x200")
window.title('GUI')
entry1 = tk.Entry (window) 
entry2 = tk.Entry (window) 

label2=ttk.Label(window, text = "Select Action:", font = ("Times New Roman", 10))
sensor=ttk.Label(window, text = "Sensor Name:", font = ("Times New Roman", 10))
location=ttk.Label(window, text = "Sensor Location:", font = ("Times New Roman", 10))
data = tk.StringVar()

c= [(0,' Show Current' ),
      ( 1, ' Show Hourly Average'),
     ( 2, ' Show Daily Average'),
     ( 3, ' Show Weekly Average'),
     (  4, ' Show Monthly Average')]
choosen = ttk.Combobox(window, width = 27, textvariable=data,values=[row[1] for row in c],
                            state="readonly")
  
choosen.bind("<<ComboboxSelected>>", get_index)
button1 = tk.Button(window,text='Push', command=average)

choosen.grid(column=1, row=0, sticky=tk.W, padx=5, pady=5)
entry1.grid(column=1, row=1, sticky=tk.W, padx=5, pady=5)
entry2.grid(column=1, row=2, sticky=tk.W, padx=5, pady=5)
button1.grid(column=1, row=3, sticky=tk.W, padx=5, pady=5)
label2.grid(column=0, row=0, sticky=tk.W, padx=5, pady=5)
sensor.grid(column=0, row=1, sticky=tk.W, padx=5, pady=5)
location.grid(column=0, row=2, sticky=tk.W, padx=5, pady=5)

window.mainloop()
# Make a dropdown with all sensor names be able to choose it and run created functions + add user login and registration functions and windows for both console and gui
# when selecting a function (dropdown one) add 2 more dropdowns (one with sensor name other sensor location), 