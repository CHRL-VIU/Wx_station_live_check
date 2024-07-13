#!/home/alex/.local/lib/python3.6/ # specify python installation on server
# -*- coding: utf-8 -*-
# version 1.0.0

# This code reads in the SQL data from the Wx station on the VIU SQL database
# and checks if the live (last 12 hours) data is transmitting correctly for 
# all stations. If not, the code alerts users via e-mail. Repeats every 12 hours
# Written by J. Bodart
import pandas as pd 
import numpy as np
import re
import datetime

# establish a connection with MySQL database 'viuhydro_wx_data_v2'
# Server log-in details stored in config file
import config
engine = config.main_sql()

# extract name of all tables within SQL database
connection = engine.raw_connection()
cursor = connection.cursor()
cursor.execute("Show tables;")
wx_stations_lst = cursor.fetchall()
wx_stations = []
for i in range(len(wx_stations_lst)):
     lst = (re.sub(r'[^\w\s]', '', str(wx_stations_lst[i])))
     wx_stations.append(lst)
   
# only keep 'clean' tables and sort out the formatting of the name for each
# Wx station
wx_stations = [x for x in wx_stations if "clean" in x ]
stephs = []
steph_num = [1,2,4,7,8,10]
for i in range(len(steph_num)):
    steph = "clean_steph" + str(steph_num[i])
    stephs.append(steph)

# remove all Stephs (1-10, except 3 and 6 as they are connected to satellite)
wx_stations = pd.DataFrame(wx_stations, columns=['Weather_stations'])
wx_stations = wx_stations[~wx_stations['Weather_stations'].isin(stephs)]
wx_stations = wx_stations.reset_index(drop=True)
wx_stations = wx_stations['Weather_stations'].tolist()
    
# remove legacy data for cairnridgerun legacy, main russell and Matchmell as 
# they are not connected to satellite
wx_stations = [x for x in wx_stations if not "legacy_ontree" in x and not "russellmain" in x and not "machmell" in x and not "mountmaya" in x and not "eastbuxton_archive" in x and not "_v2" in x and not "placeglacier" in x]
wx_stations_name = list(map(lambda st: str.replace(st, 'clean_', ''), wx_stations)) # remove 'clean_' for csv export
wx_stations_name = [wx_name.capitalize() for wx_name in wx_stations_name] # capitalise station name

# read SQL data from all 'clean' tables in the VIU SQL database and check for 
# transmission issues. Any issue is automatically added to a row in a dataframe
#  which is then emailed to alert users
sql_files = [] # initiate dataframes
msg = pd.DataFrame(columns = ['Issue']) # initiate dataframes 
for i in range(len(wx_stations)):
    # import SQL data for each Wx station and sort by date to only keep last 
    # 6 entries
    
    print ('#### Checking live data for station file: ', wx_stations[i])
    sql_files = pd.read_sql_query(sql="SELECT * FROM " + str(wx_stations[i]) + " ORDER BY DateTime DESC LIMIT 12", con = engine)
    
    # calculate datetime for last 12 hours (or 10 for Datlamen) vs latest sql entry
    #now_date = (datetime.datetime.now()- datetime.timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S") # for python PST system
    #if 'datlamen' not in wx_stations[i]:
    #    now_date = (datetime.datetime.now()- datetime.timedelta(hours=14)).strftime("%Y-%m-%d %H:%M:%S") # for Linux UTC system
    #else:
    now_date = (datetime.datetime.now()- datetime.timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S") # for Linux UTC system
    now_date = pd.to_datetime(now_date).floor('60min') # floor to round hour
    datetime_sql = str(sql_files['DateTime'].iloc[0])
    
    # replace None with nans in dataframe
    sql_files = sql_files.fillna(value=np.nan)
    
    # if SQL does not have data for last 12 hours (i.e. there is a transmission
    # issue), warn!
    #if str(now_date) >= datetime_sql and 'datlamen' not in wx_stations[i]:
    if str(now_date) >= datetime_sql:
        print('Found issue with transmissions for file:', wx_stations[i])  
        msg.loc[len(msg)] = 'Satellite data has not been transmitting for at least 12 hours for ' + wx_stations_name[i]
    #elif str(now_date) >= datetime_sql and 'datlamen' in wx_stations[i]:
    #    print('Found issue with transmissions for file:', wx_stations[i])  
    #    msg.loc[len(msg)] = 'Satellite data has not been transmitting for at least 10 hours for ' + wx_stations_name[i]
    else:
        print('No missing records for file:', wx_stations[i])  

# send email with report only if there is a transmission issue
while True:
    if msg.empty:
        print('No missing records - rechecking in 6 hours')  
        break
    else:
        print("Found issue with transmissions - sending report to email")
        import email_funcs_transmission
        email_funcs_transmission.send_email(msg)
        break

# write current time for sanity check
current_dateTime = datetime.datetime.now()
print("Done at:", current_dateTime, '- refreshing in 12 hours...')