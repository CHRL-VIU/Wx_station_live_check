#!/home/alex/.local/lib/python3.6/ # specify python installation on server
# -*- coding: utf-8 -*-
# version 1.0.0

# This code reads in the SQL data from the Wx station on the VIU SQL database
# and checks if the live (last 24 hours) data is transmitting correctly for 
# all stations. If not, the code exports a report in CSV format to an email 
# address alerting users that the live data contains issues/errors that require
# checking/fixing.
# Written by J. Bodart

import os
import pandas as pd 
import numpy as np
import glob
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
wx_stations = [x for x in wx_stations if not "legacy_ontree" in x] # remove legacy data for Cairnridgerun
wx_stations_name = list(map(lambda st: str.replace(st, 'clean_', ''), wx_stations)) # remove 'clean_' for csv export
wx_stations_name = [wx_name.capitalize() for wx_name in wx_stations_name] # capitalise station name

# remove previous CSV reports file from yesterday from server and create new one
# matching today's date
for filename in glob.glob("D:/Vancouver_Island_University/Wx_station/daily_report_VIU_hydromet_*"):
    os.remove(filename) 
csv_filename = 'daily_report_VIU_hydromet_' + datetime.datetime.now().strftime("%Y%m%d") + '.csv'

# read SQL data from all 'clean' tables in the VIU SQL database and check for 
# transmission issues, issues with duplicate data, and overall malfunctioning of
# specific sensors based on range of realistic values. Any data that appear
# suspicious is automatically added to a row in the CSV which is then emailed
# to alert users. 
sql_files = [] # initiate dataframes
msg = pd.DataFrame(columns = ['Wx Station', 'Issue']) # initiate dataframes 
for i in range(0,len(wx_stations)):
    
    # import SQL data for each Wx station and sort by date to only keep last 
    # 24 hours of data
    #print ('#### Checking live data for station file: ', wx_stations[i])
    sql_files = pd.read_sql_query(sql="SELECT * FROM " + str(wx_stations[i]) + " ORDER BY DateTime DESC LIMIT 24", con = engine)

    # calculate datetime for now vs latest sql entry
    now_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    datetime_sql = str(sql_files['DateTime'].iloc[0])
    
    # replace None with nans in dataframe
    sql_files = sql_files.fillna(value=np.nan)
    
    # if SQL does not have data for today (i.e. there is a transmission issue),
    # warn!
    # if now_date.split(' ')[0] != datetime_sql.split(' ')[0] or abs(int(now_date.split(' ')[1].split(':')[0]) - int(datetime_sql.split(' ')[1].split(':')[0])) > 3:
    if now_date.split(' ')[0] != datetime_sql.split(' ')[0]:
       print(wx_stations_name[i] + ': satellite data has not been transmitting for more than a day')
       msg.loc[len(msg)] = (wx_stations_name[i], 'Satellite data has not been transmitting for more than a day')

    # calculate if there are any consecutive zeros in data (i.e. sensor faulty)
    diff_df = pd.DataFrame.diff(sql_files.iloc[0:, 2:-1]) # calculate difference (ignore datetime and WaterYr)
    empty_cols = diff_df.iloc[1:,1:-1].columns[(diff_df.iloc[1:,1:-1] == 0).all()] # find which column has consecutive zeros
    empty_cols = (re.sub(r'[^\w\s]', '', str(empty_cols.values))) # replace dtype from object to string
    empty_cols = re.sub("\s+", "; ", empty_cols.strip()) # add comma between column names if any
    if len(empty_cols) > 0: # if there are empty columns in record, warn!
        print(wx_stations_name[i] + ': data is all the same for sensor: ' + empty_cols)
        msg.loc[len(msg)] = (wx_stations_name[i], 'Data is all the same for sensors: ' + empty_cols)
       
    # if battery is below 11.5 volts, warn!
    if any(sql_files['Batt'] < 11.5) == True:
        print(wx_stations_name[i] + ': battery is below 11.5 volts')
        msg.loc[len(msg)] = (wx_stations_name[i], 'Battery is below 11.5 volts')
        
    # if wind direction is > 360 degrees, warn!
    if any(sql_files['Wind_Dir'] > 360) == True:
        print.append(wx_stations_name[i] + ': wind direction sensor is faulty outside of range 0-360')
        msg.loc[len(msg)] = (wx_stations_name[i], 'Wind direction sensor is outside of range 0-360')
    
    # if wind direction is not changing and temperature is above freezing, 
    # warn!
    if (sql_files['Wind_Speed'] == 0).all() and all(sql_files['Air_Temp'] > 2):
        print(wx_stations_name[i] + ': wind speed sensor is faulty (all values are 0 despite above-freezing temp)')
        msg.loc[len(msg)] = (wx_stations_name[i], 'Wind speed sensor is faulty (all values are 0 despite above-freezing temp)')
        
    # if air temperature is outside of normal expected range (-45 to 45
    # celsius), warn!        
    if any(sql_files['Air_Temp'] > 45) or any(sql_files['Air_Temp'] < -45):
        print.append(wx_stations_name[i] + ': air temperature sensor is outside of range + 45 to -45 celsius')
        msg.loc[len(msg)] = (wx_stations_name[i], 'Air temperature sensor is outside of range + 45 to -45 celsius')
        
    # if soil temperature is outside of normal expected range (-10 to 25
    # celsius), warn!        
    if any(sql_files['Soil_Temperature'] > 25) or any(sql_files['Soil_Temperature'] < -10):
        print.append(wx_stations_name[i] + ': soil temperature sensor is outside of range + 25 to -10 celsius')
        msg.loc[len(msg)] = (wx_stations_name[i], 'Soil temperature sensor is outside of range + 25 to -10 celsius')
        
    # if soil moisture is outside of normal expected range (0 to 100), warn!        
    if any(sql_files['Soil_Moisture'] < 0) or any(sql_files['Soil_Moisture'] > 100):
        print.append(wx_stations_name[i] + ': soil moisture sensor is outside of range 0 to 100')
        msg.loc[len(msg)] = (wx_stations_name[i], 'Soil moisture sensor is outside of range 0 to 100')

    # if relative humidity is outside of normal expected range (0 to 100), 
    # warn!        
    if any(sql_files['RH'] < 0) or any(sql_files['RH'] > 100):
        print(wx_stations_name[i] + ': relative humidity sensor is outside of range 0 to 100')
        msg.loc[len(msg)] = (wx_stations_name[i], 'Relative humidity sensor is outside of range 0 to 100')
 
# send email with data
import email_funcs
email_funcs.send_email(csv_filename, msg)

# write current time for sanity check
current_dateTime = datetime.datetime.now()
print("Done at:", current_dateTime, '- refreshing in 24 hours...')