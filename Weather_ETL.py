#!/usr/bin/env python
# coding: utf-8

# In[6]:


import pgeocode
import requests as r
from datetime import datetime
import pandas as pd
import pyodbc
import time


# In[11]:


conn = pyodbc.connect('Driver={SQL SERVER};'
                      'Server=SF-CPU-462;'
                      'Database=Weather;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()


# In[12]:


nomi = pgeocode.Nominatim('in')
zip_code = [380001,365601,388001,392001,364001,370001,389151,382010,389001,361001,362001,384001,387001,396445,385001,384265,360575,360001,395003,363001,390001,396191]
count=0
for i in zip_code:
    geo_data = nomi.query_postal_code(i)
    re = r.get('http://api.openweathermap.org/data/2.5/weather?lat='+str(geo_data.latitude)+'&lon='+str(geo_data.longitude)+'&appid=470c26731dcb1ad567b760d5255e6702')
    weather = re.json()
    cursor.execute('''
                    INSERT INTO gujarat
                    ("City", "Time", "Weather", "Description", "Temperature")
                    VALUES(?,?,?,?,?)
                    ''',
                    geo_data.county_name,
                    datetime.now() ,
                    weather['weather'][0]['main'],
                    weather['weather'][0]['description'],
                    weather['main']['temp']-273.15
                   )
    time.sleep(2)
conn.commit()


# In[ ]:




