#!/usr/bin/env python
# coding: utf-8

# In[33]:


import csv
from datetime import datetime, timedelta
import pyodbc


# In[34]:


sharkfile= r'c:\data\GSAF5.csv'


# In[35]:


conn = pyodbc.connect('DSN=kubricksql;UID=DE14;PWD=password')
cur = conn.cursor()


# In[36]:


attack_dates = []
casenumber= []
isfatal = []
country = []
activity = []
age= []
gender= []
with open(sharkfile) as f:
    reader = csv.DictReader(f)
    for row in reader:
        casenumber.append(row['Case Number'])
        attack_dates.append(row['Date'])
        country.append(row['Country'])
        activity.append(row['Activity'])
        age.append(row['Age'])
        gender.append(row['Sex '])
        isfatal.append(row['Fatal (Y/N)'])
        
        


# In[37]:


data= zip(attack_dates, casenumber, country, activity, age, gender, isfatal)


# In[39]:


cur.execute('truncate table anya.shark')


# In[38]:


import pyodbc


# In[30]:



q= 'insert into anya.shark(attack_date, case_number, country, activity, age, gender, isfatal) values (?,?,?,?,?,?,?)'


# In[32]:


for d in data:
    try:
        cur.execute(q,d)
        conn.commit()
    except:
        conn.rollback()




