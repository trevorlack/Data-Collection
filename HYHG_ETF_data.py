import pandas as pd
import pymysql
from sqlalchemy import create_engine
from MySQL_Authorization import MySQL_Auth

access_token = MySQL_Auth()
conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd=access_token, db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' % (access_token))

hyhg = pd.read_csv('C:/Users/tlack/Documents/Python Scripts/hyhgETFupload.csv')
print(hyhg)
hyhg['Date'] = pd.to_datetime(hyhg['Date'])

hyhg.to_sql('hyhg_etf', engine, if_exists='append', index=False)