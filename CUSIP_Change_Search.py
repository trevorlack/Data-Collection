import pymysql
import pandas as pd
import datetime
from sqlalchemy import create_engine
from MySQL_Authorization import MySQL_Auth

'''
sqlalchemy
mysql+pymysql://<username>:<password>@<host>/<dbname>[?<options>]
'''
access_token = MySQL_Auth()
conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd=access_token, db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' % (access_token))

dstart = datetime.date(2017,2,6)

cusiplist = pd.DataFrame(pd.read_sql("SELECT As_Of_Date, CUSIP FROM bens_desk.hyhg_index \
    WHERE As_Of_Date > %s", conn, params={dstart}))
print(cusiplist)
