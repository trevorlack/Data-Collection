import pymysql
import pandas as pd
import datetime
from sqlalchemy import create_engine
from Bloomberg_Pandas import BLPInterface
from MySQL_Authorization import MySQL_Auth

'''
sqlalchemy
mysql+pymysql://<username>:<password>@<host>/<dbname>[?<options>]
'''

def pull_hy_match_set():

    access_token = MySQL_Auth()
    conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd=access_token, db='bens_desk')
    engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' % (access_token))

    db_max_date = pd.DataFrame(pd.read_sql("SELECT CUSIP, As_Of_Date FROM bens_desk.hyhg_index \
                                WHERE As_Of_Date IN (SELECT max(As_Of_Date) FROM bens_desk.hyhg_index)", conn))
    db_cusips = pd.DataFrame(db_max_date.CUSIP)
    return db_cusips
    #print(db_cusips)

'''
def update_hy_cusip_change():
    conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd='Porsche911!', db='bens_desk')
    cursor = conn.cursor()

    cursor.execute("""UPDATE bens_desk.hyhg_indx_returns \
     """)

def Cusip_Bank(PSA_ID, old, new):
    for i in changed_cusip:

        if cusip_iter = 'A':
            new_cusip_iter = 'B'
        elif cusip_iter = 'B':
            new_cusip_iter = 'C'
        elif cusip_iter = 'C':
            new_cusip_iter = 'D'
        elif cusip_iter = 'D':
            new_cusip_iter = 'E'

        conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd='Porsche911!', db='bens_desk')
        cursor = conn.cursor()

def add_prior_day_line():

    add_performance_plug.to_sql('hyhg_indx_returns', engine, if_exists='append', index=False)

'''
'''
DATE PARAMETER.  This section below (YYYY,M,D) will only pull cusips relevant after a certain day.
This is not fool proof!!!  If there is a new name added after the day you insert, returns will not be captured on it.


Pull list of cusips to iterate over
db_max_date = pd.DataFrame(pd.read_sql("SELECT CUSIP, As_Of_Date FROM bens_desk.hyhg_indx_return \
WHERE As_Of_Date IN (SELECT max(As_Of_Date) FROM  bens_desk.hyhg_indx_return)", conn, params={dstart}))

db_cusips = pd.DataFrame(db_max_date.CUSIP)
dstart = datetime.date(2017,2,6)

cusiplist = pd.DataFrame(pd.read_sql("SELECT As_Of_Date, CUSIP FROM bens_desk.hyhg_index \
                                WHERE As_Of_Date > %s", conn, params={dstart}))
'''


