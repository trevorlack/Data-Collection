'''
Author_Trevor Lack

This script finds the most recent date in the "psa_hyhg_holdings" database
as a reference point that is passed into the "CALENDAR" module to return
the next BUSINESS DAY.

The next business day function returns a date that is referenced against
the NYSE holiday schedule AS WELL AS Veterans day and Columbus day for
fixed income markets.  Once the next business day is established, it is used as
a reference to find the correct pricing series to insert into the database.

Linking the most recent date in the database with the next business day ensures that
no data series are omitted.

PSA HOLDINGS is sourced from the IDC pricing file sent over by JPM.
'''

import pymysql
from sqlalchemy import create_engine
from MySQL_Authorization import MySQL_Auth
import os
import pandas as pd
import glob
from CompareNewToDB_PlusBBerg import *
from CALENDAR import *

last_day = pull_last_PSA_HYHG_date()
print(last_day)
next_day = get_next_day(last_day)
print(next_day)
access_token = MySQL_Auth()
conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd=access_token, db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' %(access_token))

os.chdir('R:/Fixed Income/+Hedge High Yield/Archive/IDC Pricing')

for file in list(glob.glob("Index Pricing *17.xlsm")):
    tempfile = file
    file_date = tempfile[14:22]
    file_date = datetime.strptime(file_date, "%m %d %y").strftime('%Y-%m-%d')
    if file_date == next_day:
        print(tempfile)
        PSA_hyhg = pd.read_excel(tempfile, sheetname='HYHG')
        PSA_hyhg = PSA_hyhg[['Security Number', 'Shares/Par', 'Price']]
        PSA_hyhg['date'] = file_date
        PSA_hyhg = PSA_hyhg.rename(columns={'Security Number':'CUSIP', 'Shares/Par':'psa_par'})

        PSA_hyhg.to_sql('psa_hyhg_holdings', engine, if_exists='append', index=False)

'''
def compare_holdings(new_holdings):
    prior_holdings = pull_hy_match_set()
    holdings_link = prior_holdings.merge(new_holdings, left_on='prior_CUSIP', right_on=  how='outer', indicator=True)
    index_link = index_link[index_link['_merge'] != 'both']
    adds = index_link[index_link['_merge'] == 'left_only']
    deletes = index_link[index_link['_merge'] == 'right_only']

def pull_hy_match_set():
    db_max_date = pd.DataFrame(pd.read_sql("SELECT CUSIP, As_Of_Date FROM bens_desk.hyhg_index \
                                WHERE As_Of_Date IN (SELECT max(As_Of_Date) FROM bens_desk.hyhg_index)", conn))
    db_cusips = pd.DataFrame(db_max_date.CUSIP)
    db_cusips = db_cusips.rename(columns={'CUSIP':'prior_CUSIP'})
    return db_cusips
'''

