__author__ = 'Trevor Lack'
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
engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' %(access_token))


'''DATE PARAMETER.  This section below (YYYY,M,D) will only pull cusips relevant after a certain day.
This is not fool proof!!!  If there is a new name added after the day you insert, returns will not be captured on it.'''
dstart = datetime.date(2017,2,6)

'''Pull list of cusips to iterate over'''
cusiplist = pd.DataFrame(pd.read_sql("SELECT As_Of_Date, CUSIP FROM bens_desk.hyhg_index \
                                WHERE As_Of_Date > %s", conn, params={dstart}))

'''pull yieldbook credit ratings matrix to create letter ratings array'''
rating_matrix = pd.DataFrame(pd.read_sql("SELECT Rating, letter_rating FROM bens_desk.yieldbook_credit_ratings", conn))
print(cusiplist)
for i in range(0, len(cusiplist)-1):

    CUSIPiter = cusiplist['CUSIP'].iloc[i]
    result = pd.read_sql("SELECT As_Of_Date, CUSIP, Price, Ticker, Parent, Rating FROM bens_desk.hyhg_index WHERE CUSIP = %s", conn, params={CUSIPiter})
    pricelist = pd.DataFrame(result[['As_Of_Date','CUSIP','Price','Ticker','Parent','Rating']].sort_values(by='As_Of_Date', na_position='first'))
    daily_return = pd.DataFrame(pricelist.Price.pct_change(1))
    daily_return.columns = ['pct_change']
    daily_return.round(6)
    performance = pd.DataFrame(pricelist.join(daily_return))
    performance = pd.DataFrame(performance.join(rating_matrix.set_index('Rating')['letter_rating'], on='Rating', how='left'))
#    perf = pd.DataFrame(performance.drop_duplicates(['As_Of_Date'],keep='first'))

#    perf.to_sql('hyhg_indx_returns', engine, if_exists='append', index=False)
#    print(pd.isnull(perf).sum())
#    print(performance[performance.duplicated(['As_Of_Date'], keep='last')].groupby(('As_Of_Date')).min())
#        print(perf)
#        perf.to_csv("temp.csv")

    perf.fillna(perf.mean())
'''
These are important checks to evaluate the integrity of the data series.  Duplicates and null values should be captured here.
Since returns are being calculated on the price series, there should be only one null value located
on the first day.  Duplicated dates occur on bond holidays.  The "keep='last' clause will grab the last
instance of the duplicate data.

data.to_sql('data_chunked', engine, chunksize=1000)
'''