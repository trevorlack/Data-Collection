import pandas as pd
import pymysql
from sqlalchemy import create_engine
from MySQL_Authorization import MySQL_Auth

access_token = MySQL_Auth()
conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd=access_token, db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' %(access_token))

outstanding = pd.DataFrame(pd.read_csv('Reuters Outstanding Amount Template.csv'))

outstanding = outstanding.set_index(['As_Of_Date'])
outstanding = outstanding.replace(to_replace='NULL', value=0)
outstanding = outstanding.fillna(value=0)
outstanding = outstanding.astype(float)
print(outstanding)
#outstanding = pd.pivot_table(outstanding, index=['As_Of_Date'])
#print(outstanding)

outstanding = pd.DataFrame(outstanding.stack())
outstanding = pd.DataFrame(outstanding.to_records())
outstanding = outstanding.rename(columns={'level_0': 'CUSIP', '0': 'Outstanding_Amount'})
                           #, columns=['CUSIP', 'Outstanding_Amount'])
outstanding['CUSIP'] = outstanding['CUSIP'].str[0:9]
outstanding['As_Of_Date'] = pd.to_datetime(outstanding['As_Of_Date'])
    #outstanding['As_Of_Date'].dt.strftime('%m-%d-%Y')

print(outstanding)
print(outstanding.columns.values)

outstanding.to_sql('hyhg_outstanding_amounts', engine, if_exists='append', index=False)