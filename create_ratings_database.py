import pymysql
import pandas as pd
from sqlalchemy import create_engine
from MySQL_Authorization import MySQL_Auth

'''
sqlalchemy
mysql+pymysql://<username>:<password>@<host>/<dbname>[?<options>]
'''
access_token = MySQL_Auth()
conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd=access_token, db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' %(access_token))


lister = pd.DataFrame(pd.read_csv("Yieldbook matrix.csv"))
#lister.columns=['numerical_code','letter_rating']
print(lister)
lister.to_sql('yieldbook_credit_ratings', engine, if_exists='append', index=False)