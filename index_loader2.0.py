'''
ASOFDATE, CUSIP, ISIN, PAREN, TCKR, COUP, MATDATE, Rating, GLIC, COBS, PAR, MKV,
AVLF, ACRI, PRICE, B_YTM, BYTW,	MODD, EDUR,	DURW, GSPRD, SPRDWCALL,	OAS, CONVX,
EFFCNVX, PrincRtn, IntRtn, RIRtn, TotalRtn

As_Of_Date, CUSIP, ISIN, PAREN, TCKR, COUP, MATDATE, Rating, GLIC, COBS, PAR, MKV,
AVLF, ACRI, PRICE, B_YTM, BYTW,	MODD, EDUR,	DURW, GSPRD, SPRDWCALL,	OAS, CONVX,
EFFCNVX, PrincRtn, IntRtn, RIRtn, TotalRtn

'''

import pymysql
from sqlalchemy import create_engine

import glob
import os
from dateutil.relativedelta import relativedelta
import xlrd
from MySQL_Authorization import MySQL_Auth
from CompareNewToDB_PlusBBerg import *
from CALENDAR import *
import pandas as pd
import sys

last_day = pull_last_index_date()
next_day = get_next_day(last_day)
#next_day = '2017-05-31'
print(next_day)

access_token = MySQL_Auth()
conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd=access_token, db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' %(access_token))

os.chdir('R:/Fixed Income/+Hedge High Yield/Archive/Index Holdings')

for file in list(glob.glob("HY_Citi_Index*")):
    tempfile = file
    file_date = tempfile[14:20]
    file_date = datetime.strptime(file_date, "%m%d%y").strftime('%Y-%m-%d')
    if file_date == next_day:
        print(tempfile)
        book = xlrd.open_workbook(tempfile)
        sheet = book.sheet_by_name("HY Index Holdings")

        ASOFDATE = sheet.col_values(1, start_rowx=5)
        ASOFDATE = [dt.datetime(*xlrd.xldate_as_tuple(item, 0)) for item in ASOFDATE]
        CUSIP = sheet.col_values(2, start_rowx=5)
        ISIN = sheet.col_values(3, start_rowx=5)
        PAREN = sheet.col_values(4, start_rowx=5)
        TCKR = sheet.col_values(5, start_rowx=5)
        COUP = sheet.col_values(6, start_rowx=5)
        MATDATE = sheet.col_values(7, start_rowx=5)
        MATDATE = [dt.datetime(*xlrd.xldate_as_tuple(item, 0)) for item in MATDATE]
        Rating = sheet.col_values(8, start_rowx=5)
        GLIC = sheet.col_values(9, start_rowx=5)
        COBS = sheet.col_values(10, start_rowx=5)
        PAR = sheet.col_values(11, start_rowx=5)
        MKV = sheet.col_values(12, start_rowx=5)
        AVLF = sheet.col_values(13, start_rowx=5)
        ACRI = sheet.col_values(14, start_rowx=5)
        PRICE = sheet.col_values(15, start_rowx=5)
        B_YTM = sheet.col_values(16, start_rowx=5)
        BYTW = sheet.col_values(17, start_rowx=5)
        MODD = sheet.col_values(18, start_rowx=5)
        EDUR = sheet.col_values(19, start_rowx=5)
        DURW = sheet.col_values(20, start_rowx=5)
        GSPRD = sheet.col_values(21, start_rowx=5)
        GSPRD = [x if x != '*********' else 0 for x in GSPRD]
        SPRDWCALL = sheet.col_values(22, start_rowx=5)
        OAS = sheet.col_values(23, start_rowx=5)
        OAS = [x if x != '*********' else 0 for x in OAS]
        CONVX = sheet.col_values(24, start_rowx=5)
        EFFCNVX = sheet.col_values(25, start_rowx=5)
        PrincRtn = sheet.col_values(26, start_rowx=5)
        IntRtn = sheet.col_values(27, start_rowx=5)
        RIRtn = sheet.col_values(28, start_rowx=5)
        TotalRtn = sheet.col_values(29, start_rowx=5)

        length = len(CUSIP)

        try:
            db = pymysql.connect(
                host='localhost',
                user='tlack',
                passwd=access_token,
                db='bens_desk'
            )

        except Exception as e:
            print('MySQL server is not running')
            sys.exit('cant connect')

        cursor = db.cursor()
        values = "REPLACE INTO hyhg_index (As_Of_Date, CUSIP, ISIN, Parent, Ticker, Coupon, Maturity_Date, Rating, \
                                    GLIC, COBS, PAR, MKV, AVLF, ACRI, Price, B_YTM, BYTW, MODD, EDUR, DURW, GSPRED, \
                                    SPRDWCALL, OAS, CONVX, EFFCNVX, PrincRtn, IntRtn, RIRtn, TotalRtn) \
                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        params = [(str(ASOFDATE[i]), str(CUSIP[i]), str(ISIN[i]), str(PAREN[i]), str(TCKR[i]), str(COUP[i]), str(MATDATE[i]), str(Rating[i]),
                   str(GLIC[i]), str(COBS[i]), str(PAR[i]), str(MKV[i]), str(AVLF[i]), str(ACRI[i]), str(PRICE[i]),
                   str(B_YTM[i]), str(BYTW[i]), str(MODD[i]), str(EDUR[i]), str(DURW[i]), str(GSPRD[i]), str(SPRDWCALL[i]),
                   str(OAS[i]), str(CONVX[i]), str(EFFCNVX[i]), str(PrincRtn[i]), str(IntRtn[i]), str(RIRtn[i]), str(TotalRtn[i])
                   ) for i in
                  range(length)]

        cursor.executemany(values, params)
        db.commit()

        print(file + ' Done')