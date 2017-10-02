'''
Author_Trevor Lack

This script finds the most recent date in the "hyhg_liquidity" database
and passes it into the "CALENDAR" module to return the next BUSINESS DAY.
Linking the most recent date in the database with the next business day ensures that
no data series are omitted.

The script then finds the corresponding NEXT business day citi index file and uses the
Bloomberg API to pull TRACE Volume data for every name in the index.  The xlsm daily
index files are manually adjusted for CUSIP changes so all cusips listed in the model
should overlap with the xlsm index files.

'''

import glob
import os
from dateutil.relativedelta import relativedelta
import xlrd
from CompareNewToDB_PlusBBerg import *
from CALENDAR import *

last_day = last_HYHG_liquid_date()
print(last_day)
next_day = get_next_day(last_day)
next_day = '2017-05-31'
print(next_day)

access_token = MySQL_Auth()
conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd=access_token, db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' %(access_token))

startingDir = os.getcwd()

maturity_cutoff = date.today() + relativedelta(months=+18)

os.chdir('R:/Fixed Income/+Hedge High Yield/Archive/Index Holdings')

for file in list(glob.glob("HY_Citi_Index*")):
    tempfile = file
    file_date = tempfile[14:20]
    file_date = datetime.strptime(file_date, "%m%d%y").strftime('%Y-%m-%d')
    if file_date == next_day:
        print(tempfile)
        path = str('R:/Fixed Income/+Hedge High Yield/Archive/Index Holdings/' + tempfile)
        book = xlrd.open_workbook(path)
        sheet = book.sheet_by_name("HY Index Holdings")

        as_of_date = sheet.col_values(1, start_rowx=5)
        Cusip_8 = sheet.col_values(2, start_rowx=5)
        par = sheet.col_values(11, start_rowx=5)
        price = sheet.col_values(15, start_rowx=5)
        maturity = sheet.col_values(7, start_rowx=5)
        glic = sheet.col_values(9, start_rowx=5)
        df = pd.DataFrame(data={'date': as_of_date,
                        'Index_CUSIP': Cusip_8,
                        'par': par,
                        'mat_date': maturity,
                        'glic': glic,
                        'price': price})


        df['maturity'] = pd.to_datetime('1899-12-30') + pd.to_timedelta(df.mat_date, 'D')
        df['Date'] = pd.to_datetime('1899-12-30') + pd.to_timedelta(df.date, 'D')
        df = df[df.par > 0]
        df = df.drop(['mat_date', 'date', 'glic', 'par', 'price', 'maturity'], axis=1)

        hyhg_blp_CUSIPs = '/CUSIP/' + df['Index_CUSIP'] + '@TRAC'

        blp = BLPInterface()
        blp_Bloomberg_IDs = pd.DataFrame(blp.referenceRequest(hyhg_blp_CUSIPs, ['ID_CUSIP', 'TRADE_STATUS', 'ID_BB',
                                                                                'BOND_RECOVERY_RATE']))  # REG_SERIES_CUSIP    EXCHANGED_BOND_NEW_IDENTIFIER
        blp_Bloomberg_IDs = pd.DataFrame(blp_Bloomberg_IDs.to_records())
        blp_Bloomberg_IDs = blp_Bloomberg_IDs.rename(columns={'Security': 'Index_CUSIP'})

        hyhg_CUSIP9 = '/CUSIP/' + blp_Bloomberg_IDs['ID_CUSIP'] + '@TRAC'
        blp_date = datetime.strptime(next_day, '%Y-%m-%d').strftime('%Y%m%d')
        blp_Trace_Volume = blp.historicalRequest(hyhg_CUSIP9,
                    ['PX_VOLUME', 'TRACE_NUMBER_OF_TRADES'], blp_date,
                    blp_date)  # 'PX_VOLUME', 'RSI_14D', 'PX_BID', 'PX_ASK'
        '''REMOVE Hierarchical Index Structure'''
        blp_Trace_Volume = pd.DataFrame(blp_Trace_Volume.unstack())
        blp_Trace_Volume = blp_Trace_Volume.reset_index()
        blp_Trace_Volume = blp_Trace_Volume.rename(columns={0: 'Value'})
        blp_Trace_Volume = blp_Trace_Volume[['Security', 'Field', 'Value']]
        blp_Trace_Volume = blp_Trace_Volume.pivot(index='Security', columns='Field')
        blp_Trace_Volume.columns = blp_Trace_Volume.columns.droplevel(0)
        blp_Trace_Volume['Date'] = blp_date
        blp_Trace_Volume = blp_Trace_Volume.reset_index()

        blp_Trace_Volume = blp_Trace_Volume.rename(columns={'PX_VOLUME': 'Volume',
                                                            'TRACE_NUMBER_OF_TRADES': 'TRACE_Trades'})
        Add_Cusip9 = blp_Trace_Volume['Security'].str[7:16]
        Add_Cusip9 = pd.DataFrame(Add_Cusip9)
        Add_Cusip9.columns = ['ID_CUSIP']
        blp_Trace_Volume = pd.concat([blp_Trace_Volume, Add_Cusip9], axis=1)
        blp_Cusip8 = blp_Bloomberg_IDs['Index_CUSIP'].str[7:15]
        blp_Bloomberg_IDs['Index_CUSIP'] = blp_Cusip8
        '''MERGE'''
        hyhg_market_data = pd.merge(blp_Trace_Volume, blp_Bloomberg_IDs, on=['ID_CUSIP'], how='outer')
        hyhg_market_data = hyhg_market_data.drop(['Security'], axis=1)

        hyhg_blp_BMRK = '/CUSIP/' + blp_Bloomberg_IDs['ID_CUSIP'] + '@BVAL'
        blp_BMRK_hyhg = blp.historicalRequest(hyhg_blp_BMRK, ['PX_BID', 'PX_ASK'], blp_date, blp_date)
        blp_BMRK_hyhg = pd.DataFrame(blp_BMRK_hyhg.unstack())  # ,columns=['CUSIPs', 'Field', 'Date', 'Field_Data'])
        blp_BMRK_hyhg = pd.DataFrame(blp_BMRK_hyhg.to_records())
        dataask = blp_BMRK_hyhg[blp_BMRK_hyhg.Field == 'PX_ASK']
        databid = blp_BMRK_hyhg[blp_BMRK_hyhg.Field == 'PX_BID']
        databid = databid.rename(columns={'Field': 'Bid_Code', '0': 'Bid'})
        dataask = dataask.rename(columns={'Field': 'Ask_Code', '0': 'Ask'})
        blp_BMRK_hyhg = dataask.merge(databid, on='Security', how='outer')
        blp_BMRK_hyhg = blp_BMRK_hyhg.drop(['Date_y', 'Ask_Code', 'Bid_Code'], axis=1)
        blp_BMRK_hyhg = blp_BMRK_hyhg.rename(columns={'Date_x': 'Date'})
        blp_BMRK_hyhg['Security'] = blp_BMRK_hyhg['Security'].str[7:16]
        blp_BMRK_hyhg = blp_BMRK_hyhg.rename(columns={'Security': 'ID_CUSIP'})
        blp_BMRK_hyhg = blp_BMRK_hyhg.drop(['Date'], axis=1)

        hyhg_market_data = pd.merge(hyhg_market_data, blp_BMRK_hyhg, on=['ID_CUSIP'], how='outer')
        hyhg_market_data = hyhg_market_data.fillna(value=0)

        hyhg_market_data['Date'] = blp_date
        hyhg_market_data['Date'] = pd.to_datetime(hyhg_market_data['Date'])
        Date = pd.Series(hyhg_market_data['Date'])
        Date = Date.dt.strftime("%Y:%m:%d %H:%M:%S")
        Date = Date.tolist()

        ID_BB = hyhg_market_data['ID_BB'].tolist()
        ID_CUSIP = hyhg_market_data['ID_CUSIP'].tolist()
        Index_CUSIP = hyhg_market_data['Index_CUSIP'].tolist()
        TRADE_STATUS = hyhg_market_data['TRADE_STATUS'].tolist()

        Bid = hyhg_market_data['Bid'].tolist()
        Bid = [float(x) for x in Bid]
        Bid = [round(x, 3) for x in Bid]
        Bid = [str(x) for x in Bid]

        Ask = hyhg_market_data['Ask'].tolist()
        Ask = [float(x) for x in Ask]
        Ask = [round(x, 3) for x in Ask]
        Ask = [str(x) for x in Ask]

        recovery_rate = hyhg_market_data['BOND_RECOVERY_RATE'].tolist()
        recovery_rate = [float(x) for x in recovery_rate]
        recovery_rate = [round(x, 3) for x in recovery_rate]
        recovery_rate = [str(x) for x in recovery_rate]

        TRACE_Trades = hyhg_market_data['TRACE_Trades'].tolist()
        TRACE_Trades = [str(x) for x in TRACE_Trades]

        Volume = hyhg_market_data['Volume'].tolist()
        Volume = [str(x) for x in Volume]

        length = len(ID_CUSIP)

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
        values = "REPLACE INTO hyhg_liquidity (Date, Volume, ID_CUSIP, Index_CUSIP, TRADE_STATUS, ID_BB, Bid, Ask, TRACE_Trades, recovery_rate) \
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        params = [(str(Date[i]), str(Volume[i]), str(ID_CUSIP[i]), str(Index_CUSIP[i]),
                   str(TRADE_STATUS[i]), str(ID_BB[i]), str(Bid[i]),
                   str(Ask[i]), str(TRACE_Trades[i]), str(recovery_rate[i])) for i in range(length)]

        cursor.executemany(values, params)
        db.commit()

        print(file + ' Done')