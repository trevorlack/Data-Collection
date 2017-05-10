import pandas as pd
import os
import glob
from datetime import datetime
from Bloomberg_Pandas import BLPInterface
import pymysql
from sqlalchemy import create_engine
import numpy as np
import sys
from MySQL_Authorization import MySQL_Auth

access_token = MySQL_Auth()
conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd=access_token, db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' %(access_token))

os.chdir('C:/Users/tlack/Documents/Python Scripts/MySQL Pandas/Loader')
FileList=[]

for file in list(glob.glob("*.csv")):

    hyraw = pd.DataFrame(pd.read_csv(file, header=1))
    hyraw = hyraw.drop(['Unnamed: 0', 'Unnamed: 30'], axis=1)
    hyraw = hyraw.dropna(subset=['ASOFDATE'])
    hyraw['ASOFDATE'] = pd.to_datetime(hyraw['ASOFDATE'])
    filedater = hyraw.loc[hyraw['PAREN']=='US', 'ASOFDATE'].iloc[0]
    #print(filedater)

    hyraw['ASOFDATE'] = filedater
    hyraw = hyraw[hyraw.PAREN !='REPO']
    error = pd.DataFrame(pd.isnull(hyraw).sum(), columns=[filedater])

    if (error > 0).any().any():
        error.index.name = 'data_field'
        fieldlist = pd.Series(hyraw.columns.values.tolist())
        error = error.set_index([fieldlist])
        hyErrorLog = pd.DataFrame(pd.read_csv('C:/Users/tlack/Documents/Python Scripts/MySQL Pandas/ERROR/ErrorLog.csv', index_col=0))
        hyErrorLog = pd.concat([hyErrorLog,error], axis=1)
        hyErrorLog.to_csv('C:/Users/tlack/Documents/Python Scripts/MySQL Pandas/ERROR/ErrorLog.csv')
#        print('Error Log New Updates')

    hyhg_blp_CUSIPs = '/CUSIP/' + hyraw['CUSIP'] + '@TRAC'

    blp = BLPInterface()
    blp_Bloomberg_IDs = pd.DataFrame(blp.referenceRequest(hyhg_blp_CUSIPs, ['ID_CUSIP', 'TRADE_STATUS', 'ID_BB']))      # REG_SERIES_CUSIP    EXCHANGED_BOND_NEW_IDENTIFIER
    blp_Bloomberg_IDs = pd.DataFrame(blp_Bloomberg_IDs.to_records())
    blp_Bloomberg_IDs = blp_Bloomberg_IDs.rename(columns={'Security':'Index_CUSIP'})

    hyhg_CUSIP9 = '/CUSIP/' + blp_Bloomberg_IDs['ID_CUSIP'] + '@TRAC'

    blp_date = datetime.strftime(filedater, '%Y%m%d')
    blp_Trace_Volume = blp.historicalRequest(hyhg_CUSIP9, ['PX_VOLUME'], blp_date, blp_date)        #'PX_VOLUME', 'RSI_14D', 'PX_BID', 'PX_ASK'
    blp_Trace_Volume = pd.DataFrame(blp_Trace_Volume.unstack())    #,columns=['CUSIPs', 'Field', 'Date', 'Field_Data'])
    blp_Trace_Volume = pd.DataFrame(blp_Trace_Volume.to_records())
    blp_Trace_Volume = blp_Trace_Volume.rename(columns={'0': 'Volume'})
    Add_Cusip9 = blp_Trace_Volume['Security'].str[7:16]
    Add_Cusip9 = pd.DataFrame(Add_Cusip9)
    Add_Cusip9.columns = ['ID_CUSIP']
    blp_Trace_Volume = pd.concat([blp_Trace_Volume, Add_Cusip9], axis=1)

    blp_Cusip8 = blp_Bloomberg_IDs['Index_CUSIP'].str[7:15]
    blp_Bloomberg_IDs['Index_CUSIP'] = blp_Cusip8
    hyhg_market_data = pd.merge(blp_Trace_Volume, blp_Bloomberg_IDs, on=['ID_CUSIP'])
    hyhg_market_data = hyhg_market_data.drop(['Field', 'Security'], axis=1)

    hyhg_blp_BMRK = '/CUSIP/' + hyhg_market_data['ID_CUSIP'] + '@BVAL'
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
    blp_BMRK_hyhg = blp_BMRK_hyhg.drop(['Date'], axis =1)

    hyhg_market_data = pd.merge(hyhg_market_data, blp_BMRK_hyhg, on=['ID_CUSIP'], how='left')
    blp_error = pd.DataFrame(pd.isnull(hyhg_market_data).sum(), columns=['Field'])

    hyhg_market_data.to_sql('hyhg_liquidity', engine, if_exists='append', index=False)
    print(file+' Done')
    if (blp_error > 0).any().any():
        hyhg_blanks = hyhg_market_data[pd.isnull(hyhg_market_data).any(axis=1)]
        print(hyhg_blanks)

        '''
        for d in deletes():
            response = input('%s CUSIP is not in newest index file.  Is %s being deleted?  (yes / no) %(d)).lower()

        for i in missing_list():
            response = input('%s CUSIP can not be found in current index definition.  Is this an Addition to the Index?  (yes / no) %(i)).lower()
            if response =='yes':
                add_ref = '/CUSIP/' + i + '@TRAC'
                pdc_for_addition = blp.historicalRequest(add_ref, ['PX_BID'], blp_date, blp_date)
            elif response =='no':
                Ref_check= '/CUSIP/' + i + '@TRAC'
                Ref_CUSIP_bbg = pd.DataFrame(blp.referenceRequest(Ref_check, ['EXCHANGED_BOND_NEW_IDENTIFIER']))      # REG_SERIES_CUSIP    EXCHANGED_BOND_NEW_IDENTIFIER
                Ref_CUSIP_bbg = Ref_CUSIP_bbg.EXCHANGED_BOND_NEW_IDENTIFIER[0]
                print('Bloomberg sees %s as a Reference CUSIP.' %(Ref_CUSIP_bbg)
                response2 = input('Has this name undergone a CUSIP change? (yes/no).lower()
                if response2 =='yes':
                    new_cusip = input('please feed me the new CUSIP for %s' %(unmatched_cusip)).upper()
                elif response2 =='no':
                    print('I have to return some videotapes.  Please correct %s' %(i))
                    print('This process broke down on ' + filedater)
                    sys.exit()

        current_hyhg = pd.DataFrame(hyhg_market_data[['']])
        '''








