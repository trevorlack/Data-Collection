from urllib.request import urlopen
import pandas as pd
from datetime import datetime
import pymysql
import sys
from sqlalchemy import create_engine
from MySQL_Authorization import MySQL_Auth

access_token = MySQL_Auth()
conn = pymysql.connect(host='localhost', port=3306, user='tlack', passwd=access_token, db='bens_desk')
engine = create_engine('mysql+pymysql://tlack:%s@localhost/bens_desk' % (access_token))


def ishares_scrape(etf_ticker, url):
    hyg_url = urlopen(url)
    holdings_df = pd.read_csv(hyg_url, header=10, encoding='utf8')

    hyg_url2 = urlopen(url)
    dater = hyg_url2.read().decode('utf-8')
    dater = dater.splitlines(True)
    date_loc = dater[2][21:27] + ' ' + dater[2][29:33]
    holdings_date = datetime.strptime(date_loc, '%b %d %Y').strftime('%Y-%m-%d')

    holdings_df = holdings_df.loc[holdings_df['Asset Class'] == 'Fixed Income']
    holdings_df = holdings_df.loc[(holdings_df['Name'] != 'GCB MPM ESCROW LLC (ESCROW)') &
                                  (holdings_df['Name'] != 'ISHARES IBOXX HIGH YIELD CORP BOND') &
                                  (holdings_df['ISIN'] != '-')]
    holdings_df.dropna(subset=['YTM (%)'], how='any', inplace=True)
    holdings_df['Holdings_Date'] = holdings_date
    holdings_df['Maturity'] = holdings_df['Maturity'].str[0:6] + holdings_df['Maturity'].str[7:]
    holdings_df['Maturity'] = pd.to_datetime(holdings_df['Maturity'], format='%b %d %Y')
    holdings_df['Market Value'] = holdings_df['Market Value'].str.replace(',', '')
    holdings_df['Par Value'] = holdings_df['Par Value'].str.replace(',', '')
    holdings_df['FUND_ID'] = etf_ticker

    Holdings_Date = holdings_df['Holdings_Date'].tolist()
    Maturity = holdings_df['Maturity'].tolist()
    ISIN = holdings_df['ISIN'].tolist()
    Asset_Class = holdings_df['Asset Class'].tolist()
    FUND_ID = holdings_df['FUND_ID'].tolist()

    Weight = holdings_df['Weight (%)'].tolist()
    Weight = [float(x) for x in Weight]
    Weight = [round(x, 3) for x in Weight]
    Weight = [str(x) for x in Weight]

    Price = holdings_df['Price'].tolist()
    Price = [float(x) for x in Price]
    Price = [round(x, 3) for x in Price]
    Price = [str(x) for x in Price]

    Market_Value = holdings_df['Market Value'].tolist()
    Market_Value = [float(x) for x in Market_Value]
    Market_Value = [round(x, 3) for x in Market_Value]
    Market_Value = [str(x) for x in Market_Value]

    Coupon = holdings_df['Coupon (%)'].tolist()
    Coupon = [float(x) for x in Coupon]
    Coupon = [round(x, 3) for x in Coupon]
    Coupon = [str(x) for x in Coupon]

    # holdings_df['YTM (%)'] = holdings_df['YTM (%)'].str.replace('-', '0')
    YTM = holdings_df['YTM (%)'].tolist()
    # YTM = [float(x) for x in YTM]
    # YTM = [round(x, 3) for x in YTM]
    YTM = [str(x) for x in YTM]
    YTM = [w.replace('-', '0') for w in YTM]

    Duration = holdings_df['Duration'].tolist()
    Duration = [float(x) for x in Duration]
    Duration = [round(x, 3) for x in Duration]
    Duration = [str(x) for x in Duration]

    Par = holdings_df['Par Value'].tolist()
    Par = [float(x) for x in Par]
    Par = [round(x, 3) for x in Par]
    Par = [str(x) for x in Par]

    length = len(ISIN)

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

    values = "REPLACE INTO ishares_ig_holdings (Holdings_Date, ISIN, FUND_ID, Asset_Class, Weight, Price, Market_Value, Coupon, Maturity, YTM, Duration, Par) \
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    params = [(str(Holdings_Date[i]), str(ISIN[i]), str(FUND_ID[i]), str(Asset_Class[i]), str(Weight[i]),
               str(Price[i]), str(Market_Value[i]), str(Coupon[i]), str(Maturity[i]),
               str(YTM[i]), str(Duration[i]), str(Par[i])) for i in range(length)]

    cursor.executemany(values, params)
    db.commit()

    print('###HOLDINGS DATE###')
    print(holdings_date)
    print('###FUND TICKER###')
    print(etf_ticker)


ishares_scrape('LQD', 'https://www.ishares.com/us/products/239566/ishares-iboxx-investment-grade-corporate-bond-etf/1467271812596.ajax?fileType=csv&fileName=LQD_holdings&dataType=fund')
ishares_scrape('CSJ', 'https://www.ishares.com/us/products/239451/ishares-13-year-credit-bond-etf/1467271812596.ajax?fileType=csv&fileName=CSJ_holdings&dataType=fund')
ishares_scrape('CIU', 'https://www.ishares.com/us/products/239463/ishares-intermediate-credit-bond-etf/1467271812596.ajax?fileType=csv&fileName=CIU_holdings&dataType=fund')
ishares_scrape('FLOT', 'https://www.ishares.com/us/products/239534/ishares-floating-rate-bond-etf/1467271812596.ajax?fileType=csv&fileName=FLOT_holdings&dataType=fund')
ishares_scrape('CRED', 'https://www.ishares.com/us/products/239460/ishares-credit-bond-etf/1467271812596.ajax?fileType=csv&fileName=CRED_holdings&dataType=fund')
ishares_scrape('CLY', 'https://www.ishares.com/us/products/239423/ishares-10-year-credit-bond-etf/1467271812596.ajax?fileType=csv&fileName=CLY_holdings&dataType=fund')
ishares_scrape('SLQD', 'https://www.ishares.com/us/products/258098/ishares-05-year-investment-grade-corporate-bond-etf/1467271812596.ajax?fileType=csv&fileName=SLQD_holdings&dataType=fund')
