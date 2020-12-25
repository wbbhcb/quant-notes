import tushare as ts
import pandas as pd
import os
import numpy as np
import time
from tqdm import tqdm
import datetime

"""
获取历史数据
"""

mytoken = 'your tushare token'
ts.set_token(mytoken)
pro = ts.pro_api()


def get_daily_basic(save_path, startdate, enddate, update=False):
    pool = pro.stock_basic(exchange='',
                           list_status='L',
                           fields='ts_code,symbol,name,area,industry,fullname,list_date, market,exchange,is_hs')

    save_path2 = 'DailyBasic'
    if not os.path.exists(os.path.join(save_path, save_path2)):
        os.mkdir(os.path.join(save_path, save_path2))

    pool = pool[pool['market'].isin(['主板', '中小板', '创业板'])].reset_index()

    print('获得上市股票总数：', len(pool)-1)
    jj = 0
    for i in pool.ts_code:
        jj += 1
        # if j <=1789:
        #     continue
        print('正在获取第%d家，股票代码%s.' % (jj, i))

        # 接口限制访问200次/分钟，加一点微小的延时防止被ban
        path = os.path.join(save_path, save_path2, i + '.csv')
        df = pro.query('daily_basic', ts_code=i, start_date=startdate, end_date=enddate,
                       fields='ts_code,trade_date,pe,pe_ttm, pb,ps,ps_ttm,total_mv,circ_mv, dv_ratio, dv_ttm')
        time.sleep(0.3)
        try:
            df = df.sort_values('trade_date', ascending=True)
        except:
            print(i)

        if update:
            if df is None:
                continue
            if len(df) == 0:
                continue
            # if list(df.columns) != col:
            #     raise Exception('列不一致')
            df = df.sort_values('trade_date', ascending=True)
            if not os.path.exists(path):
                df.to_csv(path, index=False)
            else:
                df2 = pd.read_csv(path)
                drop_index = []
                for j in range(len(df)):
                    if int(df['trade_date'][j]) in list(df2['trade_date']):
                        drop_index.append(j)
                        print(str(df['trade_date'][j]) + '已有')

                if len(drop_index) != 0:
                    df.drop(drop_index, axis=0, inplace=True)
                if len(df) == 0:
                    # df2.drop_duplicates('trade_date', inplace=True)
                    # df2.to_csv(path, index=False)
                    continue
                df2 = pd.concat((df2, df))
                df2.to_csv(path, index=False)

        else:
            df.to_csv(path, index=False)


def getIndexData(save_path, startdate, enddate):
    # 上交所指数信息
    df = pro.index_basic(market='SSE')
    df.to_csv(os.path.join(save_path, 'SSE.csv'), index=False, encoding='utf-8')

    # 深交所指数信息
    df = pro.index_basic(market='SZSE')
    df.to_csv(os.path.join(save_path, 'SZSE.csv'), index=False, encoding='utf-8')

    if not os.path.exists(os.path.join(save_path, 'IndexData')):
        os.mkdir(os.path.join(save_path, 'IndexData'))

    # 获取指数历史信息
    # 这里获取几个重要的指数 【上证综指，上证50，上证A指，深证成指，深证300，中小300，创业300，中小板综，创业板综】
    index = ['000001.SH', '000016.SH', '000002.SH', '399001.SZ', '399007.SZ', '399008.SZ', '399012.SZ', '399101.SZ',
             '399102.SZ']
    for i in index:
        path = os.path.join(save_path, 'IndexData', i + '.csv')
        df = pro.index_daily(ts_code=i,
                             start_date=startdate,
                             end_date=enddate,
                             fields='ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, '
                                    'vol, amount')
        df = df.sort_values('trade_date', ascending=True)
        df.to_csv(path, index=False)


if __name__ == '__main__':

    # 设置起始日期
    today = datetime.datetime.now().strftime('%Y-%m-%d').replace('-', '')
    st = '20080101'
    et = today
    save_path = 'F:\dailybasic'

    if not os.path.exists(save_path):
        os.mkdir(save_path)
    get_daily_basic(save_path, st, et)
    # getIndexData(save_path, st, et)