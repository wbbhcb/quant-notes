import pandas as pd
import numpy as np
import os
import tushare as ts
import datetime
from tqdm import tqdm

mytoken = 'your ts token'
ts.set_token(mytoken)
pro = ts.pro_api()
codes = ['159902.SZ', '510050.SH', '510500.SH', '510180.SH', '159915.SZ', '159994.SZ', '159995.SZ',
         '510300.SH', '510800.SH', '510900.SH', '512010.SH', '512720.SH', '512760.SH', '512800.SH',
         '512880.SH', '513100.SH', '513500.SH', '515700.SH', '512710.SH', '512580.SH', '159928.SZ',
         '512690.SH',  '513050.SH']
names = ['中小板', '上证50ETF', '中证500ETF', '上证180ETF', '创业板', '5GETF', '芯片ETF1',
         '沪深300ETF', '50ETF基金', 'H股ETF', '医药ETF', '计算机ETF', '芯片ETF2', '银行ETF',
         '证券ETF', '纳指ETF', '标普500ETF', '新能源车ETF', '军工龙头ETF', '环保ETF', '消费ETF', '酒ETF',
         '中概互联ETF']
base_path = os.path.join('E:\\stock', 'ETF')
base_path2 = os.path.join('E:\\stock', 'ETF_adj')
if not os.path.exists(base_path):
    os.mkdir(base_path)
if not os.path.exists(base_path2):
    os.mkdir(base_path2)

def get_data(code, end):
    et = datetime.datetime.now()
    df_list = []
    while True:
        st = et - datetime.timedelta(days=365)
        startdate = str(st.year) + str(st.month).zfill(2) + str(st.day).zfill(2)
        enddate = str(et.year) + str(et.month).zfill(2) + str(et.day).zfill(2)
        df = pro.fund_daily(ts_code=code, start_date=startdate, end_date=enddate)
        if len(df) == 0:
            break
        df_list.append(df)
        et = st - datetime.timedelta(days=1)
        # et = st
        if et < end:
            break
    return pd.concat(df_list).sort_values('trade_date')


def get_adj(code, end):
    et = datetime.datetime.now()
    df_list = []
    while True:
        st = et - datetime.timedelta(days=365)
        startdate = str(st.year) + str(st.month).zfill(2) + str(st.day).zfill(2)
        enddate = str(et.year) + str(et.month).zfill(2) + str(et.day).zfill(2)
        df = pro.fund_adj(ts_code=code, start_date=startdate, end_date=enddate)
        if len(df) == 0:
            break
        df_list.append(df)
        et = st - datetime.timedelta(days=1)
        # et = st
        if et < end:
            break

    return pd.concat(df_list).drop_duplicates(subset=['trade_date']).sort_values('trade_date')


end = datetime.datetime(2012, 1, 1)
df_all = []
ts_codes = []
for i in tqdm(range(len(codes))):
    code = codes[i]
    df = get_data(code, end)
    df2 = get_adj(code, end)
    if len(df) != 0:
        save_path = os.path.join(base_path, names[i] + '.csv')
        save_path2 = os.path.join(base_path2, names[i] + '.csv')
        df.to_csv(save_path, index=None)
        df2.to_csv(save_path2, index=None)
        ts_codes.append(code)
        df_all.append(df)


# 获取指数历史信息
# 这里获取几个重要的指数 【上证综指】
startdate = '20110101'
et = datetime.datetime.now()
enddate = str(et.year) + str(et.month).zfill(2) + str(et.day).zfill(2)
index = ['000001.SH']
for i in index:
    path = i + '.csv'
    df = pro.index_daily(ts_code=i,
                         start_date=startdate,
                         end_date=enddate,
                         fields='ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, '
                                'vol, amount')
    df = df.sort_values('trade_date', ascending=True)
    df.to_csv(path, index=False)

