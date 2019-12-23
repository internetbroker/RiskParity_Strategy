import pandas as pd
import numpy as np
import zipfile
import tqdm as tqdm

from datetime import date, timedelta


today = date.today()
if today.weekday() == 0:
    date = date.today() - timedelta(3)
elif today.weekday() == 6:
    date = date.today() - timedelta(2)
elif today.weekday() == 5:
    date = date.today() - timedelta(1)
else:
    date = today - timedelta(1)

exchanges = ['HSX', 'HNX']
list_ = []
with zipfile.ZipFile('C:\\Users\\Win 10\\Desktop\\SSI_DauTruong\\history.zip') as myzip:
    for exchange in exchanges:
        with myzip.open('CafeF.' + exchange + '.Upto' + date.strftime('%d.%m.%Y') + '.csv') as myfile:
            data = pd.read_csv(myfile, header=0, names=['Ticker', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
            list_.append(data)
frame = pd.concat(list_)
frame['Date'] = pd.to_datetime(frame.Date, format='%Y%m%d')

stocks = list(frame['Ticker'].unique())
dictdf = {}
dates = frame['Date'].unique()
dates = pd.to_datetime(dates,format='%Y%m%d')
for each in stocks:
    if len(each) < 4:
        thisframe = frame[frame['Ticker'] == each]
        thisframe.index = thisframe.Date
        thisframe.drop_duplicates(inplace=True)
        dictdf[each] = thisframe
        dictdf[each] = dictdf[each].sort_index()
        missingdate = dates.difference(dictdf[each].index)
        dfblank = pd.DataFrame(index=missingdate)
        dfful = pd.concat([dfblank, dictdf[each]], join='outer')
        dfful = dfful.sort_index()
        dfful['Close'].fillna(method='ffill', inplace=True)
        dfful['Volume'].fillna(0,inplace=True)
        dfful['High'] = dfful.apply(lambda row: row['Close'] if np.isnan(row['High']) else row['High'], axis=1)
        dfful['Low'] = dfful.apply(lambda row: row['Close'] if np.isnan(row['Low']) else row['Low'], axis=1)
        dfful['Open'] = dfful.apply(lambda row: row['Close'] if np.isnan(row['Open']) else row['Open'], axis=1)
        dfful['Volume'] = dfful.apply(lambda row: 0 if np.isnan(row['Close']) else row['Volume'], axis=1)
        dfful.index.name = 'Date'
        dfful.drop(['Date'], axis = 1,inplace=True)
        dfful.drop(['Ticker'], axis=1, inplace=True)
        dfful.fillna(0.00001, inplace=True)
        dfful.to_csv(path_or_buf='C:\\Users\\Win 10\\Desktop\\SSI_DauTruong\\stocks_cleaned\\' + each + '.csv')