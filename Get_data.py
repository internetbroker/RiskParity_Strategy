import urllib
from urllib import request
from datetime import date, timedelta
import zipfile
import pandas as pd

today = date.today()
if today.weekday() == 0:
    date = date.today() - timedelta(3)
elif today.weekday() == 6:
    date = date.today() - timedelta(2)
elif today.weekday() == 5:
    date = date.today() - timedelta(1)
else:
    date = today - timedelta(1)

def get_data(date=date):
    link = 'http://images1.cafef.vn/data/'+date.strftime('%Y%m%d') + \
    '/CafeF.SolieuGD.Upto'+date.strftime('%d%m%Y') +'.zip'
    fileName = 'C:\\Users\\Win 10\\Desktop\\SSI_DauTruong\\history.zip'
    req = urllib.request.urlopen(link)
    file = open(fileName, 'wb')
    file.write(req.read())
    file.close()
    exchanges = ['HSX', 'HNX']
    list_ = []
    with zipfile.ZipFile(fileName) as myzip:
        for exchange in exchanges:
            with myzip.open('CafeF.'+exchange+'.Upto'+date.strftime('%d.%m.%Y')+'.csv') as myfile:
                data = pd.read_csv(myfile, header=0, names=['Ticker','Date','Open','High','Low','Close','Volume'])
                list_.append(data)
    frame = pd.concat(list_)
    frame['Date'] = pd.to_datetime(frame.Date, format='%Y%m%d')
    return frame

def pivotData(data):
    pivotedData = data.drop_duplicates(subset =['Date','Ticker'])
    pivotedData = pivotedData.pivot(index='Date',columns = 'Ticker',values ='Close')
    pivotedData.to_csv('pivotedData.csv')

if __name__  == '__main__':
    pivotData(data=get_data())
