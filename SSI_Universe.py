import pandas as pd

data = pd.read_csv('C:\\Users\\Win 10\\Desktop\\SSI_DauTruong\\pivotedData.csv')
def get_sticker():
    stickers = pd.read_excel('C:\\Users\\Win 10\\Desktop\\SSI_DauTruong\\DauTruong_Stickers.xlsx')
    Universe_DT = stickers.iloc[:,0].tolist()
    return Universe_DT

# Remove space from the right of stickers
def prep_Universe(Universe_DT=get_sticker()):
    not_stock = []
    for i in range(len(Universe_DT)):
        Universe_DT[i] = Universe_DT[i].rstrip()
        if len(Universe_DT[i]) > 3:
            not_stock.append(Universe_DT[i])

    for i in range(len(not_stock)):
        Universe_DT.remove(not_stock[i])
    return Universe_DT

def data(data=data, Universe = prep_Universe()):
    data = data.set_index('Date')
    data = data[Universe]
    return data

if __name__ == '__main__':
    prep_Universe()
    data()



