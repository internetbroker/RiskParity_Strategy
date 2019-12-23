import pandas as pd
import numpy as np
import operator

from SSI_Universe import prep_Universe


def get_topliquidity(top):
    Universe = prep_Universe()
    topLiquidity = []
    sortLiquidity = {}
    for asset in Universe:
        data = pd.read_csv('C:\\Users\\Win 10\\Desktop\\SSI_DauTruong\\stocks_cleaned\\' + asset + '.csv')
        volume = data['Volume']
        close = data['Close']
        highest_liquidity = np.multiply(volume, close)
        sortLiquidity[asset] = np.sum(highest_liquidity[-30:])
    sortLiquidity = sorted(sortLiquidity.items(), key=operator.itemgetter(1),reverse=True)
    topLiquidity_list = sortLiquidity[:top]
    topLiquidity_list = [topLiquidity_list[i][0] for i in range(len(topLiquidity_list))]
    return topLiquidity_list

if __name__ == '__main__':
 get_topliquidity()