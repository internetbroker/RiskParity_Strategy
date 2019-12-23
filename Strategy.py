import pandas as pd
import numpy as np
import backtrader as bt
import pyfolio as pf
import datetime as datetime

from scipy.optimize import minimize
from SSI_Universe import prep_Universe
from Top_liquidity import get_topliquidity

# Universe is the universe of SSI competition with the top average 30 days Liquidity
Universe = get_topliquidity(top=100)

# Initial cash & commission
startcash = 500000000
commission = 0.0025
risk_free_rate = 0

# Period for stats calculation (Covariance, expected returns, etc.)

# Backtest's period
from_date = datetime.datetime(2012,1,1)

# Feed to backtrader
class csv_data(bt.feeds.GenericCSVData):
    params = (
        ('nullvalue', float('NaN')),
        ('dtformat', '%Y-%m-%d'),
        ('datetime', 0),
        ('open', 4),
        ('high', 2),
        ('low', 3),
        ('close', 1),
        ('volume', -1),
        ('openinterest', -1),
    )

cerebro = bt.Cerebro()
for asset in Universe:
    data = csv_data(dataname='C:\\Users\\Win 10\\Desktop\\SSI_DauTruong\\stocks_cleaned\\' + asset +'.csv', fromdate=from_date)
    cerebro.adddata(data, name=asset)

# Calculate portfolio variance
def portfolio_risk(weights, covariances):
    portfolio_risk = np.sqrt((weights * covariances * weights.T))[0,0]
    return portfolio_risk

# Calculate risk contributions
def risk_contribution(weights, covariances):
    portfolio_vol = portfolio_risk(weights, covariances)
    asset_risk_contribution = np.multiply(weights.T, covariances*weights.T) / portfolio_vol
    return asset_risk_contribution


# Define objective function
def obj_func(weights, args):
    # The covariance matrix occupies the first position in the variable
    covariances = args[0]

    # The desired contribution of each asset to the portfolio risk occupies the
    # second position
    assets_risk_budget = args[1]

    # We convert the weights to a matrix
    weights = np.matrix(weights)

    # We calculate the risk of the weights distribution
    portfolio_vol = portfolio_risk(weights, covariances)

    # Calculate risk contribution of each asset
    asset_risk_contribution = risk_contribution(weights, covariances)

    # Risk target
    asset_risk_target = np.asmatrix(np.multiply(portfolio_vol, assets_risk_budget))

    ultility_func = sum(np.square(asset_risk_contribution - asset_risk_target.T))[0, 0]
    return ultility_func

# Constraint on sum of weights equal to one
def weights_cons(x):
    return np.sum(x)-1.0

# Constraint on weights larger than zero
def nonzero_cons(x):
    return x


# Solve risk parity problem to get weights
TOLERANCE = 1e-10


def solver(covariances, asset_risk_budget):
    # Constraints
    cons = ({'type': 'eq', 'fun': weights_cons},
            {'type': 'ineq', 'fun': nonzero_cons})

    # Initial weights: equally weighted
    initial_weights = [1 / covariances.shape[1]] * covariances.shape[1]

    # Solve optimize problem
    solve = minimize(fun=obj_func,
                     x0=initial_weights,
                     args=[covariances, asset_risk_budget],
                     method='SLSQP',
                     constraints=cons,
                     tol=TOLERANCE,
                     options={'disp': False})
    weights = solve.x
    return weights

# Srategy class
class Strategy(bt.Strategy):
    params = (
        ('sma_50', 50),
        ('sma_200', 200)
    )

    def __init__(self):
        self.not_enough_cash = 0
        self.rejected = 0
        self.inds = dict()
        self.counter = 0
        self.day_counter = 0
        self.count_period = 200
        for d in self.datas:
            self.inds[d] = dict()
            self.inds[d]['sma_50'] = bt.indicators.SimpleMovingAverage(
                d.close, period=self.p.sma_50
            )
            self.inds[d]['sma_200'] = bt.indicators.SimpleMovingAverage(
                d.close, period=self.p.sma_200
            )

    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def next(self):
        # if self.counter < self.count_period:
        #     self.counter += 1
        #     print(self.datetime.date(), self.counter)
        # else:
        if self.day_counter % 5 == 0:
            toSellToday = []
            toBuyToday = []
            appended_data = []
            for d in self.datas:
                dt, dn = self.datetime.date(), d._name
                get = lambda mydata: mydata.get(0, self.count_period)
                time = [d.num2date(x) for x in get(d.datetime)]
                df = pd.DataFrame({dn: get(d.close)}, index=time)
                appended_data.append(df)
            df = pd.concat(appended_data, axis=1)

            for d in self.datas:
                dt,dn = self.datetime.date(), d._name
                if d.close[0]> self.inds[d]['sma_50'][0] and self.inds[d]['sma_50'][0] > self.inds[d]['sma_200'][0]:
                    toBuyToday.append(d)
                else:
                    toSellToday.append(d)

            # Asset dataframe to optimize
            toBuyToday = [x._name for x in toBuyToday]
            Optimize_Buy_df = df[toBuyToday]

            # Drop assets have 0.00001 (NaN) values
            for col in Optimize_Buy_df.columns:
                if 0.00001 in df[col].tolist():
                    Optimize_Buy_df.drop(col, axis=1, inplace=True)

            # continue
            if Optimize_Buy_df.empty:
                pass
            else:
                # Calculate input
                daily_returns = Optimize_Buy_df.pct_change(1)
                covariances = daily_returns.cov().values

                # ERC - Risk parity
                asset_risk_budget = [1/covariances.shape[1]]*covariances.shape[1]

                # Solve optimized problem
                weights = solver(covariances, asset_risk_budget)

                # Convert the weights to a pandas Series
                weights = pd.Series(weights, index=Optimize_Buy_df.columns, name='weight')

                for toSell in toSellToday:
                    if(self.broker.getposition(toSell).size != 0):
                        self.order_target_percent(toSell, target=0.0)
                for toBuy in weights.index:
                    print(self.datetime.date(),toBuy, weights[toBuy])
                    self.order_target_percent(toBuy, target=weights[toBuy]*0.8)
        self.day_counter += 1

    def notify_order(self, order):
        if order.status == order.Margin:
            print('Not enough cash')
        if order.status == order.Rejected:
            print('Order is rejected')
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'Stock: %s, BUY EXECUTED, Price: %.2f, size: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.data._name,
                     order.executed.price,
                     order.executed.size,
                     order.executed.value,
                     order.executed.comm))

            else:  # Sell
                self.log(
                    'Stock: %s, SELL EXECUTED, Price: %.2f, size: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.data._name,
                    order.executed.price,
                    order.executed.size,
                    order.executed.value,
                    order.executed.comm))


cerebro.broker.setcash(startcash)
cerebro.broker.setcommission(commission)
cerebro.addanalyzer(bt.analyzers.PyFolio)

cerebro.addstrategy(Strategy)
results = cerebro.run()

portvalue = cerebro.broker.getvalue()
pnl = portvalue - startcash

#Print out the final result
print('Final Portfolio Value: ${}'.format(portvalue))
print('P/L: ${}'.format(pnl))

# Record returns, transactions, positions
returns, positions, transactions, gross_lev = results[0].analyzers.pyfolio.get_pf_items()
returns.to_csv('C:\\Users\\Win 10\\Desktop\\SSI_DauTruong\\Performance\\returns.csv')
positions.to_csv('C:\\Users\\Win 10\\Desktop\\SSI_DauTruong\\Performance\\positions.csv')
transactions.to_csv('C:\\Users\\Win 10\\Desktop\\SSI_DauTruong\\Performance\\transactions.csv')

