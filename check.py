import pandas as pd
from dateutil import parser
from config import *
from credentials import *


def cost_basis_not_more_than_purchases(source_path):
    insp = pd.DataFrame.from_csv('incomespend.csv')
    original = pd.DataFrame.from_csv(source_path, sep='\t')
    cost_basis_by_date = insp[insp['category']== 'spend'].groupby(['origin_date'])['cost_basis'].sum()
    purchases_by_date = original[original['Cur..1'] == 'USD'].groupby(['date'])['Sell'].sum()
    assert((cost_basis_by_date > purchases_by_date).any() == False)


def running_balances_nonnegative(source_path):
    original = pd.read_csv(source_path, sep='\t', index_col=None)
    original['SellCur'] = original['Cur..1']
    original['BuyCur'] = original['Cur.']
    # original['FeeCur'] = original['Cur..2']
    # original['date'] = original['Date'].apply(lambda x: parser.parse(x))
    original['date'] = original['Date'].apply(lambda x: datetime.datetime.strptime(x, '%d.%m.%Y %H:%M') )

    balances = {}
    original.sort_values(by='date', inplace=True)
    for i in range(len(original)):
        row = original.iloc[i]
        if row['Type'] == 'Withdrawal' or row['Type'] == 'Deposit': continue
        if row.get('Group','') == 'Margin' and ignore_margins: continue
        if row['date'].year > target_year: continue
        if type(row['Ignore']) != float: continue
        # buy_fee = 0
        # sell_fee = 0
        # if row['FeeCur'] == row['BuyCur']:
        #     buy_fee = row['Fee']
        # if row['FeeCur'] == row['SellCur']:
        #     sell_fee = row['Fee']
        if row['BuyCur'] == target_cur and print_intermediate:
            print('%d %s %s: %f + %s' % (i + 3, row['Date'], target_cur, balances.get(target_cur,0), row['Buy']))
        if row['SellCur'] == target_cur and print_intermediate:
            print('%d %s %s: %f - %s' % (i + 3, row['Date'], target_cur, balances.get(target_cur,0), row['Sell']))

        try:
            balances[row['BuyCur']] += float(row['Buy'])
        except KeyError:
            balances[row['BuyCur']] =+ float(row['Buy'])
        try:
            balances[row['SellCur']] -= float(row['Sell'])
        except:
            balances[row['SellCur']] =- float(row['Sell'])
        if balances[row['SellCur']] < -threshold and row['SellCur'] != 'USD':
            if target_cur == '':
                print('Negative balance %s for %s on date %s row %d' % (balances[row['SellCur']], row['SellCur'], row['date'], i + 3))
            elif row['SellCur'] == target_cur:
                print('Negative balance %s for %s on date %s row %d' % (balances[row['SellCur']], row['SellCur'], row['date'], i + 3))

    if target_cur != '':
        print('%s Final balance: %f' % (target_cur, balances[target_cur]))
    else:
        print('Final balances:')
        for key, value in balances.items():
            if abs(value) > threshold:
                print(key, value)


def running_balances_nonnegative_exchange(source_path):
    original = pd.read_csv(source_path, sep='\t', index_col=None)
    original['SellCur'] = original['Cur..1']
    original['BuyCur'] = original['Cur.']
    # original['FeeCur'] = original['Cur..2']
    # original['date'] = original['Date'].apply(lambda x: parser.parse(x) )
    original['date'] = original['Date'].apply(lambda x: datetime.datetime.strptime(x, '%d.%m.%Y %H:%M') )
    balances = {}
    original.sort_values(by='date', inplace=True)
    for i in range(len(original)):
        row = original.iloc[i]
        if row['Exchange'] != target_exchange:
            continue
        if row.get('Group','') == 'Margin' and ignore_margins: continue
        if row['date'].year > target_year: continue
        if type(row['Ignore']) != float:
            continue

        if row['BuyCur'] == target_cur and print_intermediate:
            print('%d %s %s: %f + %f' % (i + 3, row['Date'], target_cur, balances.get(target_cur,0), float(row['Buy'])))
        if row['SellCur'] == target_cur and print_intermediate:
            print('%d %s %s: %f - %f' % (i + 3, row['Date'], target_cur, balances.get(target_cur,0), float(row['Sell'])))

        if row['Type'] == 'Withdrawal':
            try:
                balances[row['SellCur']] -= float(row['Sell'])
            except KeyError:
                balances[row['SellCur']] =- float(row['Sell'])
        if row['Type'] == 'Deposit':
            try:
                balances[row['BuyCur']] += float(row['Buy'])
            except KeyError:
                balances[row['BuyCur']] =+ float(row['Buy'])
        if row['Type'] == 'Trade':
            try:
                balances[row['BuyCur']] += float(row['Buy'])
            except KeyError:
                balances[row['BuyCur']] =+ float(row['Buy'])
            try:
                balances[row['SellCur']] -= float(row['Sell'])
            except:
                balances[row['SellCur']] =- float(row['Sell'])
        if row['Type'] == 'Income':
            try:
                balances[row['BuyCur']] += float(row['Buy'])
            except KeyError:
                balances[row['BuyCur']] =+ float(row['Buy'])


        if type(row['SellCur']) is str and balances.get(row['SellCur'],0) < -threshold and row['SellCur'] != 'USD':
            if target_cur == '':
                print('Negative balance %s for %s on date %s row %d' % (balances[row['SellCur']], row['SellCur'], row['date'], i + 3))
            elif row['SellCur'] == target_cur:
                print('Negative balance %s for %s on date %s row %d' % (balances[row['SellCur']], row['SellCur'], row['date'], i + 3))

    if target_cur != '':
        print('%s Final balance: %f' % (target_cur, balances[target_cur]))
    else:
        print('Final balances: %s' % balances)


def fork_check():
    # check balances of coins pre fork match those post fork
    pass


def check_cost_basis(source_path):
    names = ['Type', 'BuyAmt', 'BuyCur','SellAmt','SellCur','FeeAmt','FeeCur','Exchange','Group','Comment','Date']
    source = pd.read_csv(source_path, sep='\t', names=names, header=0, parse_dates=['Date'])
    source['Year'] = source['Date'].apply(lambda x: x.year)
    #recent = source[(source['Year'] == target_year - 1) | (source['Year'] == target_year)]
    recent = source[(source['Year'] == target_year)]
    #recent = source
    purchases = recent[recent['SellCur'] == 'USD']
    purchase_total = purchases['SellAmt'].sum()
    sales = recent[recent['BuyCur'] == 'USD']
    sale_total = sales['BuyAmt'].sum()
    print(purchase_total)
    print(sale_total)
    print(sale_total - purchase_total)


def current_holdings_cost_basis(txs_path):
    txs = pd.read_json(txs_path)
    # names = ['id','previous_id','currency','amount','cost_basis','price','timestamp','direction','origin_date','category']
    # insp = pd.read_csv(income_spend_path, sep=',', names=names, header=0, parse_dates=['origin_date', 'timestamp'])
    # assert((insp['origin_date'] <= insp['timestamp']).all())
    data = {
        x: {
            'balance':0,
            'cost_basis':0
        }
        for x in txs['currency'].unique()
    }
    for i in range(len(txs)):
        row = txs.iloc[i]
        if row['direction'] == 'in':
            data[row['currency']]['balance'] += row['amount']
            data[row['currency']]['cost_basis'] += row['cost_basis']
        elif row['direction'] == 'out':
            data[row['currency']]['balance'] -= row['amount']
            data[row['currency']]['cost_basis'] -= row['cost_basis']
        else:
            raise Exception

    return data



target_cur = 'ZET'
target_exchange = ''
print_intermediate = True
threshold = 1e-4
source_path = './input_data/'+initials+'/cointracker/gsheet_custom.tsv'
# txs_path = './derived_data/'+initials+'/transactions-final.json'
running_balances_nonnegative(source_path)
# check_cost_basis(source_path)
#running_balances_nonnegative_exchange()
#current_holdings_cost_basis(txs_path)