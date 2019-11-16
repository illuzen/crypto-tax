import pandas as pd
from config import *
import json
from config import logger


def cost_basis_not_more_than_purchases(source_path):
    insp = pd.DataFrame.from_csv('incomespend.csv')
    original = pd.DataFrame.from_csv(source_path, sep='\t')
    cost_basis_by_date = insp[insp['category']== 'spend'].groupby(['origin_date'])['cost_basis'].sum()
    purchases_by_date = original[original['Cur..1'] == 'USD'].groupby(['date'])['Sell'].sum()
    assert((cost_basis_by_date > purchases_by_date).any() == False)


def running_balances_nonnegative(source_path, target_cur='', target_exchange=''):
    original = pd.read_csv(source_path, sep='\t', index_col=None, na_filter='')#, parse_dates=['Date'])
    if 'Cur..1' in original.columns:
        original['SellCur'] = original['Cur..1']
    if 'Cur.' in original.columns:
        original['BuyCur'] = original['Cur.']
    # original['FeeCur'] = original['Cur..2']
    # original['date'] = original['Date'].apply(lambda x: parser.parse(x))
    # original['date'] = original['Date'].apply(lambda x: datetime.datetime.strptime(x, '%d.%m.%Y %H:%M'))
    original['date'] = original['Date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
    original['Date'] = original['date']
    if target_exchange != '':
        original = original[original['Exchange'] == target_exchange]
    balances = {}
    original.sort_values(by='Date', inplace=True)
    for i in range(len(original)):
        row = original.iloc[i]
        d = row.to_dict()
        if (d['Type'] == 'Withdrawal' or d['Type'] == 'Deposit') and target_exchange == '': continue
        if d.get('Group','') == 'Margin' and ignore_margins: continue
        if d['Date'].year > target_year: continue
        # if type(row['Ignore']) != float: continue
        if d['Buy'] == '':
            d['Buy'] = 0
        if d['Sell'] == '':
            d['Sell'] = 0
        # buy_fee = 0
        # sell_fee = 0
        # if row['FeeCur'] == row['BuyCur']:
        #     buy_fee = row['Fee']
        # if row['FeeCur'] == row['SellCur']:
        #     sell_fee = row['Fee']
        if d['BuyCur'] == target_cur and target_cur != '' and print_intermediate:
            print('%d %s %s: %f + %s' % (i + 3, d['Date'], target_cur, balances.get(target_cur,0), row['Buy']))
        if d['SellCur'] == target_cur and target_cur != '' and print_intermediate:
            print('%d %s %s: %f - %s' % (i + 3, d['Date'], target_cur, balances.get(target_cur,0), row['Sell']))

        if d['Type'] == 'Lost' or d['Type'] == 'Stolen':
            try:
                balances[d['SellCur']] -= float(d['Sell'])
            except KeyError:
                balances[d['SellCur']] =- float(d['Sell'])
        if d['Type'] == 'Withdrawal':
            try:
                balances[d['SellCur']] -= float(d['Sell'])
            except KeyError:
                balances[d['SellCur']] =- float(d['Sell'])
        if d['Type'] == 'Deposit':
            try:
                balances[d['BuyCur']] += float(d['Buy'])
            except KeyError:
                balances[d['BuyCur']] =+ float(d['Buy'])
        if d['Type'] == 'Trade':
            try:
                balances[d['BuyCur']] += float(d['Buy'])
            except KeyError:
                balances[d['BuyCur']] =+ float(d['Buy'])
            try:
                balances[d['SellCur']] -= float(d['Sell'])
            except:
                balances[d['SellCur']] =- float(d['Sell'])
        if d['Type'] == 'Income':
            try:
                balances[d['BuyCur']] += float(d.get('Buy',0))
            except KeyError:
                balances[d['BuyCur']] =+ float(d.get('Buy',0))

        if balances.get(d['SellCur'], 0) < -threshold and d['SellCur'] != 'USD':
            if target_cur == '':
                pass
                # print('Negative balance %s for %s on date %s d %d' % (balances[d['SellCur']], d['SellCur'], d['Date'], i + 3))
                print('Negative balance %s for %s on date %s ' % (balances[d['SellCur']], d['SellCur'], d['Date']))
            elif d['SellCur'] == target_cur:
                pass
                # print('Negative balance %s for %s on date %s d %d' % (balances[d['SellCur']], d['SellCur'], d['Date'], i + 3))
                print('Negative balance %s for %s on date %s ' % (balances[d['SellCur']], d['SellCur'], d['Date']))

    if target_cur != '':
        print('%s Final balance: %f' % (target_cur, balances.get(target_cur,0)))
    else:
        print('Final balances:')
        for key, value in balances.items():
            if abs(value) > threshold:
                print(key, value)
    return balances


def running_balances_nonnegative_json(source_path, target_cur='', target_exchange=''):
    original = json.load(open(source_path, 'r'))
    balances = {tx['currency']: 0 for tx in original}
    for tx in original:
        if target_exchange != '' and target_exchange not in tx['notes']: continue
        dt = datetime.datetime.fromtimestamp(tx['timestamp'])
        if dt.year > target_year: continue
        delta = '+' if tx['direction'] == 'in' else '-'
        if tx['currency'] == target_cur and target_cur != '' and print_intermediate:
            print('%d %s %s: %f %s %s' % (tx['index'], dt, target_cur, balances.get(target_cur,0), delta, tx['amount']))

        if tx['direction'] == 'in':
            balances[tx['currency']] += tx['amount']
        if tx['direction'] == 'out':
            balances[tx['currency']] -= tx['amount']

        if balances.get(tx['currency'], 0) < -threshold and tx['currency'] != 'USD':
            if target_cur == '':
                pass
                # print('Negative balance %s for %s on date %s d %d' % (balances[d['SellCur']], d['SellCur'], d['Date'], i + 3))
                print('Negative balance %s for %s on date %s ' % (balances[tx['currency']], tx['currency'], dt))
            elif tx['currency'] == target_cur:
                pass
                # print('Negative balance %s for %s on date %s d %d' % (balances[d['SellCur']], d['SellCur'], d['Date'], i + 3))
                print('Negative balance %s for %s on date %s ' % (balances[tx['currency']], tx['currency'], dt))

    if target_cur != '':
        print('%s Final balance: %f' % (target_cur, balances.get(target_cur,0)))
    else:
        print('Final balances:')
        for key, value in balances.items():
            if abs(value) > threshold:
                print(key, value)
    return balances



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


def final_balance_per_exchange(source_path, target_cur):
    original = pd.read_csv(source_path, sep='\t', index_col=None, na_filter='')
    all_exchanges = original.Exchange.unique()
    balances = {}
    for exchange in all_exchanges:
        print('Exchange: {}'.format(exchange))
        b = running_balances_nonnegative(source_path=source_path, target_exchange=exchange, target_cur=target_cur)
        balances[exchange] = b.get(target_cur,0)
    return balances


def build_traceback_graphs(folder):
    likekind = pd.read_csv('{}/likekind.csv'.format(folder))
    incomespend = pd.read_csv('{}/incomespend.csv'.format(folder))
    more_info = {}
    back_graph = {}
    forward_graph = {}
    for df in [likekind, incomespend]:
        for i, row in df.iterrows():
            if i % 10000 == 0:
                logger.info('Processing row {}'.format(i))
            back_graph[row['id']] = row['previous_id']
            if row['previous_id'] not in forward_graph:
                forward_graph[row['previous_id']] = []
            forward_graph[row['previous_id']].append(row['id'])
            more_info[row['id']] = row

    return forward_graph, back_graph, more_info


def traceback(id, back_graph, more_info):
    current_id = id
    while current_id != -1 and current_id in back_graph and current_id != back_graph[current_id] and back_graph[current_id] != -1:
        logger.info('{} -> {}'.format(current_id, back_graph[current_id]))
        previous_id = back_graph[current_id]
        cost_basis_matches(current_id, previous_id, more_info)
        current_id = previous_id
    return current_id


def cost_basis_matches(current_id, previous_id, more_info, epsilon=1e-4):
    if previous_id == -1:
        return True

    previous_row = more_info[previous_id]
    current_row = more_info[current_id]
    recvd_amt = previous_row.get('received_amount', previous_row.get('amount'))
    rlqd_amt = current_row.get('relinquished_amount', current_row.get('amount'))
    if recvd_amt == 0 or rlqd_amt == 0:
        logger.info('Ignoring 0 amount: {}->{}'.format(previous_id, current_id))
        return True

    expected_cost_basis = (rlqd_amt / recvd_amt) * previous_row['cost_basis']
    actual_cost_basis = current_row['cost_basis']
    if abs(actual_cost_basis - expected_cost_basis) > epsilon:
        logger.warning('Expected cost basis of {}, saw {}'.format(expected_cost_basis, actual_cost_basis))
        return False

    return True


def traceforward_all(forward_graph, more_info):
    remaining_ids = forward_graph[-1]
    i = 0
    while len(remaining_ids) > 0:
        i += 1
        current_id = remaining_ids.pop(0)

        if i % 10000 == 0:
            logger.info('Processing row {} id {}'.format(i, current_id))

        previous_id = more_info[current_id]['previous_id']
        remaining_ids.extend(forward_graph.get(current_id, []))
        cost_basis_matches(current_id, previous_id, more_info)


target_cur = 'BTC'
target_exchange = 'Bittrex'
print_intermediate = True
threshold = 1e-4
b = None
target_id = 82960
# source_path = './data/{}/input_data/cointracker/gsheet.tsv'.format(initials)
source_path = ''
# running_balances_nonnegative(source_path, target_exchange=target_exchange, target_cur=target_cur)
# b = final_balance_per_exchange(source_path, target_cur)
# b = running_balances_nonnegative_json(source_path, target_exchange=target_exchange, target_cur=target_cur)
forward_graph, back_graph, more_info = build_traceback_graphs(source_path)
traceforward_all(forward_graph, more_info)
# b = traceback(target_id, back_graph, more_info)
print(b)
# check_cost_basis(source_path)
#running_balances_nonnegative_exchange()
#current_holdings_cost_basis(txs_path)