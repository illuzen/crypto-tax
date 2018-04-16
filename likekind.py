

# Big Picture:
# collect csvs
# make list of addresses that I own
# make list of transactions
# sort transactions by date
# ins are income, outs are spends, unless they co-occur, in which case, like kind or self transfer
#

import csv
import glob
import parsers
import datetime
import copy
from tqdm import tqdm
import logging
import json
from pprint import pprint

logging.basicConfig(filename='logs/all.log',level=logging.DEBUG)
derived_folder = './derived_data'
cutoff_year = datetime.datetime(2018,1,1)


# 1 day in seconds
one_day = 24 * 60 * 60
one_minute = 55 * 1000

dollar_epsilon = 100
dollar_pct = .2
#queues = { 'BTC': [], 'ETH': [], 'DASH': [], 'BCH': [] }
# queues = {}
# balances = {}
#
# my_addresses = []

def initialize():
    global queues
    global balances
    global my_addresses
    global likekind_sheet
    global income_spend_sheet
    global income_spend_file
    global likekind_file
    global tx_id

    tx_id = 0
    queues = {}
    balances = {}
    my_addresses = []
    income_spend_file = open('%s/incomespend.csv' % derived_folder, 'w')
    income_spend_sheet = csv.writer(income_spend_file)
    income_spend_sheet.writerow(['id','currency','amount','cost_basis','price','timestamp','direction', 'origin_date', 'category'])
    likekind_file = open('%s/likekind.csv' % derived_folder, 'w')
    likekind_sheet = csv.writer(likekind_file)
    likekind_sheet.writerow(['id','previous_id','received','received_amount','received_price','relinquished','relinquished_amount','relinquished_price','swap_date','last_trade_date','origin_date','cost_basis'])

def sort_txs_by_date(txs):
    sort = sorted(txs, key= lambda x: x['timestamp'])
    for i,tx in enumerate(sort):
        tx['index'] = i
    return sort

def process_exchange_order(txs, i):
    tx1 = txs[i]
    found = False
    for j in range(i+1, len(txs)):
        if txs[j]['notes'] == tx1['notes'] and txs[j]['direction'] != tx1['direction']:
            tx2 = txs[j]
            found = True
            break
    #print(tx1)
    #print(tx2)

    if found:
        logging.info('pairing order %d with %d' % (tx1['index'], tx2['index']))

        out_tx = tx1 if tx1['direction'] == 'out' else tx2
        in_tx = tx1 if tx1['direction'] == 'in' else tx2

        if out_tx['currency'] == 'USD' or in_tx['currency'] == 'USD':
            handle_purchase_sale(out_tx,in_tx)
        else:
            #print('likekind')
            handle_likekind(out_tx, in_tx)

        txs.remove(tx2)
    else:
        raise Exception('no order found to pair with %s' % tx1)

def process_off_exchange(txs, i):
    # look at next in tx to determine if likekind or not
    tx1 = txs[i]
    found = False
    for j in range(i+1, len(txs)):
        tx2 = txs[j]
        close_time = abs(tx2['timestamp'] - tx1['timestamp']) < one_day
        close_dollar = abs(tx2['dollar'] - tx1['dollar']) / max(tx2['dollar'], tx1['dollar']) < dollar_pct
        different_currency = tx2['currency'] != tx1['currency']
        different_direction = tx2['direction'] != tx1['direction']
        not_order = 'order' not in tx2['notes']
        if not close_time:
            break
        if different_direction and not_order and close_dollar:
            found = True
            break

    #print(tx2)
    # if close in time and dollar amount, probably likekind or self transfer
    if found:
        if tx1['currency'] == 'USD' or tx2['currency'] == 'USD':
            handle_purchase_sale(tx1,tx2)
        elif different_currency:
            handle_likekind(tx1, tx2)
        else:
            handle_self_transfer(tx1, tx2)
        txs.remove(tx2)
    else:
        # print('txs %d and %d not paired' % (tx1['index'], tx2['index']))
        # print('close_time %s' % close_time)
        # print('close_dollar %s' % close_dollar)
        if tx1['currency'] != 'USD':
            handle_single(tx1)

def process_txs(data):
    txs = copy.deepcopy(sort_txs_by_date(data))
    i = 0
    while i < len(txs):
        tx1 = txs[i]
        #print(tx1)
        tx2 = {}
        if tx1['timestamp'] > cutoff_year.timestamp():
            logging.info('reached tx past cutoff year %s. Stopping' % cutoff_year)
            break

        # handle exchange orders
        if 'order' in tx1['notes']:
            process_exchange_order(txs, i)
        else:
            process_off_exchange(txs, i)
        i += 1

    income_spend_file.close()
    likekind_file.close()
    print(balances)

def handle_purchase_sale(tx1, tx2):
    incoming = tx1 if tx1['direction'] == 'in' else tx2
    outgoing = tx2 if tx2['direction'] == 'out' else tx1
    if incoming['currency'] == 'USD':
        handle_spend(outgoing)
    elif outgoing['currency'] == 'USD':
        handle_purchase(tx1,tx2)

def handle_single(tx):
    if tx['direction'] == 'in':
        handle_income(tx)
    elif tx['direction'] == 'out':
        handle_spend(tx)
    else:
        raise Exception('Unknown direction type %s' % tx)

# we purchased or received crypto
def handle_purchase(tx1,tx2):
    usd = tx1 if tx1['currency'] == 'USD' else tx2
    crypto = tx2 if tx1['currency'] == 'USD' else tx1
    logging.info('treating txs %d and %d as purchase' % (tx1['index'], tx2['index']))
    maybe_print('treating txs %d and %d as purchase' % (tx1['index'], tx2['index']))
    crypto['category'] = 'purchase'
    crypto['paired'] = usd['index']
    usd['category'] = 'purchase'
    usd['paired'] = crypto['paired']
    crypto['cost_basis'] = crypto['dollar']
    crypto['origin_date'] = crypto['timestamp']
    crypto['id'] = get_new_tx_id()
    q_in = get_queue_for_currency(crypto['currency'])
    q_in.insert(0, crypto)
    update_balance(crypto['currency'], crypto['amount'])
    #write_income_spend(crypto)

# we purchased or received crypto
def handle_income(income):
    logging.info('Handling income tx %s' % income)
    maybe_print('treating tx %d as income' % income['index'])
    income['category'] = 'income'
    income['cost_basis'] = income['dollar']
    income['origin_date'] = income['timestamp']
    income['id'] = get_new_tx_id()
    q_in = get_queue_for_currency(income['currency'])
    q_in.insert(0, income)
    update_balance(income['currency'], income['amount'])
    write_income_spend(income)

def handle_spend(spend):
    maybe_print('treating tx %d as spend' % spend['index'])
    price = spend['price']
    spend_amount_left = spend['amount']
    timestamp = spend['timestamp']


    currency = spend['currency']
#    if 'cost_basis' not in spend:
#        spend['cost_basis'] = spend['dollar']
    q_out = get_queue_for_currency(currency)
    update_balance(currency, -1 * spend['amount'])

    if len(q_out) == 0:
        pprint(spend)
        pprint(balances)

    while spend_amount_left > 0:
        if len(q_out) == 0:
            msg = "Empty queue for currency %s" % out_currency
            print(msg)
            logging.warn(msg)
            pprint(spend)
            pprint(income)
            pprint(balances)
        tx_out = q_out[-1]
        if spend_amount_left >= tx_out['amount']:
            spend_piece_amount = tx_out['amount']
            cost_basis = tx_out['cost_basis']
            q_out.pop()
        else:
            spend_piece_amount = spend_amount_left
            cost_basis = tx_out['cost_basis'] * spend_piece_amount / tx_out['amount']
            tx_out['cost_basis'] -= cost_basis
            tx_out['amount'] -= spend_piece_amount

        spend_amount_left -= spend_piece_amount
        if spend_amount_left < 1e-8:
            spend_amount_left = 0

        income_spend = {
            'id':tx_out['id'],
            'currency':currency,
            'category':'spend',
            'amount': spend_piece_amount,
            'cost_basis': cost_basis,
            'price':price,
            'timestamp': timestamp,
            'direction':'out',
            'origin_date': tx_out['origin_date']
        }
        write_income_spend(income_spend)


def handle_self_transfer(spend,income):
    maybe_print('pairing self transfer: %d with %d' % (spend['index'], income['index']))
    spend['category'] = 'self transfer'
    spend['paired'] = income['index']
    income['category'] = 'self transfer'
    income['paired'] = spend['index']

def handle_likekind(tx1, tx2):
    maybe_print('pairing likekind txs %d and %d' % (tx1['index'], tx2['index']))
    spend = tx1 if tx1['direction'] == 'out' else tx2
    income = tx1 if tx1['direction'] == 'in' else tx2
    out_currency = spend['currency']
    in_currency = income['currency']
    q_in = get_queue_for_currency(in_currency)
    q_out = get_queue_for_currency(out_currency)
    update_balance(out_currency, -1 * spend['amount'])
    update_balance(in_currency, income['amount'])
    out_amount = spend['amount']
    in_amount = income['amount']
    out_price = spend['price']
    in_price = income['price']
    spend_amount_left = out_amount
    income_amount_left = in_amount

    # deplete items from the out queue until spend is accounted for
    while spend_amount_left > 0:
        if len(q_out) == 0:
            msg = "Empty queue for currency %s" % out_currency
            print(msg)
            logging.warn(msg)
            pprint(spend)
            pprint(income)
            pprint(balances)
        tx_out = q_out[-1]
        if spend_amount_left > tx_out['amount']:
            spend_piece_amount = tx_out['amount']
            income_piece_amount = in_amount * spend_piece_amount / out_amount
            cost_basis = tx_out['cost_basis']
            q_out.pop()
            final = False
        elif spend_amount_left == tx_out['amount']:
            spend_piece_amount = spend_amount_left
            income_piece_amount = income_amount_left
            cost_basis = tx_out['cost_basis']
            q_out.pop()
            final = True
        else:
            spend_piece_amount = spend_amount_left
            income_piece_amount = income_amount_left
            cost_basis = tx_out['cost_basis'] * spend_piece_amount / tx_out['amount']
            tx_out['amount'] -= spend_piece_amount
            final = True

        #income_piece_amount = round(income_piece_amount, 8)
        #spend_piece_amount = round(spend_piece_amount, 8)
        income_amount_left -= income_piece_amount
        spend_amount_left -= spend_piece_amount
        if spend_amount_left < 1e-8:
            spend_amount_left = 0

        # if out_currency == 'FCT' or in_currency == 'FCT':
        #     print('final %s' % final)
        #     print('tx_out[amount] %s' % tx_out['amount'])
        #     print('spend_amount_left %s' % spend_amount_left)
        #     print('income_amount_left %s' % income_amount_left)
        #     print('spend_piece_amount %s' % spend_piece_amount)
        #     print('income_piece_amount %s' % income_piece_amount)

        income_piece = copy.deepcopy(income)
        income_piece['id'] = get_new_tx_id()
        income_piece['amount'] = income_piece_amount
        income_piece['dollar'] = income_piece['amount'] * income_piece['price']
        income_piece['origin_date'] = tx_out['origin_date']
        income_piece['cost_basis'] = cost_basis
        q_in.insert(0, income_piece)

        likekind = {
            'id': income_piece['id'],
            'previous_id': tx_out['id'],
            'received':in_currency,
            'received_amount':income_piece_amount,
            'received_price':in_price,
            'relinquished': out_currency,
            'relinquished_amount': spend_piece_amount,
            'relinquished_price': out_price,
            'swap_date': income_piece['timestamp'],
            'last_trade_date': spend['timestamp'],
            'origin_date': income_piece['origin_date'],
            'cost_basis': income_piece['cost_basis'],
        }
        write_likekind(likekind)

def get_queue_for_currency(currency):
    try:
        return queues[currency]
    except KeyError:
        queues[currency] = []
        return queues[currency]

def update_balance(currency, amount):
    try:
        balances[currency] += amount
    except KeyError:
        balances[currency] = amount

    balances[currency] = round(balances[currency], 8)

    if balances[currency] < 0:
        logging.warn('Negative balance %s' % currency)


def get_new_tx_id():
    global tx_id
    tx_id += 1
    return tx_id

def write_income_spend(income_spend):
    time_format = '%Y/%m/%d'
    row = [
        income_spend['id'],
        income_spend['currency'],
        income_spend['amount'],
        income_spend['cost_basis'],
        income_spend['price'],
        datetime.datetime.fromtimestamp(income_spend['timestamp']).strftime(time_format),
        income_spend['direction'],
        datetime.datetime.fromtimestamp(income_spend['origin_date']).strftime(time_format),
        income_spend['category']
    ]
    income_spend_sheet.writerow(row)

def write_likekind(likekind):
    time_format = '%Y/%m/%d'
    row = [
        likekind['id'],
        likekind['previous_id'],
        likekind['received'],
        likekind['received_amount'],
        likekind['received_price'],
        likekind['relinquished'],
        likekind['relinquished_amount'],
        likekind['relinquished_price'],
        datetime.datetime.fromtimestamp(likekind['swap_date']).strftime(time_format),
        datetime.datetime.fromtimestamp(likekind['last_trade_date']).strftime(time_format),
        datetime.datetime.fromtimestamp(likekind['origin_date']).strftime(time_format),
        likekind['cost_basis'],
    ]
    likekind_sheet.writerow(row)

def collect_addresses():
    # first get the exports
    g = glob.glob('./input_csvs/etherscan/export*')
    for path in tqdm(g):
        addr = path.split('export-')[1].split(' (')[0].split('.csv')[0]
        my_addresses.append([addr.lower(), 'ETH'])

    f = open('./input_csvs/trezor/BCH_Account.csv')
    first_row = True
    for row in f:
        if first_row:
            first_row = False
            continue
        date,time,tx_hash,address,tx_type,value,tx_total,fee,balance = row.split(',')
        my_addresses.append([address, 'BCH'])

    f = open('./input_csvs/trezor/BTC_Account.csv')
    first_row = True
    for row in f:
        if first_row:
            first_row = False
            continue
        date,time,tx_hash,address,tx_type,value,tx_total,fee,balance = row.split(',')
        my_addresses.append([address, 'BTC'])

    g = glob.glob('./input_csvs/trezor/DASH_Account*')
    for path in tqdm(g):
        f = open(path, 'r')
        first_row = True
        for row in f:
            if first_row:
                first_row = False
                continue
            date,time,tx_hash,address,tx_type,value,tx_total,fee,balance = row.split(',')
            my_addresses.append([address, 'DASH'])

    f = open('./derived_data/addresses.csv', 'w')
    writer = csv.writer(f)
    for addr in my_addresses:
        writer.writerow(addr)

    return my_addresses



def collect_transactions():
    txs = []

    g = glob.glob('./input_csvs/etherscan/*')
    for path in tqdm(g):
        txs.extend(parsers.parse_etherscan(path))

    g = glob.glob('./input_csvs/ethplorer/*')
    for path in tqdm(g):
        txs.extend(parsers.parse_ethplorer(path))

    txs.extend(parsers.parse_bittrex_orders('./input_csvs/bittrex/bittrex-fullOrders.csv'))
    txs.extend(parsers.parse_bittrex_deposits('./input_csvs/bittrex/bittrex-depositHistory.json'))
    txs.extend(parsers.parse_bittrex_withdrawals('./input_csvs/bittrex/bittrex-withdrawalHistory.json'))

    txs.extend(parsers.parse_poloniex_orders('./input_csvs/poloniex/poloniex-tradeHistory.csv'))
    txs.extend(parsers.parse_poloniex_deposits('./input_csvs/poloniex/poloniex-depositHistory.csv'))
    txs.extend(parsers.parse_poloniex_withdrawals('./input_csvs/poloniex/poloniex-withdrawalHistory.csv'))

    txs.extend(parsers.parse_kraken('./input_csvs/kraken/ledgers.csv'))

    g = glob.glob('./input_csvs/gdax/*')
    for path in tqdm(g):
        txs.extend(parsers.parse_gdax(path))

    g = glob.glob('./input_csvs/trezor/*')
    for path in tqdm(g):
        txs.extend(parsers.parse_trezor(path))

    g = glob.glob('./input_csvs/dash_core/*')
    for path in tqdm(g):
        txs.extend(parsers.parse_dash_core(path))

    for tx in txs:
        tx['date'] = datetime.datetime.fromtimestamp(tx['timestamp']).strftime('%Y/%m/%d')

    return txs

def maybe_print(s):
    if True:
        print(s)

def start_to_finish():
    initialize()
    try:
        txs = json.load(open('./derived_data/transactions-final.json', 'r'))
    except FileNotFoundError:
        txs = collect_transactions()
        json.dump(txs, open('./derived_data/transactions.json', 'w'), indent=4, separators=(',',':'))
    process_txs(txs)
