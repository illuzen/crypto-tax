

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
from config import *
import os
from prices import prices
from hashlib import sha3_256 as sha

anomalies = []
target_currencies = ['CRYPT']

#queues = { 'BTC': [], 'ETH': [], 'DASH': [], 'BCH': [] }
# queues = {}
# balances = {}
#
# my_addresses = []

def initialize():
    global queues
    global balances
    global cost_bases
    global my_addresses
    global likekind_sheet
    global likekind_file
    global income_spend_sheet
    global income_spend_file
    global cost_basis_sheet
    global cost_basis_file
    global tx_id
    global spend_queues

    tx_id = 0
    queues = {}
    spend_queues = {}
    balances = {}
    cost_bases = {}
    my_addresses = []
    income_spend_file = open('%s/incomespend.csv' % derived_folder, 'w')
    income_spend_sheet = csv.writer(income_spend_file)
    income_spend_sheet.writerow([
        'id',
        'previous_id',
        'currency',
        'amount',
        'cost_basis',
        'price',
        'timestamp',
        'direction',
        'origin_date',
        'category'
    ])
    likekind_file = open('%s/likekind.csv' % derived_folder, 'w')
    likekind_sheet = csv.writer(likekind_file)
    likekind_sheet.writerow([
        'id',
        'previous_id',
        'received',
        'received_amount',
        'received_price',
        'relinquished',
        'relinquished_amount',
        'relinquished_price',
        'swap_date',
        'last_trade_date',
        'origin_date',
        'origin_id',
        'cost_basis'
    ])
    cost_basis_file = open('%s/cost_basis.csv' % derived_folder, 'w')
    cost_basis_sheet = csv.writer(cost_basis_file)
    cost_basis_sheet.writerow([
        'Type',
        'Buy',
        'Cur.',
        'Sell',
        'Cur.',
        'Fee',
        'Cur.',
        'Exchange',
        'Group',
        'Comment',
        'Date'
    ])


def sort_txs_by_date(txs):
    sort = sorted(txs, key= lambda x: x['timestamp'])
    for i,tx in enumerate(sort):
        tx['index'] = i
    return sort


def likekind_eligible(tx):
    return int(tx['date'].split('/')[0]) < 2018


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

        if out_tx['currency'] == 'USD' or in_tx['currency'] == 'USD' or likekind_eligible(out_tx) == False:
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

        # is this a different currency we are looking at?
        different_currency = tx2['currency'] != tx1['currency']
        if not different_currency: continue

        # are we close enough to tx1 in time?
        close_time = abs(tx2['timestamp'] - tx1['timestamp']) < one_day
        if not close_time: break

        # must be different directions, neither is exchange order and close enough dollar amounts
        different_direction = tx2['direction'] != tx1['direction']
        if different_direction is False: continue
        close_dollar = abs(tx2['dollar'] - tx1['dollar']) / max(tx2['dollar'], tx1['dollar']) < dollar_pct
        not_order = 'order' not in tx2['notes']
        if not_order and close_dollar:
            found = True
            break

    #print(tx2)
    # if close in time and dollar amount, probably likekind or self transfer
    if found:
        if tx1['currency'] == 'USD' or tx2['currency'] == 'USD' or likekind_eligible(tx2) == False:
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
    json.dump(txs, open('%s/transactions-final-%d.json' % (derived_folder,datetime.datetime.now().timestamp()), 'w'), indent=4, separators=(',', ':'))

    i = 0
    while i < len(txs):
        tx1 = txs[i]
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
    dump_balances_cost_basis()


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
    #maybe_print('treating txs %d and %d as purchase' % (tx1['index'], tx2['index']))
    crypto['category'] = 'purchase'
    crypto['paired'] = usd['index']
    usd['category'] = 'purchase'
    usd['paired'] = crypto['paired']
    crypto['cost_basis'] = crypto['dollar']
    crypto['origin_date'] = crypto['timestamp']
    crypto['id'] = usd['index']
    crypto['origin_id'] = crypto['id']
    q_in = get_queue_for_currency(crypto['currency'])
    q_in.insert(0, crypto)
    update_balance(crypto['currency'], crypto['amount'])
    update_cost_basis(crypto['currency'], crypto['cost_basis'])
    #write_income_spend(crypto)


# we purchased or received crypto
def handle_income(income):
    logging.info('Handling income tx %s' % income)
    #maybe_print('treating tx %d as income' % income['index'])
    income['category'] = 'income'
    income['cost_basis'] = income['dollar']
    income['origin_date'] = income['timestamp']
    income['id'] = income['index']
    income['previous_id'] = -1
    income['origin_id'] = income['id']
    q_in = get_queue_for_currency(income['currency'])
    q_in.insert(0, income)
    update_balance(income['currency'], income['amount'])
    update_cost_basis(income['currency'], income['cost_basis'])
    write_income_spend(income)


def handle_spend(spend):
    #maybe_print('treating tx %d as spend' % spend['index'])
    price = spend['price']
    spend_amount_left = spend['amount']
    timestamp = spend['timestamp']
    out_currency = spend['currency']

    currency = spend['currency']
    q_out = get_queue_for_currency(currency)
    #assert_q_sorted(q_out)
    update_balance(currency, -1 * spend['amount'])

    while spend_amount_left > 0:
        if len(q_out) == 0:
            msg = "Empty queue for currency %s" % out_currency
            maybe_print(msg)
            pprint(spend)
            pprint(balances)
            anomalies.append([spend])
            return
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
        update_cost_basis(currency, -1 * cost_basis)

        if spend_amount_left < 1e-8:
            spend_amount_left = 0

        income_spend = {
            'id':get_new_tx_id(),
            'previous_id':tx_out['id'],
            'currency':currency,
            'category':'spend',
            'amount': spend_piece_amount,
            'cost_basis': cost_basis,
            'price':price,
            'timestamp': timestamp,
            'direction':'out',
            'origin_date': tx_out['origin_date'],
            'origin_id': tx_out['origin_id']
        }
        write_income_spend(income_spend)


def handle_self_transfer(spend,income):
    #maybe_print('pairing self transfer: %d with %d' % (spend['index'], income['index']))
    spend['category'] = 'self transfer'
    spend['paired'] = income['index']
    income['category'] = 'self transfer'
    income['paired'] = spend['index']


def handle_likekind(tx1, tx2):
    #maybe_print('pairing likekind txs %d and %d' % (tx1['index'], tx2['index']))
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
    #maybe_print('Spending %f %s, have %f' % (out_amount, out_currency, balances[out_currency]))
    #maybe_print('Receiving %f %s, have %f' % (in_amount, in_currency, balances[in_currency]))

    # deplete items from the out queue until spend is accounted for
    while spend_amount_left > 0:
        if len(q_out) == 0:
            msg = "Empty queue for currency %s" % out_currency
            maybe_print(msg)
            pprint(spend)
            pprint(income)
            pprint(balances)
            anomalies.append([tx1,tx2])
            return
        tx_out = q_out[-1]
        if spend_amount_left > tx_out['amount']:
            spend_piece_amount = tx_out['amount']
            income_piece_amount = in_amount * spend_piece_amount / out_amount
            cost_basis = tx_out['cost_basis']
            q_out.pop()
        elif spend_amount_left == tx_out['amount']:
            spend_piece_amount = spend_amount_left
            income_piece_amount = income_amount_left
            cost_basis = tx_out['cost_basis']
            q_out.pop()
        else:
            spend_piece_amount = spend_amount_left
            income_piece_amount = income_amount_left
            cost_basis = tx_out['cost_basis'] * spend_piece_amount / tx_out['amount']
            tx_out['amount'] -= spend_piece_amount
            tx_out['cost_basis'] -= cost_basis

        income_amount_left -= income_piece_amount
        spend_amount_left -= spend_piece_amount
        update_cost_basis(out_currency, -1 * cost_basis)
        update_cost_basis(in_currency, cost_basis)

        if spend_amount_left < 5e-6:
            spend_amount_left = 0

        income_piece = copy.deepcopy(income)
        income_piece['id'] = get_new_tx_id()
        income_piece['amount'] = income_piece_amount
        income_piece['dollar'] = income_piece['amount'] * income_piece['price']
        income_piece['origin_date'] = tx_out['origin_date']
        income_piece['origin_id'] = tx_out['origin_id']
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
            'origin_id': income_piece['origin_id'],
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
    global target_currencies
    if currency in target_currencies:
        print('balance[%s] = %s + %s' % (currency, balances.get(currency,0), amount))

    try:
        balances[currency] += amount
    except KeyError:
        balances[currency] = amount

    balances[currency] = round(balances[currency], 7)

    if balances[currency] < 0:
        maybe_print('Negative balance %s: %f' % (currency, balances[currency]))


def update_cost_basis(currency, amount):
    global target_currencies
    if currency in target_currencies:
        print('cost_basis[%s] = %s + %s' % (currency, cost_bases.get(currency,0), amount))

    try:
        cost_bases[currency] += amount
    except KeyError:
        cost_bases[currency] = amount

    cost_bases[currency] = round(cost_bases[currency], 7)

    if cost_bases[currency] < 0:
        maybe_print('Negative cost basis %s: %f' % (currency, cost_bases[currency]))


def process_remainder():
    for symbol, q in queues.items():
        for item in q:
            cost_basis = {
                'amount': item['amount'],
                'symbol': symbol,
                'cost_basis': item['cost_basis'],
                'origin_date': item['origin_date']
            }
            write_cost_basis(cost_basis)
    cost_basis_file.close()

def get_new_tx_id():
    global tx_id
    tx_id += 1
    return tx_id


def write_income_spend(income_spend):
    time_format = '%Y/%m/%d'
    row = [
        income_spend['id'],
        income_spend['previous_id'],
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
        likekind['origin_id'],
        likekind['cost_basis'],
    ]
    likekind_sheet.writerow(row)


def write_cost_basis(cost_basis):
    time_format = '%Y/%m/%d'
    row = [
        'Trade',
        cost_basis['amount'],
        cost_basis['symbol'],
        cost_basis['cost_basis'],
        'USD',
        '',
        '',
        'N/A - Like Kind Exchange',
        '',
        '',
        datetime.datetime.fromtimestamp(cost_basis['origin_date']).strftime(time_format)
    ]
    cost_basis_sheet.writerow(row)


def collect_addresses():
    # first get the exports
    g = glob.glob('%s/etherscan/export*' % input_folder)
    for path in tqdm(g):
        addr = path.split('export-')[1].split(' (')[0].split('.csv')[0]
        my_addresses.append([addr.lower(), 'ETH'])

    f = open('%s/trezor/BCH_Account.csv' % input_folder)
    first_row = True
    for row in f:
        if first_row:
            first_row = False
            continue
        date,time,tx_hash,address,tx_type,value,tx_total,fee,balance = row.split(',')
        my_addresses.append([address, 'BCH'])

    f = open('%s/trezor/BTC_Account.csv' % input_folder)
    first_row = True
    for row in f:
        if first_row:
            first_row = False
            continue
        date,time,tx_hash,address,tx_type,value,tx_total,fee,balance = row.split(',')
        my_addresses.append([address, 'BTC'])

    g = glob.glob('%s/trezor/DASH_Account*' % input_folder)
    for path in tqdm(g):
        f = open(path, 'r')
        first_row = True
        for row in f:
            if first_row:
                first_row = False
                continue
            date,time,tx_hash,address,tx_type,value,tx_total,fee,balance = row.split(',')
            my_addresses.append([address, 'DASH'])

    f = open('%s/addresses.csv' % derived_folder, 'w')
    writer = csv.writer(f)
    for addr in my_addresses:
        writer.writerow(addr)

    return my_addresses


def hash_path(path):
    with open(path, 'r') as file:
        return sha(file.read().encode()).hexdigest()


def collect_transactions():
    txs = []
    failed = []
    hashes = []
    g = glob.glob('%s/cointracker/*' % input_folder)
    for path in g:
        parsed, not_parsed = parsers.parse_coin_tracker(path)
        txs.extend(parsed)
        failed.extend(not_parsed)

    g = glob.glob('%s/etherscan/*' % input_folder)
    for path in g:
        txs.extend(parsers.parse_etherscan(path))

    g = glob.glob('%s/ethplorer/*' % input_folder)
    for path in g:
        txs.extend(parsers.parse_ethplorer(path))

    txs.extend(parsers.parse_bittrex_orders('%s/bittrex/bittrex-fullOrders.csv' % input_folder))
    txs.extend(parsers.parse_bittrex_deposits('%s/bittrex/bittrex-depositHistory.json' % input_folder))
    txs.extend(parsers.parse_bittrex_withdrawals('%s/bittrex/bittrex-withdrawalHistory.json' % input_folder))

    txs.extend(parsers.parse_poloniex_orders('%s/poloniex/poloniex-tradeHistory.csv' % input_folder))
    txs.extend(parsers.parse_poloniex_deposits('%s/poloniex/poloniex-depositHistory.csv' % input_folder))
    txs.extend(parsers.parse_poloniex_withdrawals('%s/poloniex/poloniex-withdrawalHistory.csv' % input_folder))

    txs.extend(parsers.parse_kraken('%s/kraken/ledgers.csv' % input_folder))

    g = glob.glob('%s/gdax/*' % input_folder)
    for path in g:
        txs.extend(parsers.parse_gdax(path))

    g = glob.glob('%s/trezor/*' % input_folder)
    for path in g:
        txs.extend(parsers.parse_trezor(path))

    g = glob.glob('%s/dash_core/*' % input_folder)
    for path in g:
        txs.extend(parsers.parse_dash_core(path))

    for tx in txs:
        tx['date'] = datetime.datetime.fromtimestamp(tx['timestamp']).strftime('%Y/%m/%d')

    if len(failed) > 0: json.dump(failed, open('%s/failed.tsv' % derived_folder, 'w'))

    return txs


def dump_txs(txs):
    maybe_print('Writing txs to disk')
    json.dump(txs, open('%s/transactions.json' % derived_folder, 'w'), indent=4, separators=(',', ':'))


def dump_prices(p):
    # prices[symbol][date_string]
    maybe_print('Writing prices to disk')
    with open('./derived_data/prices/prices.csv', 'w') as file:
        for symbol in p:
            for date_string in p[symbol]:
                price = p[symbol][date_string]
                file.write('%s,%s,%s\n' % (symbol, date_string, price))


def dump_balances_cost_basis():
    maybe_print('Writing final balances to disk')
    d = {'balances': balances, 'cost_basis': cost_bases}
    json.dump(d, open('%s/balances.json' % derived_folder, 'w'), indent=4, separators=(',', ':'))


# def assert_q_sorted(q):
#     sorted_q = sorted(q, key=lambda x: x['timestamp'], reverse=True)
#     if sorted_q != q:
#         print('badbad')


def start_to_finish():
    initialize()

    if os.path.isfile(final_file) and not reload_data:
        maybe_print('Loading previously collected txs from disk')
        txs = json.load(open(final_file, 'r'))
    else:
        maybe_print('Collecting transactions')
        txs = collect_transactions()
        dump_txs(txs)
        dump_prices(prices)
    process_txs(txs)
    process_remainder()

if __name__ == "__main__":
    start_to_finish()
    print('Dun')

