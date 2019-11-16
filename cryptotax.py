

# Big Picture:
# collect csvs
# make list of addresses that I own
# make list of transactions
# sort transactions by date
# ins are income, outs are spends, unless they co-occur, in which case, like kind or self transfer
# keep track of the lots as they dissolve

import copy
import json
from config import *
import os
from prices import prices
from hashlib import sha3_256 as sha
from config import logger
from wrangler import Wrangler

anomalies = []
target_currencies = ['ZET']

wrangler = Wrangler()

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
    global tx_id
    global spend_queues
    global data_hash

    tx_id = 0
    queues = {}
    spend_queues = {}
    balances = {}
    cost_bases = {}
    my_addresses = []


def likekind_eligible(tx):
    return int(tx['date'].split('/')[0]) < likekind_cutoff_year


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
        logger.debug('pairing order %d with %d' % (tx1['index'], tx2['index']))

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
        close_dollar = abs(tx2['dollar'] - tx1['dollar']) / max(tx2['dollar'], tx1['dollar'], 1) < dollar_pct
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
    txs = copy.deepcopy(wrangler.sort_txs_by_date(data))
    global tx_id
    tx_id = txs[-1]['index']
    i = 0
    while i < len(txs):
        tx1 = txs[i]
        if tx1['timestamp'] > cutoff_year.timestamp():
            logger.info('reached tx past cutoff year %s. Stopping' % cutoff_year)
            break

        # handle exchange orders
        if 'order' in tx1['notes']:
            process_exchange_order(txs, i)
        else:
            process_off_exchange(txs, i)
        i += 1

    wrangler.dump_balances_cost_basis(balances, cost_bases)


def handle_purchase_sale(tx1, tx2):
    incoming = tx1 if tx1['direction'] == 'in' else tx2
    outgoing = tx2 if tx2['direction'] == 'out' else tx1
    if incoming['currency'] == 'USD':
        handle_spend(outgoing, incoming)
    elif outgoing['currency'] == 'USD':
        handle_purchase(usd=outgoing,crypto=incoming)
    else:
        handle_spend(outgoing)
        handle_income(incoming)


def handle_single(tx):
    if tx['direction'] == 'in':
        handle_income(tx)
    elif tx['direction'] == 'out':
        handle_spend(tx)
    else:
        raise Exception('Unknown direction type %s' % tx)


# we purchased or received crypto
def handle_purchase(usd,crypto):
    #maybe_print('treating txs %d and %d as purchase' % (tx1['index'], tx2['index']))
    crypto['category'] = 'purchase'
    crypto['paired'] = usd['index']
    usd['category'] = 'purchase'
    usd['paired'] = crypto['index']
    crypto['cost_basis'] = usd['dollar']
    crypto['origin_date'] = crypto['timestamp']
    crypto['id'] = crypto['index']
    crypto['previous_id'] = -1
    crypto['origin_id'] = crypto['id']
    q_in = get_queue_for_currency(crypto['currency'])
    q_in.insert(0, crypto)
    update_balance(crypto['currency'], crypto['amount'])
    update_cost_basis(crypto['currency'], crypto['cost_basis'])
    wrangler.write_income_spend(crypto)


# we purchased or received crypto
def handle_income(income):
    logger.info('Handling income tx %s' % income)
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
    wrangler.write_income_spend(income)


def handle_spend(spend, incoming=None):
    #maybe_print('treating tx %d as spend' % spend['index'])
    if incoming is not None and incoming['currency'] == 'USD' and incoming['amount'] > 0:
        price =  incoming['amount'] / spend['amount']
    else:
        price = spend['price']
    spend_amount_left = spend['amount']
    timestamp = spend['timestamp']

    currency = spend['currency']
    q_out = get_queue_for_currency(currency)
    #assert_q_sorted(q_out)
    update_balance(currency, -1 * spend['amount'])

    while spend_amount_left > 0:
        if len(q_out) == 0:
            msg = "Empty queue for currency %s, tried to spend %s" % (currency, spend_amount_left)
            maybe_print(msg)
            # pprint(spend)
            # pprint(balances)
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
            'id': get_new_tx_id(),
            'previous_id': tx_out['id'],
            'currency': currency,
            'category': 'spend',
            'amount': spend_piece_amount,
            'cost_basis': cost_basis,
            'price': price,
            'timestamp': timestamp,
            'direction':'out',
            'origin_date': tx_out['origin_date'],
            'origin_id': tx_out['origin_id']
        }
        wrangler.write_income_spend(income_spend)


def handle_self_transfer(spend,income):
    spend['category'] = 'self transfer'
    spend['paired'] = income['index']
    income['category'] = 'self transfer'
    income['paired'] = spend['index']


def handle_likekind(tx1, tx2):
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
            msg = "Empty queue for currency %s, tried to pull %s" % (out_currency, spend_amount_left)
            maybe_print(msg)
            # pprint(spend)
            # pprint(income)
            # pprint(balances)
            anomalies.append([tx1,tx2])
            return
        tx_out = q_out[-1]
        if spend_amount_left > tx_out['amount']:
            spend_piece_amount = tx_out['amount']
            income_piece_amount = in_amount * spend_piece_amount / out_amount
            cost_basis = tx_out['cost_basis']
            q_out.pop()
        elif spend_amount_left == tx_out['amount']:
            spend_piece_amount = tx_out['amount']
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
        wrangler.write_likekind(likekind)


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
        logger.warning('Negative balance %s: %f' % (currency, balances[currency]))


def update_cost_basis(currency, amount):
    global target_currencies
    if currency in target_currencies:
        pass
        # print('cost_basis[%s] = %s + %s' % (currency, cost_bases.get(currency,0), amount))

    try:
        cost_bases[currency] += amount
    except KeyError:
        cost_bases[currency] = amount

    cost_bases[currency] = round(cost_bases[currency], 7)

    if cost_bases[currency] < 0:
        logger.warning('Negative cost basis %s: %f' % (currency, cost_bases[currency]))


def process_remainder():
    for symbol, q in queues.items():
        for item in q:
            cost_basis = {
                'amount': item['amount'],
                'symbol': symbol,
                'cost_basis': item['cost_basis'],
                'origin_date': item['origin_date']
            }
            wrangler.write_cost_basis(cost_basis)


def get_new_tx_id():
    global tx_id
    tx_id += 1
    return tx_id


def hash_path(path):
    with open(path, 'r') as file:
        return sha(file.read().encode()).hexdigest()


# def assert_q_sorted(q):
#     sorted_q = sorted(q, key=lambda x: x['timestamp'], reverse=True)
#     if sorted_q != q:
#         print('badbad')


def start_to_finish():

    if os.path.isfile(final_file) and not reload_data:
        maybe_print('Loading previously collected txs from disk')
        txs = json.load(open(final_file, 'r'))
    else:
        maybe_print('Collecting transactions')
        txs = wrangler.collect_transactions()
        wrangler.dump_txs(txs)
        wrangler.dump_prices(prices)
    initialize()
    process_txs(txs)
    process_remainder()


if __name__ == "__main__":
    start_to_finish()
    print('Dun')

