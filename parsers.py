import prices
import dateutil
import json
from config import *
import pandas as pd

from config import logger


token_symbols = {}

def load_token_symbols():
    global token_symbols
    if len(token_symbols) == 0:
        df = pd.read_csv('./data/token_symbol.csv')
        df.index = df['Address'].apply(lambda x: x.replace("'",''))
        df.drop(columns=['Address'], inplace=True)
        token_symbols = df.to_dict()['Symbol']
    return token_symbols


def parse_ethplorer(path):
    logger.info('parsing file %s' % path)
    txs = []

    f = open(path, 'r')
    f.__next__()
    lines = f.readlines()
    _, _, addr1, addr2, _, _, _, _ = lines[0].split(';')
    addr1_present, addr2_present = True, True
    for row in lines[1:]:
        _, _, frum, to, _, _, _, _ = row.split(';')
        if frum != addr1 and to != addr1:
            addr1_present = False
        if frum != addr2 and to != addr2:
            addr2_present = False

    if addr1_present and not addr2_present:
        my_address = addr1
    elif addr2_present and not addr1_present:
        my_address = addr2
    elif len(lines) == 1:
        my_address = addr2
    else:
        logger.warning('Unable to infer which address is ours: {}'.format(path))
        return txs

    f = open(path, 'r')
    first_row = True
    for row in f:
        if first_row:
            first_row = False
            continue
        date,txhash,frum,to,token_name,token_address,quantity,symbol = row.split(';')
        symbol = symbol.replace('\n', '')
        direction = 'in' if to == my_address else 'out'
        quantity = float(quantity)
        dt = dateutil.parser.parse(date)
        # dt = date.split(' ')[0].split('-')
        # dt.extend(date.split(' ')[1].split(':'))
        # dt = [int(x) for x in dt]
        # dt = datetime.datetime(*dt)
        try:
            price = prices.get_price(symbol, dt)
            if not price: price = 0
            dollar = quantity * price
        except Exception as e:
            logger.warning(e)
            continue

        if direction == '':
            raise Exception('empty direction from ethplorer: %s' % row)

        txs.append({
            'dollar': dollar,
            'direction': direction,
            'price': price,
            'amount': quantity,
            'currency': symbol,
            'timestamp': dt.timestamp(),
            'notes': 'ethplorer'
        })
    return txs


def parse_etherscan_token(path):
    logger.info('parsing file %s' % path)
    this_addr = path.split('.csv')[0].split('-')[-1].lower()
    f = open(path, 'r')
    first_row = True
    txs = []
    for row in f:
        cols = row.split(',')
        if first_row:
            first_row = False
            continue
        txhash, unix_timestamp, date_time, frum, to, quantity, token_addr = cols
        token_addr = token_addr.replace('"','').replace('\n','' ).lower()
        frum = frum.replace('"','').replace('\n','' ).lower()
        to = to.replace('"','').replace('\n','' ).lower()
        currency = load_token_symbols().get(token_addr)
        if currency is None:
            logger.warning('Unknown token address: {}'.format(token_addr))
            continue
        quantity = float(quantity.replace('"',''))
        if frum == this_addr:
            direction = 'out'
        elif to == this_addr:
            direction = 'in'
        else:
            logger.warning('Unknown direction {}, {}, {}'.format(frum, to, this_addr))
            continue

        date_time = date_time.replace('"', '')
        dt = dateutil.parser.parse(date_time)

        try:
            price = prices.get_price(currency, dt)
        except Exception:
            logger.warning('Unable to get price for {} {}'.format(currency, dt))
            continue
        logger.info('quantity: {} price: {} currency: {} dt: {}'.format(quantity, price, currency, dt))
        dollar = quantity * price

        txs.append({
            'dollar': dollar,
            'direction': direction,
            'price': price,
            'amount': quantity,
            'currency': currency,
            'timestamp': dt.timestamp(),
            'notes': 'etherscan token'
        })
    return txs


def parse_etherscan(path):
    logger.info('parsing file %s' % path)
    f = open(path, 'r')
    first_row = True
    txs = []
    type_branch = 0
    for row in f:
        cols = row.split(',')
        if first_row:
            first_row = False
            if 'Type' in cols[-1]:
                type_branch = 1
            elif 'ErrCode' in cols[-1]:
                type_branch = 2
            else:
                raise Exception('Unexpected column format for csv at %s' % (path))
            continue
        if type_branch == 1:
            txhash,blockno,unix_timestamp,date_time,frum,to,contract_address,value_in,value_out,current_value,historical_price,status,err_code,taip = cols
        elif type_branch == 2:
            if len(cols) == 15:
                txhash,blockno,unix_timestamp,date_time,frum,to,contract_address,value_in,value_out,current_value,fee_eth,fee_usd,historical_price,status,err_code = cols
            elif len(cols) == 16:
                txhash,blockno,unix_timestamp,date_time,frum,to,contract_address,value_in,value_out,current_value,fee_eth,fee_usd,historical_price,status,err_code,_ = cols

        value_in = float(value_in.replace('"', ''))
        value_out = float(value_out.replace('"', ''))
        date_time = date_time.replace('"', '')
        dt = date_time.split(' ')[0].split('/')
        dt.extend(date_time.split(' ')[1].split(':'))
        dt = [int(x) for x in dt]
        month, day, year, hour, minute, second = dt
        dt = datetime.datetime(year, month, day, hour, minute, second)

        try:
            price = float(historical_price.replace('"', ''))
        except Exception:
            price = prices.get_price('ETH',dt)

        if value_in == 0 and value_out == 0:
            continue
        elif value_in > 0:
            direction = 'in'
            quantity = value_in
        elif value_out > 0:
            direction = 'out'
            quantity = value_out
        else:
            raise Exception ('Value should be non-negative: %f,%f' % (value_in, value_out))

        dollar = quantity * price

        txs.append({
            'dollar': dollar,
            'direction': direction,
            'price': price,
            'amount': quantity,
            'currency': 'ETH',
            'timestamp': dt.timestamp(),
            'notes': 'etherscan'
        })
    return txs


def parse_bittrex_orders(path):
    f = maybe_open(path)
    if f is None: return []
    first_row = True
    txs = []
    for row in f:
        if first_row:
            first_row = False
            continue
        OrderUuid,exchange,order_type,quantity,limit,commission,price,date_opened,date_closed = row.split(',')
        quantity = float(quantity)
        price = float(price)
        commission = float(commission)

        date_format = '%m/%d/%Y %I:%M:%S %p'
        dt = datetime.datetime.strptime(date_closed.replace('\n',''),date_format)

        # determine orientation
        if 'BUY' in order_type:
            out_currency, in_currency = exchange.split('-')[:2]
            # badly named column
            out_quantity = price - commission
            in_quantity = quantity
        elif 'SELL' in order_type:
            in_currency, out_currency = exchange.split('-')[:2]
            # badly named column
            out_quantity = quantity
            in_quantity = price - commission
        if in_currency == 'BCC':
            in_currency = 'BCH'
        if out_currency == 'BCC':
            out_currency = 'BCH'
        if in_currency == 'ANS':
            in_currency = 'NEO'
        if out_currency == 'ANS':
            out_currency = 'NEO'


        try:
            out_price = prices.get_price(out_currency, dt)
            out_dollar = out_quantity * out_price
            in_price = prices.get_price(in_currency, dt)
            in_dollar = in_quantity * in_price
        except Exception as e:
            logger.warning(e)
            continue

        txs.append({
            'dollar': out_dollar,
            'direction': 'out',
            'price': out_price,
            'amount': out_quantity,
            'currency': out_currency,
            'timestamp':  dt.timestamp(),
            'notes': 'bittrex order'
        })

        txs.append({
            'dollar': in_dollar,
            'direction': 'in',
            'price': in_price,
            'amount': in_quantity,
            'currency': in_currency,
            'timestamp': dt.timestamp(),
            'notes': 'bittrex order'
        })
    return txs


def parse_bittrex_withdrawals(path):
    f = maybe_open(path)
    if f is None: return []
    data = json.load(f)
    withdrawals = data['result']['withdrawals']
    txs = []

    for withdrawal in withdrawals:
        currency = withdrawal['Currency']
        if currency == 'BCC':
            currency = 'BCH'
        if currency == 'ANS':
            currency = 'NEO'
        # 'Opened': '2017-10-21T02:38:30.693',
        date, time = withdrawal['Opened'].split('T')
        date = date.split('-')
        date.extend(time.split(':'))
        date[-1] = date[-1].split('.')[0]
        dt = [int(x) for x in date]
        dt = datetime.datetime(*dt)
        try:
            price = prices.get_price(currency, dt)
        except Exception as e:
            logger.warning(e)
            continue
        amount = withdrawal['Amount']
        dollar = price * amount

        txs.append({
            'dollar': dollar,
            'direction':'out',
            'price': price,
            'amount': amount,
            'currency': currency,
            'timestamp': dt.timestamp(),
            'notes':'bittrex withdrawal'
        })

    return txs


def parse_bittrex_deposits(path):
    f = maybe_open(path)
    if f is None: return []
    data = json.load(f)
    deposits = data['result']['deposits']
    txs = []

    for deposit in deposits:
        currency = deposit['Currency']
        if currency == 'BCC':
            currency = 'BCH'
        if currency == 'ANS':
            currency = 'NEO'
        # 'Opened': '2017-10-21T02:38:30.693',
        date, time = deposit['LastUpdated'].split('T')
        date = date.split('-')
        date.extend(time.split(':'))
        date[-1] = date[-1].split('.')[0]
        dt = [int(x) for x in date]
        dt = datetime.datetime(*dt)
        try:
            price = prices.get_price(currency, dt)
        except Exception as e:
            logger.warning(e)
            continue
        amount = deposit['Amount']
        dollar = price * amount

        txs.append({
            'dollar': dollar,
            'direction':'in',
            'price': price,
            'amount': amount,
            'currency': currency,
            'timestamp': dt.timestamp(),
            'notes':'bittrex deposit'
        })

    return txs


def parse_poloniex_orders(path):
    f = maybe_open(path)
    if f is None: return []
    first_row = True
    txs = []
    for row in f:
        if first_row:
            first_row = False
            continue
        date,exchange,category,order_type,price,amount,total,fee,order_number,base_total_less_fee,quote_total_less_fee = row.split(',')
        dt = date.split(' ')[0].split('-')
        t = date.split(' ')[1].split(':')
        dt.extend(t)
        dt = [int(x) for x in dt]
        dt = datetime.datetime(*dt)
        cur1, cur2 = exchange.split('/')
        if cur1 == 'STR':
            cur1 = 'XLM'
        if cur2 == 'STR':
            cur2 = 'XLM'
        # determine orientation
        if 'Buy' in order_type:
            in_currency, out_currency = cur1, cur2
            out_quantity = abs(float(base_total_less_fee))
            in_quantity = abs(float(quote_total_less_fee))

        elif 'Sell' in order_type:
            out_currency, in_currency = cur1, cur2
            in_quantity = abs(float(base_total_less_fee))
            out_quantity = abs(float(quote_total_less_fee))

        try:
            out_price = prices.get_price(out_currency, dt)
            out_dollar = out_quantity * out_price
            in_price = prices.get_price(in_currency, dt)
            in_dollar = in_quantity * in_price
        except Exception as e:
            print('Dropping poloniex order', row)
            logger.warning(e)
            continue

        txs.append({
            'dollar': out_dollar,
            'direction': 'out',
            'price': out_price,
            'amount': out_quantity,
            'currency': out_currency,
            'timestamp': dt.timestamp(),
            'notes': 'poloniex order'
        })

        txs.append({
            'dollar': in_dollar,
            'direction': 'in',
            'price': in_price,
            'amount': in_quantity,
            'currency': in_currency,
            'timestamp': dt.timestamp(),
            'notes': 'poloniex order'
        })
    return txs


def parse_poloniex_withdrawals(path):
    f = maybe_open(path)
    if f is None: return []
    first_row = True
    txs = []
    for row in f:
        if first_row:
            first_row = False
            continue
        date,currency,amount,address,status = row.split(',')
        amount = float(amount)
        if currency == 'BCC':
            currency = 'BCH'
        dt = date.split(' ')
        date = dt[0].split('-')
        date.extend(dt[1].split(':'))
        dt = [int(x) for x in date]
        dt = datetime.datetime(*dt)
        try:
            price = prices.get_price(currency, dt)
        except Exception as e:
            logger.warning(e)
            continue

        dollar = price * amount

        txs.append({
            'dollar': dollar,
            'direction':'out',
            'price': price,
            'amount': amount,
            'currency': currency,
            'timestamp': dt.timestamp(),
            'notes':'poloniex withdrawal'
        })

    return txs


def parse_poloniex_deposits(path):
    f = maybe_open(path)
    if f is None: return []
    first_row = True
    txs = []
    for row in f:
        if first_row:
            first_row = False
            continue
        date,currency,amount,address,status = row.split(',')
        amount = float(amount)
        if currency == 'BCC':
            currency = 'BCH'
        dt = date.split(' ')
        date = dt[0].split('-')
        date.extend(dt[1].split(':'))
        dt = [int(x) for x in date]
        dt = datetime.datetime(*dt)
        try:
            price = prices.get_price(currency, dt)
        except Exception as e:
            logger.warning(e)
            continue

        dollar = price * amount

        txs.append({
            'dollar': dollar,
            'direction':'in',
            'price': price,
            'amount': amount,
            'currency': currency,
            'timestamp': dt.timestamp(),
            'notes':'poloniex deposit'
        })

    return txs


def parse_gdax(path):
    f = maybe_open(path)
    if f is None: return []
    first_row = True
    txs = []
    for row in f:
        if first_row:
            first_row = False
            continue
        entry_type,time,amount,balance,currency,transfer_id,trade_id,order_id = row.split(',')
        amount = float(amount)
        balance = float(balance)
        date, time = time.split('T')
        date = date.split('-')
        date.extend(time.split(':'))
        date[-1] = date[-1].split('.')[0]
        dt = [int(x) for x in date]
        dt = datetime.datetime(*dt)

        try:
            price = prices.get_price(currency, dt)
        except Exception as e:
            logger.warning(e)
            print(e)
            continue

        if entry_type == 'deposit':
            notes = 'gdax deposit'
            direction = 'in'
        elif entry_type == 'match':
            notes = 'gdax order'
            if amount < 0:
                direction = 'out'
                amount *= -1
            elif amount > 0:
                direction = 'in'
            else:
                raise Exception('Entry with zero amount %s' % row)
        elif entry_type == 'withdrawal':
            notes = 'gdax withdrawal'
            direction = 'out'
            amount *= -1
        elif entry_type == 'fee':
            continue
        elif entry_type == 'rebate':
            continue
        else:
            raise Exception('unrecognized entry type: %s' % entry_type)

        dollar = price * amount

        if direction == '':
            raise Exception('empty direction from gdax: %s' % row)

        txs.append({
            'dollar': dollar,
            'direction':direction,
            'price': price,
            'amount': amount,
            'currency': currency,
            'timestamp': dt.timestamp(),
            'notes':notes
        })

    return txs


def parse_binance_pair(pair):
    pair = pair.replace('BCHABC', 'BCH').replace('BCC','BCH').replace('BCHSV','BSV')
    for i in range(len(pair)):
        if pair[:i] in prices.prices and pair[i:] in prices.prices:
            return pair[:i], pair[i:]
    logger.error('Could not parse binance pair {}'.format(pair))
    raise ValueError()


def parse_binance_orders(path):
    f = maybe_open(path)
    if f is None: return []
    f = pd.read_excel(path)
    txs = []
    for i, row in f.iterrows():
        if type(row['Date(UTC)']) != str:
            continue
        dt = dateutil.parser.parse(row['Date(UTC)'])
        if dt.year > target_year:
            continue

        cur1, cur2 = parse_binance_pair(row['Pair'])
        if row['Type'] == 'BUY':
            in_currency = cur1
            in_qty = row['Filled']
            out_currency = cur2
            out_qty = row['Total']
        elif row['Type'] == 'SELL':
            in_currency = cur2
            in_qty = row['Total']
            out_currency = cur1
            out_qty = row['Filled']
        else:
            logger.warning('Unknown Type {}'.format(row['Type']))
            continue

        in_price = prices.get_price(in_currency, dt)
        in_dollar = in_price * in_qty
        out_price = prices.get_price(out_currency, dt)
        out_dollar = out_price * out_qty

        txs.append({
            'dollar': out_dollar,
            'direction': 'out',
            'price': out_price,
            'amount': out_qty,
            'currency': out_currency,
            'timestamp':  dt.timestamp(),
            'notes': 'binance order'
        })

        txs.append({
            'dollar': in_dollar,
            'direction': 'in',
            'price': in_price,
            'amount': in_qty,
            'currency': in_currency,
            'timestamp': dt.timestamp(),
            'notes': 'binance order'
        })
    return txs


def parse_binance_withdrawals(path):
    f = maybe_open(path)
    if f is None: return []
    f = pd.read_excel(path)
    txs = []
    for i, row in f.iterrows():
        dt = dateutil.parser.parse(row['Date'])
        if dt.year > target_year:
            continue
        currency = row['Coin'].replace('BCHABC','BCH').replace('BCC','BCH').replace('BCHSV','BSV')
        try:
            price = prices.get_price(currency, dt)
        except Exception as e:
            logger.warning('Could not get price for row {}'.format(row))
            continue
        amount = row['Amount']
        dollar = price * amount

        txs.append({
            'dollar': dollar,
            'direction':'out',
            'price': price,
            'amount': amount,
            'currency': currency,
            'timestamp': dt.timestamp(),
            'notes':'binance withdrawal'
        })
    return txs


def parse_binance_distributions(path):
    f = maybe_open(path)
    if f is None: return []
    f = pd.read_csv(path)
    txs = []
    for i, row in f.iterrows():
        dt = dateutil.parser.parse(row['Date'])
        if dt.year > target_year:
            continue
        currency = row['Coin'].replace('BCHABC','BCH').replace('BCC','BCH').replace('BCHSV','BSV')
        try:
            price = prices.get_price(currency, dt)
        except Exception as e:
            logger.warning('Could not get price for row {}'.format(row))
            continue
        amount = row['Amount']
        dollar = price * amount

        txs.append({
            'dollar': dollar,
            'direction':'in',
            'price': price,
            'amount': amount,
            'currency': currency,
            'timestamp': dt.timestamp(),
            'notes':'binance distribution'
        })
    return txs



def parse_binance_deposits(path):
    f = maybe_open(path)
    if f is None: return []
    f = pd.read_excel(path)
    txs = []
    for i, row in f.iterrows():

        dt = dateutil.parser.parse(row['Date'])
        if dt.year > target_year:
            continue
        currency = row['Coin'].replace('BCHABC','BCH').replace('BCC','BCH').replace('BCHSV','BSV')
        try:
            price = prices.get_price(currency, dt)
        except Exception as e:
            logger.warning('Could not get price for row {}'.format(row))
            continue
        amount = row['Amount']
        dollar = price * amount

        txs.append({
            'dollar': dollar,
            'direction':'in',
            'price': price,
            'amount': amount,
            'currency': currency,
            'timestamp': dt.timestamp(),
            'notes':'binance deposit'
        })
    return txs



def parse_kraken(path):
    f = maybe_open(path)
    if f is None: return []
    first_row = True
    txs = []
    for row in f:
        if first_row:
            first_row = False
            continue
        cols = [ col.replace('"','') for col in row.split(',') ]

        txid,refid,time,entry_type,aclass,currency,amount,fee,balance = cols
        amount = float(amount)

        date, time = time.split(' ')
        date = date.split('-')
        date.extend(time.split(':'))
        dt = [int(x) for x in date]
        dt = datetime.datetime(*dt)

        if currency == 'XETH':
            currency = 'ETH'
        elif currency == 'XXBT':
            currency = 'BTC'
        elif currency == 'XXMR':
            currency = 'XMR'
        elif currency == 'ZUSD':
            currency = 'USD'

        try:
            price = prices.get_price(currency, dt)
        except Exception as e:
            logger.warning(e)
            continue

        if entry_type == 'deposit':
            notes = 'kraken deposit'
            direction = 'in'
        elif entry_type == 'trade':
            notes = 'kraken order'
            if amount < 0:
                direction = 'out'
                amount *= -1
            elif amount > 0:
                direction = 'in'
            else:
                raise Exception('Entry with zero amount %s' % row)
        elif entry_type == 'withdrawal':
            notes = 'kraken withdrawal'
            direction = 'out'
            amount *= -1
        else:
            raise Exception('unrecognized entry type: %s' % entry_type)

        dollar = price * amount

        if direction == '':
            raise Exception('empty direction from gdax: %s' % row)

        txs.append({
            'dollar': dollar,
            'direction':direction,
            'price': price,
            'amount': amount,
            'currency': currency,
            'timestamp': dt.timestamp(),
            'notes':notes
        })

    return txs


# def parse_coinbase_pro(path):
#     f = maybe_open(path)
#     if f is None: return []
#     first_row = True
#     txs = []
#     for row in f:
#         if first_row:
#             first_row = False
#             continue
#         cols = row.split(',')
#         txid,product,side,created_at,size,size_unit,price,fee,total,price_unit = cols
#         if side == 'BUY':
#             buy_cur, sell_cur = product.split('-')
#         elif side == 'SELL':
#             sell_cur, buy_cur = product.split('-')
#         else:
#             logger.warning('Unknown side {}'.format(side))
#             continue
#
#         if size_unit == buy_cur:
#             buy_qty = float(size)
#             sell_qty = -1 * float(total)
#         elif size_unit == sell_cur:
#             buy_qty = float(total)
#             sell_qty = float(size)
#         else:
#             logger.warning('Unknown size_unit {}'.format(size_unit))
#             continue
#         dt = dateutil.parser.parse(created_at)
#
#         try:
#             buy_price = prices.get_price(buy_cur, dt)
#             buy_dollar = buy_qty * buy_price
#             sell_price = prices.get_price(sell_cur, dt)
#             sell_dollar = sell_qty * sell_price
#         except Exception as e:
#             logger.warning('Dropping coinbase pro order {}'.format(row))
#             continue
#
#         txs.append({
#             'dollar': sell_dollar,
#             'direction': 'out',
#             'price': sell_price,
#             'amount': sell_qty,
#             'currency': sell_cur,
#             'timestamp': dt.timestamp(),
#             'notes': 'coinbase_pro order'
#         })
#
#         txs.append({
#             'dollar': buy_dollar,
#             'direction': 'in',
#             'price': buy_price,
#             'amount': buy_qty,
#             'currency': buy_cur,
#             'timestamp': dt.timestamp(),
#             'notes': 'coinbase_pro order'
#         })
#     return txs


def parse_trezor(path):
    f = maybe_open(path)
    if f is None: return []
    currency = path.split('_')[-1].split('.')[0].upper()
    first_row = True
    txs = []
    for row in f:
        if first_row:
            first_row = False
            continue
        date,time,tx_id,address,direction,amount,total,balance = row.split(',')
        direction = direction.lower()
        if direction != 'in' and direction != 'out':
            logger.info('Unknown direction {}'.format(direction))
            continue
        amount = abs(float(total))
        dt = date.split('-')
        dt.extend(time.split(':'))
        dt = [int(x) for x in dt]
        dt = datetime.datetime(*dt)
        if dt.year > target_year: continue

        try:
            price = prices.get_price(currency, dt)
        except Exception as e:
            logger.warning(e)
            continue

        dollar = price * amount

        if direction == '':
            raise Exception('empty direction from trezor: %s' % row)

        txs.append({
            'dollar': dollar,
            'direction':direction,
            'price': price,
            'amount': amount,
            'currency': currency,
            'timestamp': dt.timestamp(),
            'notes':'trezor'
        })

    return txs


def parse_dash_core(path):
    f = maybe_open(path)
    if f is None: return []
    currency = 'DASH'
    first_row = True
    txs = []
    for row in f:
        if first_row:
            first_row = False
            continue
        cols = [col.replace('"', '') for col in row.split(',')]

        confirmed,date,taip,label,address,amount,tx_id = cols
        if not ('Sent' in taip or 'Received' in taip or 'PrivateSend' == taip):
            logger.info('Ignoring tx type %s' % taip)
            continue
        if 'Sent' in taip or 'PrivateSend' == taip:
            direction = 'out'
        elif 'Received' in taip:
            direction = 'in'
        date, time = date.split('T')
        date = date.split('-')
        date.extend(time.split(':'))
        dt = [int(x) for x in date]
        dt = datetime.datetime(*dt)
        amount = abs(float(amount))

        try:
            price = prices.get_price(currency, dt)
        except Exception as e:
            logger.warning(e)
            continue

        dollar = price * amount

        if direction == '':
            raise Exception('empty direction from dash core: %s' % row)

        txs.append({
            'dollar': dollar,
            'direction':direction,
            'price': price,
            'amount': amount,
            'currency': currency,
            'timestamp': dt.timestamp(),
            'notes':'dash core'
        })
    return txs


def parse_coin_tracker(path):
    f = maybe_open(path)
    if f is None: return []
    f.__next__()
    lines = f.readlines()
    txs = []
    failed_rows = []
    for i, row in enumerate(lines):
        type,buy_amt,buy_cur,sell_amt,sell_cur,fee_amt,fee_cur,exchange,group,_,date = row.split('\t')
        date = date.replace('\n', '')
        dt = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        if dt >= cutoff_year:
            maybe_print('Skipping row %d because date %s is out of range' % (i, date))
            continue
        if group == 'Margin' and ignore_margins:
            maybe_print('Skipping row %d because margin trade %s' % (i, row))
            continue
        if buy_cur == 'STR': buy_cur = 'XLM'
        if sell_cur == 'STR': sell_cur = 'XLM'
        if fee_cur == 'STR': fee_cur = 'XLM'
        buy_amt = float(buy_amt) if len(buy_amt) > 0 else 0
        sell_amt = float(sell_amt) if len(sell_amt) > 0 else 0
        fee_amt = float(fee_amt) if len(fee_amt) > 0 else 0
        # if exchange != 'Poloniex':
        #     if fee_cur == buy_cur:
        #         buy_amt -= fee_amt
        #     if fee_cur == sell_cur:
        #         sell_amt -= fee_amt
        if type == 'Trade':
            try:
                sell_price = prices.get_price(sell_cur, dt)
            except Exception as e:
                sell_price = None
            try:
                buy_price = prices.get_price(buy_cur, dt)
            except Exception as e:
                sell_price = None

            if buy_price is None and sell_price is None:
                print('Cannot handle row, saving %s' % row)
                failed_rows.append(row)
                continue
            if buy_price is None:
                buy_dollar = sell_price * sell_amt
                buy_price = buy_dollar / buy_amt
            else:
                buy_dollar = buy_price * buy_amt
            if sell_price is None:
                sell_dollar = buy_price * buy_amt
                sell_price = sell_dollar / sell_amt
            else:
                sell_dollar = sell_price * sell_amt

            buy_dir = 'in'
            sell_dir = 'out'
            txs.append({
                'dollar': sell_dollar,
                'direction': sell_dir,
                'price': sell_price,
                'amount': sell_amt,
                'currency': sell_cur,
                'timestamp': dt.timestamp(),
                'notes': 'gsheet order'
            })

            txs.append({
                'dollar': buy_dollar,
                'direction': buy_dir,
                'price': buy_price,
                'amount': buy_amt,
                'currency': buy_cur,
                'timestamp': dt.timestamp(),
                'notes': 'gsheet order'
            })

        elif type == 'Withdrawal':
            # not taxable event
            pass
        elif type == 'Deposit':
            # not taxable event
            pass
        elif type == 'Income' or type == 'Mining' or type == 'Gift':
            try:
                buy_price = prices.get_price(buy_cur, dt)
            except Exception as e:
                buy_price = None
            if buy_price is None:
                print('Cannot get price for %s on %s for Income, using 0 as price for cost_basis' % (buy_cur, dt))
                buy_price = 0
            buy_dollar = buy_price * buy_amt
            buy_dir = 'in'
            txs.append({
                'dollar': buy_dollar,
                'direction': buy_dir,
                'price': buy_price,
                'amount': buy_amt,
                'currency': buy_cur,
                'timestamp': dt.timestamp(),
                'notes': 'gsheet'
            })

        elif type == 'Spend':
            # we are not doing capital gains / losses for this one
            sell_price = prices.get_price(sell_cur, dt)
            if sell_price is None:
                print('Cannot handle row, saving %s' % row)
                failed_rows.append(row)
                continue
            sell_dollar = sell_price * sell_amt
            sell_dir = 'out'
            txs.append({
                'dollar': sell_dollar,
                'direction': sell_dir,
                'price': sell_price,
                'amount': sell_amt,
                'currency': sell_cur,
                'timestamp': dt.timestamp(),
                'notes': 'gsheet'
            })
        elif type == 'Lost' or type == 'Stolen':
            # treat as a trade for 0 USD
            sell_price = 0
            buy_price = 1
            buy_dollar = 0
            sell_dollar = 0

            buy_dir = 'in'
            sell_dir = 'out'
            txs.append({
                'dollar': sell_dollar,
                'direction': sell_dir,
                'price': sell_price,
                'amount': sell_amt,
                'currency': sell_cur,
                'timestamp': dt.timestamp(),
                'notes': 'gsheet order'
            })

            txs.append({
                'dollar': buy_dollar,
                'direction': buy_dir,
                'price': buy_price,
                'amount': buy_amt,
                'currency': buy_cur,
                'timestamp': dt.timestamp(),
                'notes': 'gsheet order'
            })
        else:
            print('Unknown trade type: {}'.format(type))

    return txs, failed_rows


def parse_coin_tracker_custom(path):
    f = maybe_open(path)
    if f is None: return []
    f.__next__()
    lines = f.readlines()
    txs = []
    failed_rows = []
    for i, row in enumerate(lines):
        _,type,buy_amt,buy_cur,sell_amt,sell_cur,exchange,ignore,_,date = row.split('\t')
        if len(ignore) > 0:
            maybe_print('Ignoring row: %s' % row)
            continue
        date = date.replace('\n', '')
        dt = datetime.datetime.strptime(date, '%d.%m.%Y %H:%M')
        if dt >= cutoff_year:
            maybe_print('Skipping row %d because date %s is out of range' % (i, date))
            continue
        if buy_cur == 'STR': buy_cur = 'XLM'
        if sell_cur == 'STR': sell_cur = 'XLM'
        buy_amt = float(buy_amt) if len(buy_amt) > 0 and buy_amt != '-' else 0
        sell_amt = float(sell_amt) if len(sell_amt) > 0 and sell_amt != '-' else 0
        if type == 'Trade':
            sell_price = prices.get_price(sell_cur, dt)
            buy_price = prices.get_price(buy_cur, dt)
            if buy_price is None and sell_price is None:
                print('Cannot handle row, saving %s' % row)
                failed_rows.append(row)
                continue
            if buy_price is None:
                buy_dollar = sell_price * sell_amt
                buy_price = buy_dollar / buy_amt
            else:
                buy_dollar = buy_price * buy_amt
            if sell_price is None:
                sell_dollar = buy_price * buy_amt
                sell_price = sell_dollar / sell_amt
            else:
                sell_dollar = sell_price * sell_amt

            buy_dir = 'in'
            sell_dir = 'out'
            txs.append({
                'dollar': sell_dollar,
                'direction': sell_dir,
                'price': sell_price,
                'amount': sell_amt,
                'currency': sell_cur,
                'timestamp': dt.timestamp(),
                'notes': 'gsheet order'
            })

            txs.append({
                'dollar': buy_dollar,
                'direction': buy_dir,
                'price': buy_price,
                'amount': buy_amt,
                'currency': buy_cur,
                'timestamp': dt.timestamp(),
                'notes': 'gsheet order'
            })

        elif type == 'Withdrawal':
            # not taxable event
            pass
        elif type == 'Deposit':
            # not taxable event
            pass
        elif type == 'Income' or type == 'Mining':
            buy_price = prices.get_price(buy_cur, dt)
            if buy_price is None:
                print('Cannot get price for %s on %s for Income, using 0 as price for cost_basis' % (buy_cur, dt))
                buy_price = 0
            buy_dollar = buy_price * buy_amt
            buy_dir = 'in'
            txs.append({
                'dollar': buy_dollar,
                'direction': buy_dir,
                'price': buy_price,
                'amount': buy_amt,
                'currency': buy_cur,
                'timestamp': dt.timestamp(),
                'notes': 'gsheet'
            })

        elif type == 'Spend':
            # we are not doing capital gains / losses for this one
            sell_price = prices.get_price(sell_cur, dt)
            if sell_price is None:
                print('Cannot handle row, saving %s' % row)
                failed_rows.append(row)
                continue
            sell_dollar = sell_price * sell_amt
            sell_dir = 'out'
            txs.append({
                'dollar': sell_dollar,
                'direction': sell_dir,
                'price': sell_price,
                'amount': sell_amt,
                'currency': sell_cur,
                'timestamp': dt.timestamp(),
                'notes': 'gsheet'
            })
        elif type == 'Lost':
            # fees? ocean?
            pass
        else:
            logger.warning('Unknown row type: %s ' % type)

    return txs, failed_rows


def parse_coin_tracker_custom_2(path):
    f = maybe_open(path)
    if f is None: return []
    f.__next__()
    lines = f.readlines()
    txs = []
    failed_rows = []
    for i, row in enumerate(lines):
        _, type,buy_amt,buy_cur,sell_amt,sell_cur,fee_amt, fee_cur, exchange,_,comment,date = row.split('\t')
        if 'ignore' in comment.lower():
            maybe_print('Ignoring row: %s' % row)
            continue
        date = date.replace('\n', '')
        dt = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        if dt >= cutoff_year:
            maybe_print('Skipping row %d because date %s is out of range' % (i, date))
            continue
        if buy_cur == 'STR': buy_cur = 'XLM'
        if sell_cur == 'STR': sell_cur = 'XLM'
        buy_amt = float(buy_amt) if len(buy_amt) > 0 and buy_amt != '-' else 0
        sell_amt = float(sell_amt) if len(sell_amt) > 0 and sell_amt != '-' else 0
        if type == 'Trade':
            sell_price = prices.get_price(sell_cur, dt)
            buy_price = prices.get_price(buy_cur, dt)
            if buy_price is None and sell_price is None:
                print('Cannot handle row, saving %s' % row)
                failed_rows.append(row)
                continue
            if buy_price is None:
                buy_dollar = sell_price * sell_amt
                buy_price = buy_dollar / buy_amt
            else:
                buy_dollar = buy_price * buy_amt
            if sell_price is None:
                sell_dollar = buy_price * buy_amt
                sell_price = sell_dollar / sell_amt
            else:
                sell_dollar = sell_price * sell_amt

            buy_dir = 'in'
            sell_dir = 'out'
            txs.append({
                'dollar': sell_dollar,
                'direction': sell_dir,
                'price': sell_price,
                'amount': sell_amt,
                'currency': sell_cur,
                'timestamp': dt.timestamp(),
                'notes': 'gsheet order'
            })

            txs.append({
                'dollar': buy_dollar,
                'direction': buy_dir,
                'price': buy_price,
                'amount': buy_amt,
                'currency': buy_cur,
                'timestamp': dt.timestamp(),
                'notes': 'gsheet order'
            })

        elif type == 'Withdrawal':
            # not taxable event
            pass
        elif type == 'Deposit':
            # not taxable event
            pass
        elif type == 'Income' or type == 'Mining':
            buy_price = prices.get_price(buy_cur, dt)
            if buy_price is None:
                print('Cannot get price for %s on %s for Income, using 0 as price for cost_basis' % (buy_cur, dt))
                buy_price = 0
            buy_dollar = buy_price * buy_amt
            buy_dir = 'in'
            txs.append({
                'dollar': buy_dollar,
                'direction': buy_dir,
                'price': buy_price,
                'amount': buy_amt,
                'currency': buy_cur,
                'timestamp': dt.timestamp(),
                'notes': 'gsheet'
            })

        elif type == 'Spend':
            # we are not doing capital gains / losses for this one
            sell_price = prices.get_price(sell_cur, dt)
            if sell_price is None:
                print('Cannot handle row, saving %s' % row)
                failed_rows.append(row)
                continue
            sell_dollar = sell_price * sell_amt
            sell_dir = 'out'
            txs.append({
                'dollar': sell_dollar,
                'direction': sell_dir,
                'price': sell_price,
                'amount': sell_amt,
                'currency': sell_cur,
                'timestamp': dt.timestamp(),
                'notes': 'gsheet'
            })
        elif type == 'Lost':
            # fees? ocean?
            pass
        else:
            logger.warning('Unknown row type: %s ' % type)

    return txs, failed_rows
