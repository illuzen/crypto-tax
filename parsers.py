
import datetime
import prices
import logging
import json
import os
from config import *
from pprint import pprint

logging.basicConfig(filename='logs/all.log',level=logging.DEBUG)


def parse_ethplorer(path):
    logging.info('parsing file %s' % path)
    txs = []

    f = open(path, 'r')
    row_num = 0
    frum1,frum2,to1,to2 = 0,0,0,0
    for row in f:
        if row_num == 0:
            row_num += 1
            continue
        date,txhash,frum,to,token_name,token_address,quantity,symbol = row.split(';')
        if row_num == 1:
            frum1 = frum
            to1 = to
        elif row_num == 2:
            frum2 = frum
            to2 = to
        else:
            break
        row_num += 1

    if frum1 == to2 or frum1 == frum2:
        my_address = frum1
    elif to1 == frum2 or to1 == to2:
        my_address = to1
    else:
        assert(row_num == 2)
        my_address = to1

    f = open(path, 'r')
    first_row = True
    my_address
    for row in f:
        if first_row:
            first_row = False
            continue
        date,txhash,frum,to,token_name,token_address,quantity,symbol = row.split(';')
        symbol = symbol.replace('\n', '')
        direction = 'in' if to == my_address else 'out'
        quantity = float(quantity)
        dt = date.split(' ')[0].split('-')
        dt.extend(date.split(' ')[1].split(':'))
        dt = [int(x) for x in dt]
        dt = datetime.datetime(*dt)
        try:
            price = prices.get_price(symbol, dt)
            dollar = quantity * price
        except Exception as e:
            logging.warn(e)
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


def parse_etherscan(path):
    logging.info('parsing file %s' % path)
    f = open(path, 'r')
    first_row = True
    txs = []
    type_branch = True
    for row in f:
        cols = row.split(',')
        if first_row:
            first_row = False
            if 'Type' in cols[-1]:
                type_branch = True
#                txhash,blockno,unix_timestamp,date_time,frum,to,contract_address,value_in,value_out,current_value,fee_eth,fee_usd,historical_price,status,err_code = cols
            elif 'ErrCode' in cols[-1]:
                type_branch = False
#                txhash,blockno,unix_timestamp,date_time,frum,to,contract_address,value_in,value_out,current_value,historical_price,status,err_code,taip = cols
            else:
                raise Exception('Unexpected column format for csv at %s' % (path))
            continue
        if type_branch:
            txhash,blockno,unix_timestamp,date_time,frum,to,contract_address,value_in,value_out,current_value,historical_price,status,err_code,taip = cols
        else:
            txhash,blockno,unix_timestamp,date_time,frum,to,contract_address,value_in,value_out,current_value,fee_eth,fee_usd,historical_price,status,err_code = cols

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

        try:
            out_price = prices.get_price(out_currency, dt)
            out_dollar = out_quantity * out_price
            in_price = prices.get_price(in_currency, dt)
            in_dollar = in_quantity * in_price
        except Exception as e:
            logging.warn(e)
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
            logging.warn(e)
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
            logging.warn(e)
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

        # determine orientation
        if 'Buy' in order_type:
            in_currency, out_currency = exchange.split('/')[:2]
            out_quantity = abs(float(base_total_less_fee))
            in_quantity = abs(float(quote_total_less_fee))

        elif 'Sell' in order_type:
            out_currency, in_currency = exchange.split('/')[:2]
            in_quantity = abs(float(base_total_less_fee))
            out_quantity = abs(float(quote_total_less_fee))

        try:
            out_price = prices.get_price(out_currency, dt)
            out_dollar = out_quantity * out_price
            in_price = prices.get_price(in_currency, dt)
            in_dollar = in_quantity * in_price
        except Exception as e:
            print('Dropping poloniex order', row)
            logging.warn(e)
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
            logging.warn(e)
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
            logging.warn(e)
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
            logging.warn(e)
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
            logging.warn(e)
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


def parse_trezor(path):
    f = maybe_open(path)
    if f is None: return []
    currency = path.split('/')[-1].split('_')[0]
    first_row = True
    txs = []
    for row in f:
        if first_row:
            first_row = False
            continue
        date,time,tx_id,address,direction,amount,total,fee,balance = row.split(',')
        direction = direction.lower()
        amount = abs(float(total))
        dt = date.split('-')
        dt.extend(time.split(':'))
        dt = [int(x) for x in dt]
        dt = datetime.datetime(*dt)

        try:
            price = prices.get_price(currency, dt)
        except Exception as e:
            logging.warn(e)
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
            logging.info('Ignoring tx type %s' % taip)
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
            logging.warn(e)
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
        type,buy_amt,buy_cur,sell_amt,sell_cur,fee_amt,fee_cur,exchange,_,_,date = row.split('\t')
        date = date.replace('\n', '')
        dt = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        if dt > cutoff_year:
            maybe_print('Skipping row %d because date %s is out of range' % (i, date))
            continue
        if buy_cur == 'STR': buy_cur = 'XLM'
        if sell_cur == 'STR': sell_cur = 'XLM'
        if fee_cur == 'STR': fee_cur = 'XLM'
        buy_amt = float(buy_amt) if len(buy_amt) > 0 else 0
        sell_amt = float(sell_amt) if len(sell_amt) > 0 else 0
        fee_amt = float(fee_amt) if len(fee_amt) > 0 else 0
        if exchange != 'Poloniex':
            if fee_cur == buy_cur:
                buy_amt -= fee_amt
            if fee_cur == sell_cur:
                sell_amt -= fee_amt
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

    return txs, failed_rows
