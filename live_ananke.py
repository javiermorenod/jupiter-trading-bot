import pandas as pd
import logging
from binance.spot import Spot
import settings.indicators
from settings.log import log_message
from settings.risk import order_size


unit = 'USDC'


def parse_klines(klines):
    """Parse Binance klines to DataFrame"""
    df = pd.DataFrame(
        klines,
        columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_volume', 'trades',
            'taker_buy_base', 'taker_buy_quote', 'ignore'
        ]
    )
    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df.set_index('timestamp')


def search_entry_point(klines):
    df = parse_klines(klines)
    rsi = settings.indicators.rsi(df['close'].values)[-1]
    macd = settings.indicators.macd(df['close'].values)
    if (rsi < 30) and (macd['macd_line'][-1] > macd['signal_line'][-1]) and (macd['macd_line'][-2] <= macd['signal_line'][-2]):
        df['signal'] = 'BUY'
    elif (rsi > 70) and (macd['macd_line'][-1] < macd['signal_line'][-1]) and (macd['macd_line'][-2] >= macd['signal_line'][-2]):
        df['signal'] = 'SELL'
    else:
        df['signal'] = ''
    return df


def open_position(client: Spot, df: pd.DataFrame, logger: logging.Logger):
    signal = df['signal'].iloc[-1]
    symbol = df['symbol'].iloc[-1]
    # Open new position
    try:
        if signal == 'BUY' and symbol.endswith(unit):
            size = order_size(client)
            # Execute buy order
            client.new_order(
                symbol=symbol,
                side='BUY',
                type='MARKET',
                quoteOrderQty=size
            )
            # Set stop loss
            client.new_order(
                symbol=symbol,
                side='SELL',
                type='STOP_LOSS',
                stopPrice=df['close'].iloc[-1] * 0.9,
                quantity=size
            )
        elif signal == 'SELL' and symbol.startswith(unit):
            size = order_size(client)
            # Execute sell order
            client.new_order(
                symbol=symbol,
                side='SELL',
                type='MARKET',
                quantity=size
            )
            # Set stop loss
            client.new_order(
                symbol=symbol,
                side='BUY',
                type='STOP_LOSS',
                stopPrice=df['close'].iloc[-1] * 1.1,
                quoteOrderQty=size
            )
        # Close position
        elif signal == 'SELL' and symbol.endswith(unit):
            for asset in client.account()['balances']:
                if asset['asset'] == unit:
                    balance = float(asset['free'])
                    break
            if balance > 0:
                client.new_order(
                    symbol=symbol,
                    side='SELL',
                    type='MARKET',
                    quantity=balance
                )
        elif signal == 'BUY' and symbol.startswith(unit):
            for asset in client.account()['balances']:
                if asset['asset'] == unit:
                    balance = float(asset['free'])
                    break
            if balance > 0:
                client.new_order(
                    symbol=symbol,
                    side='BUY',
                    type='MARKET',
                    quoteOrderQty=balance
                )
        else:
            'ok'
    except Exception as e:
        log_message(logger, 'error',
                    f'Error opening position for {symbol}: {e}')


def execute_ananke(client: Spot, logger: logging.Logger):
    # Get exchange information for SPOT trading
    try:
        exchange_info_spot = client.exchange_info(permissions=['SPOT'])

        # Filter for USDC pairs that are trading
        for symbol in exchange_info_spot['symbols']:
            if (symbol['quoteAsset'] == unit or symbol['baseAsset'] == unit) and symbol['status'] == 'TRADING':
                klines = client.klines(
                    symbol=symbol['symbol'],
                    interval='5m'
                )
                df = search_entry_point(klines)
                signal = df['signal'].iloc[-1]
                if signal in ['BUY', 'SELL']:
                    log_message(logger, 'info',
                                f'{signal} signal detected for {symbol["symbol"]}')
                    open_position(client, klines, logger)
    except Exception as e:
        log_message(logger, 'error', f'Error executing Ananke strategy: {e}')
