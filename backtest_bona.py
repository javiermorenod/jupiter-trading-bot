import pandas as pd
import logging
from settings import backtest
from settings.connect import binance_client, sqlalchemy_create_engine
from settings.log import start_logging

# Settings for backtest
initial_balance = 100.0
balance = 0.0
positions = {}
trade_history = []
risk_per_trade = 0.1

# Set up log
logger = start_logging('settings/bona/bona_backtest')


# Implement strategy
def search_entry_points(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Shift columns to get previous rows
    prev_1_open = df['open'].shift(1)
    prev_1_close = df['close'].shift(1)
    prev_2_open = df['open'].shift(2)
    prev_2_close = df['close'].shift(2)
    prev_3_open = df['open'].shift(3)
    prev_3_close = df['close'].shift(3)

    # Create boolean masks
    buy_mask = (prev_1_open < prev_1_close) & (
        prev_2_open < prev_2_close) & (
        prev_3_open < prev_3_close)
    # sell_mask = (prev_1_open > prev_1_close) & (prev_2_open > prev_2_close)

    # Assign signals vectorized
    df['signal'] = ''
    df.loc[buy_mask, 'signal'] = 'BUY'
    # df.loc[sell_mask, 'signal'] = 'SELL'

    return df


def close_positions(kline: pd.DataFrame, balance: float, positions: dict, trade_history: list, logger: logging.Logger):
    symbol = kline['symbol'].iloc[0]
    current_price = float(kline['close'].iloc[0])
    current_time = kline.index[0]
    for sym, pos in list(positions.items()):
        if sym != symbol:
            continue
        # Close the position
        if pos['side'] == 'LONG':
            proceeds = pos['qty'] * current_price
            profit = proceeds - pos['usd_in']
            balance += proceeds
        else:  # SHORT
            proceeds = pos['qty'] * pos['entry_price']
            current_value = pos['qty'] * current_price
            profit = proceeds - current_value
            balance += proceeds - current_value + pos['usd_in']

        logger.info(
            f"{kline.index[0]} CLOSE {pos['side']}: {sym} @ {current_price:.2f} | "f"Profit: {profit:.4f}")

        trade_history.append({
            'timestamp': current_time,
            'symbol': sym,
            'side': f'CLOSE_{pos["side"]}',
            'price': current_price,
            'profit': profit,
        })

        del positions[sym]

    return balance, positions, trade_history


# Test
def test_on_btc(initial_balance: float, balance: float, positions: dict, trade_history: list, risk_per_trade: float):
    """Run backtest on BTC/USDC pair"""
    logger.info('TESTING on BTCUSDC')

    engine = sqlalchemy_create_engine()
    symbol = 'BTCUSDC'

    # Minimal query - only get what we need
    query = f"""
    SELECT 
        open_time AS timestamp,
        open,
        close
    FROM klines
    WHERE symbol = '{symbol}'
    ORDER BY open_time ASC
    """

    try:
        df = pd.read_sql(query, engine)

        if df.empty:
            logger.error("No data for %s", symbol)
            return

        # Convert and clean data
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        df['symbol'] = symbol
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df = df.dropna(subset=['close'])
        df['signal'] = ''

        # Compute signals for entire DataFrame
        df = search_entry_points(df)

        balance = initial_balance

        # Process each candle
        for i in range(len(df)):
            kline = df.iloc[i:i+1].copy()
            if not kline['signal'].empty and kline['signal'].iloc[0] in ['BUY', 'SELL']:
                balance, positions, trade_history = close_positions(
                    kline, balance, positions, trade_history, logger)
                balance, positions, trade_history = backtest.open_position(
                    kline, balance, positions, trade_history, risk_per_trade, logger)

        # Final liquidation
        if not df.empty:
            balance, positions, trade_history = backtest.close_all_positions(
                {symbol: df}, balance, positions, trade_history, logger)

        # Calculate performance
        metrics = backtest.calculate_metrics(
            initial_balance, balance, trade_history, True, logger)

    except Exception as e:
        logger.error("Backtest failed: %s", str(e))
        raise


def test_on_all_pairs_independently(initial_balance: float, risk_per_trade: str):
    """Run backtest on all pairs independtly"""
    logger.info('TESTING on all pairs independently')

    engine = sqlalchemy_create_engine()

    # Query for symbols
    query = """
    SELECT DISTINCT symbol 
    FROM klines 
    ORDER BY symbol
    """

    try:
        symbols = (pd.read_sql(query, engine))['symbol'].tolist()

        for symbol in symbols:
            # Minimal query - only get what we need
            query = f"""
            SELECT 
                open_time AS timestamp,
                open,
                close
            FROM klines
            WHERE symbol = '{symbol}'
            ORDER BY open_time ASC
            """
            df = pd.read_sql(query, engine)

            # Convert and clean data
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            df['symbol'] = symbol
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df = df.dropna(subset=['close'])
            df['signal'] = ''

            # Compute signals for entire DataFrame
            df = search_entry_points(df)

            balance = initial_balance
            positions = {}
            trade_history = []

            # Process each candle
            for i in range(len(df)):
                kline = df.iloc[i:i+1].copy()
                if not kline['signal'].empty and kline['signal'].iloc[0] in ['BUY', 'SELL']:
                    balance, positions, trade_history = close_positions(
                        kline, balance, positions, trade_history, logger)
                    balance, positions, trade_history = backtest.open_position(
                        kline, balance, positions, trade_history, risk_per_trade, logger)

            # Final liquidation
            if not df.empty:
                balance, positions, trade_history = backtest.close_all_positions(
                    {symbol: df}, balance, positions, trade_history, logger)

            # Calculate performance
            metrics = backtest.calculate_metrics(
                initial_balance, balance, trade_history, True, logger)

    except Exception as e:
        logger.error("Backtest failed: %s", str(e))
        raise


def test_ananke(initial_balance: float, balance: float, positions: dict, trade_history: list, risk_per_trade: float):
    """Run backtest on Ananke strategy"""

    logger.info('TESTING Ananke strategy')

    engine = sqlalchemy_create_engine()

    # Query for symbols
    query = """
    SELECT DISTINCT symbol 
    FROM klines 
    ORDER BY symbol
    """

    try:
        client = binance_client()
        binance_symbols = []
        exchange_info_spot_symbols = client.exchange_info(permissions=['SPOT'])[
            'symbols']
        for symbol in exchange_info_spot_symbols:
            binance_symbols.append(symbol['symbol'])

        symbols = (pd.read_sql(query, engine))['symbol'].tolist()
        logger.info('List of symbols obtained')

        dfs = {}
        for symbol in symbols:
            # Minimal query - only get what we need
            query = f"""
            SELECT 
                open_time AS timestamp,
                open,
                close
            FROM klines
            WHERE symbol = '{symbol}'
            ORDER BY open_time ASC
            """
            df = pd.read_sql(query, engine)

            # Convert and clean data
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            df['symbol'] = symbol
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df = df.dropna(subset=['close'])
            df['signal'] = ''

            # Compute signals for entire DataFrame
            df = search_entry_points(df)

            dfs[symbol] = df

        # Get all unique timestamps
        timeline = backtest.create_unified_timeline(dfs)

        logger.info("Starting portfolio backtest with %d symbols and %d timestamps",
                    len(dfs), len(timeline))

        balance = initial_balance

        # Main backtest loop
        for i, timestamp in enumerate(timeline):
            # Manage existing positions
            for symbol in list(positions.keys()):
                if symbol in dfs and timestamp in dfs[symbol].index:
                    kline = dfs[symbol].loc[[timestamp]]
                    balance, positions, trade_history = close_positions(
                        kline, balance, positions, trade_history, logger)

            # Check for new signals in Binance order
            for symbol in binance_symbols:
                if symbol in dfs and timestamp in dfs[symbol].index:
                    kline = dfs[symbol].loc[[timestamp]]
                    if kline['signal'].iloc[0] in ['BUY', 'SELL'] and symbol not in positions:
                        balance, positions, trade_history = backtest.open_position(
                            kline, balance, positions, trade_history, risk_per_trade, logger)

            # Progress logging
            if i % 1000 == 0:
                logger.info("Processed %d/%d timestamps", i, len(timeline))

        # Final liquidation
        balance, positions, trade_history = backtest.close_all_positions(
            dfs, balance, positions, trade_history, logger)

        # Calculate performance
        metrics = backtest.calculate_metrics(
            initial_balance, balance, trade_history, True, logger)

    except Exception as e:
        logger.error("Backtest failed: %s", str(e))
        raise


if __name__ == '__main__':
    # test_on_btc(initial_balance, balance, positions,
    #             trade_history, risk_per_trade)
    # test_on_all_pairs_independently(
    #     initial_balance, risk_per_trade)
    # test_ananke(initial_balance, balance, positions,
    #             trade_history, risk_per_trade)
    pass
