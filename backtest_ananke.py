import pandas as pd
import numpy as np
from settings import indicators
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
logger = start_logging('settings/strategies/ananke_backtest')


def search_entry_point(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate trading signals using RSI and MACD with your specific indicators"""
    df = df.copy()

    # Get close prices as numpy array
    close_prices = pd.to_numeric(df['close'], errors='coerce').values

    # Calculate indicators for entire series
    rsi_values = indicators.rsi(close_prices, window=14)
    macd_data = indicators.macd(close_prices)

    # Extract MACD components
    macd_line = macd_data['macd_line']
    signal_line = macd_data['signal_line']

    # Calculate MACD crossovers
    macd_above = macd_line > signal_line
    macd_cross_up = np.zeros_like(macd_above, dtype=bool)
    macd_cross_down = np.zeros_like(macd_above, dtype=bool)

    # Manual shift for crossover detection (since we're using numpy arrays)
    for i in range(1, len(macd_above)):
        macd_cross_up[i] = macd_above[i] and not macd_above[i-1]
        macd_cross_down[i] = not macd_above[i] and macd_above[i-1]

    # Generate signals
    for i in range(len(df)):
        # Skip if indicators haven't warmed up yet
        if (np.isnan(rsi_values[i]) or
            np.isnan(macd_line[i]) or
            np.isnan(signal_line[i]) or
                i < 1):  # Need at least 2 periods for crossover detection
            continue

        # Buy signal: RSI < 30 + MACD crossover up
        if (rsi_values[i] < 30 and
            macd_cross_up[i] and
                not np.isnan(rsi_values[i])):
            df.loc[df.index[i], 'signal'] = 'BUY'

        # Sell signal: RSI > 70 + MACD crossover down
        elif (rsi_values[i] > 70 and
              macd_cross_down[i] and
              not np.isnan(rsi_values[i])):
            df.loc[df.index[i], 'signal'] = 'SELL'

        # Keep existing signal if no new signal
        elif pd.isna(df.loc[df.index[i], 'signal']) or df.loc[df.index[i], 'signal'] == '':
            df.loc[df.index[i], 'signal'] = ''

    return df


def test_on_btc(initial_balance: float, balance: float, positions: dict, trade_history: list, risk_per_trade: float):
    """Run backtest on BTC/USDC pair"""
    logger.info('TESTING on BTCUSDC')

    engine = sqlalchemy_create_engine()
    symbol = 'BTCUSDC'

    # Minimal query - only get what we need
    query = f"""
    SELECT 
        open_time AS timestamp,
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
        df = search_entry_point(df)

        balance = initial_balance

        # Process each candle
        for i in range(len(df)):
            kline = df.iloc[i:i+1].copy()
            if not kline['signal'].empty and kline['signal'].iloc[0] in ['BUY', 'SELL']:
                balance, positions, trade_history = backtest.open_position(
                    kline, balance, positions, trade_history, risk_per_trade, logger)
                balance, positions, trade_history = backtest.manage_positions(
                    kline, balance, positions, trade_history, logger)

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
            df = search_entry_point(df)

            balance = initial_balance
            positions = {}
            trade_history = []

            # Process each candle
            for i in range(len(df)):
                kline = df.iloc[i:i+1].copy()
                if not kline['signal'].empty and kline['signal'].iloc[0] in ['BUY', 'SELL']:
                    balance, positions, trade_history = backtest.open_position(
                        kline, balance, positions, trade_history, risk_per_trade, logger)
                    balance, positions, trade_history = backtest.manage_positions(
                        kline, balance, positions, trade_history, logger)

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
            df = search_entry_point(df)

            dfs[symbol] = df

        # Get all unique timestamps
        timeline = backtest.create_unified_timeline(dfs)

        logger.info("Starting portfolio backtest with %d symbols and %d timestamps",
                    len(dfs), len(timeline))

        balance = initial_balance

        # Main backtest loop
        for i, timestamp in enumerate(timeline):
            # Check for new signals in Binance order
            for symbol in binance_symbols:
                if symbol in dfs and timestamp in dfs[symbol].index:
                    kline = dfs[symbol].loc[[timestamp]]
                    if kline['signal'].iloc[0] in ['BUY', 'SELL'] and symbol not in positions:
                        balance, positions, trade_history = backtest.open_position(
                            kline, balance, positions, trade_history, risk_per_trade, logger)

            # Manage existing positions
            for symbol in list(positions.keys()):
                if symbol in dfs and timestamp in dfs[symbol].index:
                    kline = dfs[symbol].loc[[timestamp]]
                    balance, positions, trade_history = backtest.manage_positions(
                        kline, balance, positions, trade_history, logger)

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
    test_ananke(initial_balance, balance, positions,
                trade_history, risk_per_trade)
    pass
