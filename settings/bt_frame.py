import pandas as pd
import logging
import math


def open_position(kline: pd.DataFrame, balance: float, positions: dict, trade_history: list, risk_per_trade: float, logger: logging.Logger):
    signal = kline['signal'].iat[0]
    symbol = kline['symbol'].iat[0]
    price = float(kline['close'].iat[0])
    timestamp = kline.index[0]

    # OPEN LONG POSITION
    if signal == 'BUY' and symbol not in positions:
        max_invest = risk_per_trade * balance
        if max_invest < 0.01:  # Minimum trade size check
            return balance, positions, trade_history

        qty = max_invest / price
        positions[symbol] = {
            'side': 'LONG',
            'usd_in': max_invest,
            'qty': qty,
            'entry_price': price,
            'entry_time': timestamp
        }
        balance -= max_invest
        logger.info(
            f"{timestamp} OPEN LONG: {symbol} @ {price:.2f} | "
            f"Invested={max_invest:.4f} qty={qty:.8f} balance={balance:.4f}")

        trade_history.append({
            'timestamp': timestamp,
            'symbol': symbol,
            'side': 'OPEN_LONG',
            'price': price,
            'qty': qty,
            'usd_flow': -max_invest,
            'balance': balance
        })

    # OPEN SHORT POSITION
    elif signal == 'SELL' and symbol not in positions:
        max_invest = risk_per_trade * balance
        if max_invest < 0.01:  # Minimum trade size check
            return balance, positions, trade_history

        qty = max_invest / price
        positions[symbol] = {
            'side': 'SHORT',
            'usd_in': max_invest,
            'qty': qty,
            'entry_price': price,
            'entry_time': timestamp
        }
        balance -= max_invest
        logger.info(
            f"{timestamp} OPEN SHORT: {symbol} @ {price:.2f} | "
            f"Invested={max_invest:.4f} qty={qty:.8f} balance={balance:.4f}")

        trade_history.append({
            'timestamp': timestamp,
            'symbol': symbol,
            'side': 'OPEN_SHORT',
            'price': price,
            'qty': qty,
            'usd_flow': -max_invest,
            'balance': balance
        })

    return balance, positions, trade_history


def should_close_position(position, current_time, max_hold_hours=24):
    """Close position after maximum hold time"""
    hold_time_hours = (
        current_time - position['entry_time']).total_seconds() / 3600
    return hold_time_hours >= max_hold_hours


def trailing_stop_loss(position, current_price, trail_percent=0.15):
    """Trailing stop loss that follows price upward"""
    if position['side'] == 'LONG':
        # Update highest price seen
        if 'highest_price' not in position:
            position['highest_price'] = current_price
        else:
            position['highest_price'] = max(
                position['highest_price'], current_price)

        # Check if current price is below trail threshold
        stop_price = position['highest_price'] * (1 - trail_percent)
        return current_price <= stop_price

    elif position['side'] == 'SHORT':
        # Update lowest price seen
        if 'lowest_price' not in position:
            position['lowest_price'] = current_price
        else:
            position['lowest_price'] = min(
                position['lowest_price'], current_price)

        # Check if current price is above trail threshold
        stop_price = position['lowest_price'] * (1 + trail_percent)
        return current_price >= stop_price


def take_profit_target(position, current_price, profit_percent=0.20):
    """Close position when reaching profit target"""
    if position['side'] == 'LONG':
        profit_pct = (current_price -
                      position['entry_price']) / position['entry_price']
        return profit_pct >= profit_percent

    elif position['side'] == 'SHORT':
        profit_pct = (position['entry_price'] -
                      current_price) / position['entry_price']
        return profit_pct >= profit_percent


def opposite_signal_exit(position, current_signal):
    """Close position when opposite signal appears"""
    if position['side'] == 'LONG' and current_signal == 'SELL':
        return True
    elif position['side'] == 'SHORT' and current_signal == 'BUY':
        return True
    return False


def volatility_stop(position, current_price, df, atr_period=14, atr_multiplier=2):
    """Stop loss based on volatility (ATR)"""
    # Calculate ATR (Average True Range)
    high_low = df['high'] - df['low']
    high_close = abs(df['high'] - df['close'].shift(1))
    low_close = abs(df['low'] - df['close'].shift(1))
    true_range = pd.concat(
        [high_low, high_close, low_close], axis=1).max(axis=1)
    atr = true_range.rolling(atr_period).mean().iloc[-1]

    if position['side'] == 'LONG':
        stop_price = position['entry_price'] - (atr * atr_multiplier)
        return current_price <= stop_price

    elif position['side'] == 'SHORT':
        stop_price = position['entry_price'] + (atr * atr_multiplier)
        return current_price >= stop_price


def manage_positions(kline: pd.DataFrame, balance: float, positions: dict, trade_history: list, logger: logging.Logger):
    """Manage all open positions for current candle"""

    symbol = kline['symbol'].iloc[0]
    current_price = float(kline['close'].iloc[0])
    current_time = kline.index[0]
    current_signal = kline['signal'].iloc[0]

    for sym, pos in list(positions.items()):
        if sym != symbol:
            continue

        close_position = False
        close_reason = ""

        # 1. Opposite signal exit
        if opposite_signal_exit(pos, current_signal):
            close_position = True
            close_reason = "OPPOSITE_SIGNAL"

        # 2. Take profit (20%)
        elif take_profit_target(pos, current_price, 0.20):
            close_position = True
            close_reason = "TAKE_PROFIT"

        # 3. Trailing stop loss (15%)
        elif trailing_stop_loss(pos, current_price, 0.15):
            close_position = True
            close_reason = "TRAILING_STOP"

        # 4. Time-based exit (48 hours max)
        elif should_close_position(pos, current_time, 48):
            close_position = True
            close_reason = "TIME_EXIT"

        if close_position:
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

            logger.info(f"{kline.index[0]} CLOSE {pos['side']}: {sym} @ {current_price:.2f} | "
                        f"Profit: {profit:.4f} ({close_reason})")

            trade_history.append({
                'timestamp': current_time,
                'symbol': sym,
                'side': f'CLOSE_{pos["side"]}',
                'price': current_price,
                'profit': profit,
                'reason': close_reason
            })

            del positions[sym]

    return balance, positions, trade_history


def close_all_positions(
    dfs: dict[str, pd.DataFrame],
    balance: float,
    positions: dict,
    trade_history: list,
    logger: logging.Logger
):
    """Force-close all open positions at last known price (per symbol in dfs)."""

    if not positions:
        return balance, positions, trade_history

    for sym, pos in list(positions.items()):
        if sym not in dfs or dfs[sym].empty:
            logger.warning(f"No data found for {sym}, skipping force-close.")
            continue

        # Get last available row for this symbol
        last_row = dfs[sym].iloc[-1]
        price = float(last_row['close'])
        timestamp = last_row.name

        if pos['side'] == 'LONG':
            qty = pos['qty']
            proceeds = qty * price
            profit = proceeds - pos['usd_in']
            balance += proceeds
            logger.info(
                f"{timestamp} FORCE CLOSE LONG: {sym} @ {price:.2f} | "
                f"Profit={profit:.4f} balance={balance:.4f}"
            )

            trade_history.append({
                'timestamp': timestamp,
                'symbol': sym,
                'side': 'FORCE_LONG',
                'price': price,
                'qty': qty,
                'usd_flow': proceeds,
                'profit': profit,
                'balance': balance
            })

        elif pos['side'] == 'SHORT':
            qty = pos['qty']
            proceeds = qty * pos['entry_price']
            current_value = qty * price
            profit = proceeds - current_value
            balance += proceeds - current_value + pos['usd_in']
            logger.info(
                f"{timestamp} FORCE CLOSE SHORT: {sym} @ {price:.2f} | "
                f"Profit={profit:.4f} balance={balance:.4f}"
            )

            trade_history.append({
                'timestamp': timestamp,
                'symbol': sym,
                'side': 'FORCE_SHORT',
                'price': price,
                'qty': qty,
                'usd_flow': proceeds - current_value,
                'profit': profit,
                'balance': balance
            })

        # Remove closed position
        del positions[sym]

    return balance, positions, trade_history


def calculate_metrics(initial_balance: float, balance: float, trade_history: list, log_metrics: bool, logger: logging.Logger) -> dict:
    """Calculate performance metrics from trade history with optional logging"""
    # Initialize metrics with default values
    metrics = {
        'final_balance': balance,
        'total_return': (balance - initial_balance) / initial_balance * 100,
        'win_rate': 0.0,
        'profit_factor': 0.0,
        'max_drawdown': 0.0,
        'num_trades': 0,
        'gross_profit': 0.0,
        'gross_loss': 0.0,
        'avg_win': 0.0,
        'avg_loss': 0.0,
        'largest_win': 0.0,
        'largest_loss': 0.0
    }

    if not trade_history:
        if log_metrics:
            logger.info("No trade history available for metrics calculation")
        return metrics

    try:
        df_trades = pd.DataFrame(trade_history)
        closed_trades = df_trades.dropna(subset=['profit'])

        if closed_trades.empty:
            if log_metrics:
                logger.info(
                    "No closed trades available for metrics calculation")
            return metrics

        metrics['num_trades'] = len(closed_trades)

        # Win rate
        winning_trades = closed_trades[closed_trades['profit'] > 0]
        losing_trades = closed_trades[closed_trades['profit'] <= 0]

        metrics['win_rate'] = len(winning_trades) / metrics['num_trades'] * 100

        # Profit factor
        metrics['gross_profit'] = winning_trades['profit'].sum()
        metrics['gross_loss'] = abs(losing_trades['profit'].sum())
        metrics['profit_factor'] = metrics['gross_profit'] / \
            metrics['gross_loss'] if metrics['gross_loss'] > 0 else math.inf

        # Average win/loss
        metrics['avg_win'] = winning_trades['profit'].mean(
        ) if not winning_trades.empty else 0
        metrics['avg_loss'] = losing_trades['profit'].mean(
        ) if not losing_trades.empty else 0

        # Largest win/loss
        metrics['largest_win'] = winning_trades['profit'].max(
        ) if not winning_trades.empty else 0
        metrics['largest_loss'] = losing_trades['profit'].min(
        ) if not losing_trades.empty else 0

        # Max drawdown
        df_trades['cumulative'] = df_trades['profit'].fillna(
            0).cumsum() + initial_balance
        df_trades['peak'] = df_trades['cumulative'].cummax()
        df_trades['drawdown'] = (
            df_trades['cumulative'] - df_trades['peak']) / df_trades['peak']
        metrics['max_drawdown'] = df_trades['drawdown'].min() * 100

        # Additional metrics if available
        if 'side' in df_trades.columns:
            metrics['long_trades'] = len(
                df_trades[df_trades['side'].str.contains('LONG', na=False)])
            metrics['short_trades'] = len(
                df_trades[df_trades['side'].str.contains('SHORT', na=False)])

        if 'entry_time' in df_trades.columns and 'exit_time' in df_trades.columns:
            df_trades['duration'] = (
                df_trades['exit_time'] - df_trades['entry_time']).dt.total_seconds() / 3600
            metrics['avg_trade_duration'] = df_trades['duration'].mean()

        # Log metrics if requested
        if log_metrics:
            logger.info("PERFORMANCE METRICS:")
            logger.info("Initial Balance: %.4f", initial_balance)
            logger.info("Final Balance: %.4f", metrics['final_balance'])
            logger.info("Total Return: %.2f%%", metrics['total_return'])
            logger.info("Number of Trades: %d", metrics['num_trades'])
            logger.info("Win Rate: %.2f%%", metrics['win_rate'])
            logger.info("Profit Factor: %.2f", metrics['profit_factor'])
            logger.info("Max Drawdown: %.2f%%", metrics['max_drawdown'])
            logger.info("Gross Profit: %.4f", metrics['gross_profit'])
            logger.info("Gross Loss: %.4f", metrics['gross_loss'])
            logger.info("Average Win: %.4f", metrics['avg_win'])
            logger.info("Average Loss: %.4f", metrics['avg_loss'])
            logger.info("Largest Win: %.4f", metrics['largest_win'])
            logger.info("Largest Loss: %.4f", metrics['largest_loss'])

            if 'long_trades' in metrics:
                logger.info("Long Trades: %d", metrics['long_trades'])
                logger.info("Short Trades: %d", metrics['short_trades'])

            if 'avg_trade_duration' in metrics:
                logger.info("Avg Trade Duration: %.2f hours",
                            metrics['avg_trade_duration'])

    except Exception as e:
        logger.error("Error calculating metrics: %s", str(e))
        if log_metrics:
            logger.error("Failed to generate complete metrics report")

    return metrics


def create_unified_timeline(pairs_data: dict):
    """Create a master timeline from all pairs"""
    all_timestamps = set()
    for symbol, df in pairs_data.items():
        all_timestamps.update(df.index)
    return sorted(all_timestamps)
