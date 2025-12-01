import numpy as np
from typing import Dict


def rsi(prices: np.ndarray, window: int = 14) -> np.ndarray:
    """Vectorized RSI calculation"""
    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)

    avg_gain = np.zeros_like(prices)
    avg_loss = np.zeros_like(prices)
    avg_gain[window] = gains[:window].mean()
    avg_loss[window] = losses[:window].mean()

    for i in range(window + 1, len(prices)):
        avg_gain[i] = (avg_gain[i-1] * (window - 1) + gains[i-1]) / window
        avg_loss[i] = (avg_loss[i-1] * (window - 1) + losses[i-1]) / window

    with np.errstate(divide='ignore', invalid='ignore'):
        rs = np.divide(avg_gain,
                       avg_loss,
                       out=np.ones_like(avg_gain),
                       where=avg_loss != 0)
        rsi = 100 - (100 / (1 + rs))

    rsi[:window] = np.nan
    return rsi


def ema(prices: np.ndarray, period: int) -> np.ndarray:
    """Vectorized EMA calculation"""
    if len(prices) < period:
        return np.full_like(prices, np.nan)

    ema = np.full_like(prices, np.nan)
    multiplier = 2 / (period + 1)
    ema[period-1] = prices[:period].mean()

    for i in range(period, len(prices)):
        ema[i] = (prices[i] - ema[i-1]) * multiplier + ema[i-1]

    return ema


def macd(prices: np.ndarray, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> Dict[str, np.ndarray]:
    fast_ema = ema(prices, fast_period)
    slow_ema = ema(prices, slow_period)
    macd_line = fast_ema - slow_ema

    # Find the first non-NaN index in macd_line
    first_valid_idx = np.where(~np.isnan(macd_line))[0]
    if len(first_valid_idx) == 0:
        return {'macd_line': macd_line, 'signal_line': macd_line, 'histogram': macd_line}

    first_valid_idx = first_valid_idx[0]

    # Calculate signal line only on valid macd values
    valid_macd = macd_line[first_valid_idx:]
    signal_line_full = np.full_like(macd_line, np.nan)

    if len(valid_macd) >= signal_period:
        signal_line_valid = ema(valid_macd, signal_period)
        signal_line_full[first_valid_idx + signal_period -
                         1:] = signal_line_valid[signal_period - 1:]

    return {
        'macd_line': macd_line,
        'signal_line': signal_line_full,
        'histogram': macd_line - signal_line_full
    }
