import time
from binance.spot import Spot


def order_size(client: Spot, unit: str = 'USDC') -> float:
    if unit == 'USDC':
        balance = client.account()['balances'][50]['free']
        if balance > 10:
            size = float(balance) * 0.1
        else:
            size = 0.0
    else:
        for asset in client.account()['balances']:
            if asset['asset'] == unit:
                balance = float(asset['free'])

                '''Hay que ajustar en base al precio del USD !!!!!'''

                if balance > 10:
                    size = balance * 0.1
                else:
                    size = 0.0
                break
    return size


def portfolio_value(client: Spot, units: str = 'USDC') -> float:
    total = 0.0
    balances = client.account(omitZeroBalances='true')['balances']
    for asset in balances:
        if asset['asset'] != units:
            try:
                total += (float(asset['free']) + float(asset['locked'])) * float(
                    client.ticker_price(symbol=f'{asset['asset']}{units}')['price'])
            except Exception as e:
                try:
                    total += (float(asset['free']) + float(asset['locked'])) * float(
                        client.ticker_price(symbol=f'{units}{asset['asset']}')['price'])
                except Exception as e:
                    print(asset['asset'])
        else:
            total += (float(asset['free']) + float(asset['locked']))
    return total


def liquidate_account(client: Spot, target_quote: str = 'USDC'):
    # Get current balances
    balances = client.account(omitZeroBalances='true')['balances']
    symbols_info = client.exchange_info()['symbols']
    valid_symbols = {s['symbol']
        : s for s in symbols_info if s['status'] == 'TRADING'}

    for asset in balances:
        asset_name = asset['asset']
        free_amount = float(asset['free'])

        # Skip quote currencies and zero balances
        if asset_name in [target_quote, 'USDT', 'USDC'] or free_amount <= 0:
            continue

        # Try to sell ASSETTARGET (e.g. BTCUSDC)
        direct_symbol = f'{asset_name}{target_quote}'
        reverse_symbol = f'{target_quote}{asset_name}'

        if direct_symbol in valid_symbols:
            try:
                client.new_order(
                    symbol=direct_symbol,
                    side='SELL',
                    type='MARKET',
                    quantity=free_amount
                )
                print(
                    f'Sold {asset_name} for {target_quote} via {direct_symbol}')
                continue
            except Exception as e:
                print(f'Error selling {direct_symbol}')

        elif reverse_symbol in valid_symbols:
            try:
                client.new_order(
                    symbol=reverse_symbol,
                    side='BUY',
                    type='MARKET',
                    quoteOrderQty=free_amount
                )
                continue
            except Exception as e:
                print(f'Error buying {reverse_symbol}')
        else:
            print(
                f'No available pair to convert {asset_name} to {target_quote}')


def check_ping(client: Spot) -> float:
    try:
        initial = time.time()
        client.ping()
        final = time.time()
        latency = (final - initial) * 1000
        print(f'Ping successful: {latency:.2f} ms')
        return latency
    except Exception as e:
        print(f'Ping failed: {e}')
