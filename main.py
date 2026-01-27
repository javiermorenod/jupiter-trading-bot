from settings.connect import binance_client
from settings.log import start_logging, log_message
from live_ananke import execute_ananke
import time


# Initialize logging
logger = start_logging('settings/jupiter')
log_message(logger, 'info', '   INITIALIZING JUPITER:')


# Initialize Binance client
client = binance_client(testnet=True)


while True:
    # Ping the Binance API to check latency
    start_time = time.time()
    client.ping()
    latency_ms = (time.time() - start_time) * 1000
    log_message(logger, 'info',
                f'Iteration starting with {round(latency_ms, 2)} ms latency.')

    try:
        # Execute the ananke strategy
        execute_ananke(client, logger)
        log_message(logger, 'info', 'Iteration executed successfully.')
    except Exception as e:
        log_message(logger, 'error',
                    f'Error executing ananke strategy: {e}')
        break

    # Ensure at least 5 minutes between iterations
    finish_time = time.time()
    elapsed_time = finish_time - start_time
    log_message(logger, 'info', f'Elapsed time: {elapsed_time:.2f} seconds.')
    time.sleep(max(0, 300 - elapsed_time))
