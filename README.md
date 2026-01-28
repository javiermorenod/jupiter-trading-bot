# Jupiter Trading Bot

**Automated Trading System with Risk Management & Backtesting.**

This repository contains a Python-based trading bot designed for automated trading on **Binance**. It features a live execution engine using Binance API keys and a robust backtesting module that utilizes a **MySQL** database for historical data management.

---

## Features

* **Automated Trading**: Execute trades automatically on Binance based on predefined strategies.
* **Risk Management**: Integrated logic to handle position sizing, stop-losses, and take-profits to protect capital.
* **MySQL Backtesting**: dedicated backtesting engine that queries a MySQL database for historical market data to simulate and verify strategy performance.
* **Modular Configuration**: Easy-to-adjust settings located in the `settings/` directory for API keys and database credentials.

## Project Structure

* `main.py`: The entry point for the live trading bot. Handles the connection to the Binance API and executes the trading logic.
* `settings/`: Directory containing configuration files.

## Prerequisites

* **Python 3.8+**
* **Binance Account**: You will need an API Key and Secret Key with trading permissions enabled.
* **MySQL Database**: A running MySQL instance to store and retrieve historical price data for backtesting.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/javiermorenod/jupiter-trading-bot.git](https://github.com/javiermorenod/jupiter-trading-bot.git)
    cd jupiter-trading-bot
    ```

2.  **Install dependencies:**
    *(Note: Ensure you have a virtual environment active)*
    ```bash
    pip install -r requirements.txt
    ```
    *Common libraries likely required:*
    ```bash
    pip install python-binance mysql-connector-python pandas numpy
    ```

3.  **Configuration:**
    * Navigate to the `settings/` folder.
    * Configure your credentials (usually in a `.json` or `.py` file within this folder):
        * **Binance**: Add your `API_KEY` and `API_SECRET`.
        * **Database**: Add your MySQL `HOST`, `USER`, `PASSWORD`, and `DATABASE_NAME`.
    * **Security Warning:** Never commit your API keys or database passwords to GitHub. Ensure your config files are added to `.gitignore`.
