import pandas as pd
import matplotlib.pyplot as plt

def backtest(
    orders_df: pd.DataFrame,
    price_df: pd.DataFrame,
    initial_capital: float = 1_000_000
) -> pd.DataFrame:
    """
    Run a simple backtest given a set of orders and daily prices.

    Parameters
    ----------
    orders_df : pd.DataFrame
        Must contain columns:
          - 'date' (Timestamp or string in YYYY-MM-DD)
          - 'deal_id' (an identifier corresponding to the deal_index from your CSV)
          - 'price_type' (e.g. 'target' or 'acquirer')
          - 'shares' (number of shares to buy (>0) or sell (<0))

    price_df : pd.DataFrame
        Must contain columns:
          - 'date'
          - 'deal_id'
          - 'price'  (daily close price)
          - 'price_type' (e.g. 'target' or 'acquirer')
        It is assumed that you have concatenated the Target_Prices.csv and Acquirer_Prices.csv,
        and that duplicate entries for the same (date, deal_id, price_type) will be averaged.

    initial_capital : float
        Starting cash amount, defaults to 1,000,000.

    Returns
    -------
    portfolio_values_df : pd.DataFrame
        Index: date (daily)
        Columns: 
            'value' - the end-of-day portfolio value in USD,
            'invested_capital' - total value of open positions,
            'holdings' - dictionary of positions { (deal_id, price_type): shares }.
    """

    # Ensure 'date' is datetime
    orders_df['date'] = pd.to_datetime(orders_df['date'])
    price_df['date'] = pd.to_datetime(price_df['date'])

    # If needed, rename columns for consistency:
    if 'prc' in price_df.columns:
        price_df.rename(columns={'prc': 'price'}, inplace=True)
    if 'deal_index' in price_df.columns:
        price_df.rename(columns={'deal_index': 'deal_id'}, inplace=True)
    
    # Group by date, deal_id, and price_type to average duplicate entries
    price_df = price_df.groupby(['date', 'deal_id', 'price_type'], as_index=False).agg({'price': 'mean'})
    
    # Create a lookup for fast access: key = (date, deal_id, price_type)
    price_lookup = price_df.set_index(['date', 'deal_id', 'price_type'])['price']

    # Sort orders by date
    orders_df = orders_df.sort_values(by='date')
    all_dates = sorted(price_df['date'].unique())

    # Track positions using keys (deal_id, price_type)
    positions = {}
    cash = initial_capital
    portfolio_history = []

    # Group orders by date for faster processing
    orders_by_date = orders_df.groupby('date')

    for current_date in all_dates:
        # 1) Execute any orders for current_date
        if current_date in orders_by_date.groups:
            daily_orders = orders_by_date.get_group(current_date)
            for _, row in daily_orders.iterrows():
                deal_id = row['deal_id']
                price_type = row['price_type']
                shares_to_trade = row['shares']
                key = (current_date, deal_id, price_type)
                if key in price_lookup:
                    current_price = float(price_lookup.loc[key])
                else:
                    raise ValueError(f"Price not found for date {current_date}, deal_id {deal_id}, price_type {price_type}")
                
                order_cost = current_price * shares_to_trade
                positions[(deal_id, price_type)] = positions.get((deal_id, price_type), 0) + shares_to_trade
                cash -= order_cost

        # 2) Compute daily portfolio value: cash + sum(positions * today's price)
        invested_capital = 0
        for (deal_id, price_type), shares_owned in positions.items():
            key = (current_date, deal_id, price_type)
            if key in price_lookup:
                price_today = price_lookup.loc[key]
                invested_capital += shares_owned * price_today
        daily_value = cash + invested_capital

        portfolio_history.append({
            'date': current_date,
            'value': daily_value,
            'invested_capital': invested_capital,
            'holdings': {str(k): v for k, v in positions.items() if v != 0}
        })

    portfolio_values_df = pd.DataFrame(portfolio_history)
    portfolio_values_df.set_index('date', inplace=True)

    return portfolio_values_df

if __name__ == '__main__':
    # 1) Load your price data from CSVs
    target_prices_df = pd.read_csv('Target_Prices.csv', parse_dates=['date'])
    acquirer_prices_df = pd.read_csv('Acquirer_Prices.csv', parse_dates=['date'])
    
    # They should both contain columns: date, deal_id, price (or prc), and price_type.
    # Concatenate the two DataFrames.
    price_df = pd.concat([target_prices_df, acquirer_prices_df], ignore_index=True)
    
    # 2) Create a sample orders DataFrame.
    # For a merger arbitrage trade, you might have one order to buy the target and one order to short the acquirer.
    orders_data = [
        # For example, buy 1000 shares of the target (deal 0, price_type 'target') on 2024-12-05
        {'date': '2024-12-05', 'deal_id': 0, 'price_type': 'target', 'shares': 1000},
        # And short 1000 shares of the acquirer (deal 0, price_type 'acquirer') on 2024-12-05
        {'date': '2024-12-05', 'deal_id': 0, 'price_type': 'acquirer', 'shares': -1000},
        # Then close both positions on 2024-12-10 (sell target, buy back acquirer)
        {'date': '2024-12-10', 'deal_id': 0, 'price_type': 'target', 'shares': -1000},
        {'date': '2024-12-10', 'deal_id': 0, 'price_type': 'acquirer', 'shares': 1000},
    ]
    orders_df = pd.DataFrame(orders_data)
    
    # 3) Run the backtest
    portfolio_values_df = backtest(orders_df, price_df, initial_capital=100_000)
    
    # 4) Visualize the portfolio value over time
    plt.figure(figsize=(12, 6))
    plt.plot(portfolio_values_df.index, portfolio_values_df['value'], label='Portfolio Value', linewidth=2)
    plt.title('Portfolio Value Over Time')
    plt.xlabel('Date')
    plt.ylabel('Portfolio Value (USD)')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()
