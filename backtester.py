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
          - 'event_id' (or ticker, some identifier)
          - 'shares' (number of shares to buy (>0) or sell (<0))

        Example row: 
            date       2025-01-05
            event_id   AAPL
            shares     100
            (means buy 100 shares of AAPL on 2025-01-05)

    price_df : pd.DataFrame
        Must contain columns:
          - 'date'
          - 'event_id'
          - 'price'
        There should be one row per (date, event_id). The date must align
        with the trading calendar you wish to simulate. 
        For instance, each event_id/ticker's daily close price.

    initial_capital : float
        Starting cash amount, defaults to 1,000,000.

    Returns
    -------
    portfolio_values_df : pd.DataFrame
        Index: date (daily)
        Column: 'value'
        The “end of day” value of the portfolio, in USD.
    """

    # Ensure 'date' is a datetime in both dataframes
    if not pd.api.types.is_datetime64_any_dtype(orders_df['date']):
        orders_df['date'] = pd.to_datetime(orders_df['date'])

    if not pd.api.types.is_datetime64_any_dtype(price_df['date']):
        price_df['date'] = pd.to_datetime(price_df['date'])

    # Sort orders by date (important if multiple on the same day)
    orders_df = orders_df.sort_values(by='date')

    # Create a list of all trading dates from price_df
    all_dates = sorted(price_df['date'].unique())

    # Positions will be tracked in a dictionary: {event_id: number_of_shares}
    positions = {}
    cash = initial_capital

    portfolio_history = []

    # Convert price_df to a multi-index for quick lookups:
    #   price_lookup[(date, event_id)] -> price
    duplicates = price_df.duplicated(subset=['date', 'deal_id'], keep=False)
    if duplicates.any():
        print("Warning: Duplicate (date, deal_id) entries found in price_df!")
        print(price_df[duplicates])
    
    price_lookup = price_df.set_index(['date', 'deal_id'])['price']

    # For convenience, group orders by date so we can process them in the daily loop
    orders_by_date = orders_df.groupby('date')

    for current_date in all_dates:
        # ----- 1) Execute any orders for current_date -----
        if current_date in orders_by_date.groups:
            daily_orders = orders_by_date.get_group(current_date)
            for _, row in daily_orders.iterrows():
                deal_id = int(row['deal_id'])
                shares_to_buy = row['shares']

                if (current_date, deal_id) in price_lookup:
                    debug_series = price_lookup.loc[(current_date, deal_id)]
                    current_price = float(price_lookup.loc[(current_date, deal_id)])
                    
                else:
                    raise ValueError(f"Price not found for date {current_date} and event_id {deal_id}")

                # Calculate cost of this order = price * shares
                # shares_to_buy can be negative (sell).
                order_cost = current_price * shares_to_buy

                # Check if enough cash (if it's a buy). If not, you can reject, or
                # partially fill, etc. For simplicity, let's assume unlimited margin or skip.
                # If you want to enforce constraints, you'd do it here.
                # Example: if we want to disallow going below zero cash on buy:
                #   if shares_to_buy > 0 and cash < order_cost:
                #       # skip or partial fill
                #       continue

                # Update positions
                positions[deal_id] = positions.get(deal_id, 0) + shares_to_buy

                # Update cash (spent or received)
                cash -= order_cost

        # ----- 2) Compute daily portfolio value -----
        # = sum of (positions[event_id] * today's price) over all event_ids + cash
        invested_capital = 0

        # Sum the value of each open position
        for deal_id, shares_owned in positions.items():
            if (current_date, deal_id) in price_lookup:
                p = price_lookup[(current_date, deal_id)]
                invested_capital += shares_owned * p

        daily_value = cash + invested_capital
        filtered_positions = {key: value for key, value in positions.items() if value > 0}

        # Record the daily portfolio value
        portfolio_history.append({'date': current_date, 'value': daily_value, 'invested_capital': invested_capital, 'holdings': filtered_positions})

    # Create a DataFrame of results
    portfolio_values_df = pd.DataFrame(portfolio_history)
    portfolio_values_df.set_index('date', inplace=True)

    return portfolio_values_df


if __name__ == '__main__':
    # -----------------------------
    # Example usage:
    # -----------------------------
    # 1) Load or create your price data
    price_df = pd.read_csv('price.csv', parse_dates=['date'])
    
    # 2) Suppose you have a separate strategy that decides to buy 100 shares of 'AAPL'
    #    on 2025-01-05 and then sell 100 shares on 2025-01-10. 
    #    You generate an orders DataFrame:
    orders_data = [
        {'date': '2023-10-11', 'deal_id': '1', 'shares': 100},
        {'date': '2024-05-02', 'deal_id': '1', 'shares': -100}
    ]
    orders_df = pd.DataFrame(orders_data)

    # 3) Run the backtest
    portfolio_values_df = backtest(orders_df, price_df, initial_capital=100_000)

    # 4) Visualize
    plt.figure(figsize=(12, 6))
    plt.plot(portfolio_values_df.index, portfolio_values_df['value'], label='Portfolio Value', linewidth=2)
    plt.title('Portfolio Value Over Time')
    plt.xlabel('Date')
    plt.ylabel('Value (USD)')
    plt.legend()
    plt.grid()
    plt.show()
