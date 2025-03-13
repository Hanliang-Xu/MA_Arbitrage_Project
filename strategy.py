import pandas as pd
import matplotlib.pyplot as plt

############################################
# 1) Helper functions for shifting dates
############################################
def get_next_trading_day(date, trading_dates):
    """
    Return the earliest trading day strictly after `date`.
    If none is found, return None.
    """
    for d in trading_dates:
        if d > date:
            return d
    return None

def get_previous_trading_day(date, trading_dates):
    """
    Return the latest trading day strictly before `date`.
    If none is found, return None.
    """
    prev = None
    for d in trading_dates:
        if d < date:
            prev = d
        else:
            break
    return prev

############################################
# 2) Strategy code: generate_orders_from_deals
############################################
def generate_orders_from_deals(
    deals_csv_path: str,
    all_trading_dates,               # pass in the sorted list of valid trading days
    shares_on_announce: int = 100,
    shares_on_amendment: int = 50
) -> pd.DataFrame:
    """
    Generate a strategy that:
      - Buys 'shares_on_announce' shares on the first valid trading day
        *after* the Announce Date.
      - Buys additional 'shares_on_amendment' on the Amendment Date (unchanged).
      - Sells everything on the last valid trading day *before* the Completion/Termination Date.
    """
    # 1) Read deals CSV normally (no parse_dates argument)
    deals_df = pd.read_csv(deals_csv_path)

    # 2) Convert date columns to datetime with errors='coerce'
    for col in ["Announce Date", "Amendment Date", "Completion/Termination Date"]:
        deals_df[col] = pd.to_datetime(deals_df[col], errors='coerce')

    orders = []

    for _, deal in deals_df.iterrows():
        # Filter to M&A rows only
        if deal.get("Deal Type", "") != "M&A":
            continue

        deal_id = deal['deal_id']
        announce_date = deal.get("Announce Date", pd.NaT)
        amend_date = deal.get("Amendment Date", pd.NaT)
        completion_date = deal.get("Completion/Termination Date", pd.NaT)

        # ------------------------------------
        # Shift the Announce Date to the *next* trading day and buy shares_on_announce
        # ------------------------------------
        if pd.notna(announce_date):
            buy_date = get_next_trading_day(announce_date, all_trading_dates)
            if buy_date is not None:
                orders.append({
                    "date": buy_date.strftime("%Y-%m-%d"),
                    "deal_id": deal_id,
                    "shares": shares_on_announce
                })

        # ------------------------------------
        # Buy additional on the *exact* Amendment Date (unchanged)
        # ------------------------------------
        """
        if pd.notna(amend_date):
            orders.append({
                "date": amend_date.strftime("%Y-%m-%d"),
                "deal_id": deal_id,
                "shares": shares_on_amendment
            })
        """

        # ------------------------------------
        # Shift the Completion Date to the *previous* trading day and sell everything
        # ------------------------------------
        if pd.notna(completion_date):
            sell_date = get_previous_trading_day(completion_date, all_trading_dates)
            # total shares to sell = sum of all we bought for that deal
            total_shares_bought = shares_on_announce
            if pd.notna(amend_date):
                total_shares_bought += shares_on_amendment

            if sell_date is not None:
                orders.append({
                    "date": sell_date.strftime("%Y-%m-%d"),
                    "deal_id": deal_id,
                    "shares": -total_shares_bought
                })

    # Convert orders to DataFrame
    orders_df = pd.DataFrame(orders)

    # Sort by date (ascending)
    orders_df["date"] = pd.to_datetime(orders_df["date"])
    orders_df.sort_values(by="date", inplace=True)

    # Convert the date column back to string if desired
    orders_df["date"] = orders_df["date"].dt.strftime("%Y-%m-%d")
    return orders_df

############################################
# 3) Backtester code
############################################
def backtest(
    orders_df: pd.DataFrame,
    price_df: pd.DataFrame,
    initial_capital: float = 1_000_000
) -> pd.DataFrame:
    """
    Run a simple backtest given a set of orders and daily prices,
    keyed by (date, deal_id).

    Parameters
    ----------
    orders_df : pd.DataFrame
        Must contain columns:
          - 'date' (Timestamp or string YYYY-MM-DD)
          - 'deal_id'
          - 'shares' (number of shares to buy (>0) or sell (<0))

    price_df : pd.DataFrame
        Must contain columns:
          - 'date' (Timestamp)
          - 'deal_id'
          - 'price'

    Returns
    -------
    portfolio_values_df : pd.DataFrame
        Index: date
        Column: 'value'
    """
    # Ensure 'date' is datetime in both
    if not pd.api.types.is_datetime64_any_dtype(orders_df['date']):
        orders_df['date'] = pd.to_datetime(orders_df['date'])
    if not pd.api.types.is_datetime64_any_dtype(price_df['date']):
        price_df['date'] = pd.to_datetime(price_df['date'])

    # Sort orders by date
    orders_df = orders_df.sort_values(by='date')

    # Make a list of all trading dates from price_df
    all_dates = sorted(price_df['date'].unique())

    # Positions: {deal_id: shares}
    positions = {}
    cash = initial_capital
    portfolio_history = []

    # Convert price_df to a lookup table
    price_lookup = price_df.set_index(['date', 'deal_id'])['price']

    # Group orders by date
    orders_by_date = orders_df.groupby('date')

    for current_date in all_dates:
        # ---------- 1) Execute orders for current_date ----------
        if current_date in orders_by_date.groups:
            daily_orders = orders_by_date.get_group(current_date)
            for _, row in daily_orders.iterrows():
                deal_id = row['deal_id']
                shares_to_buy = row['shares']

                # Price lookup for (current_date, deal_id)
                if (current_date, deal_id) not in price_lookup:
                    raise ValueError(f"Price not found for date {current_date} and event_id {deal_id}")
                debug_series = price_lookup.loc[(current_date, deal_id)]

                current_price = float(price_lookup.loc[(current_date, deal_id)].iloc[0])

                order_cost = current_price * shares_to_buy
                # Update positions
                positions[deal_id] = positions.get(deal_id, 0) + shares_to_buy
                # Update cash
                cash -= order_cost

        # ---------- 2) Compute daily portfolio value ----------
        daily_value = cash
        for d_id, shares_owned in positions.items():
            if (current_date, d_id) in price_lookup:
                #print(f"DEBUG: {[(current_date, d_id)]}")
                p = price_lookup[(current_date, d_id)]
                
                daily_value += shares_owned * p

        portfolio_history.append({'date': current_date, 'value': daily_value})

    # Build a DataFrame
    portfolio_values_df = pd.DataFrame(portfolio_history)
    portfolio_values_df.set_index('date', inplace=True)
    return portfolio_values_df

############################################
# 4) Putting it all together
############################################
if __name__ == "__main__":
    # 1) Load your daily price data, get all trading dates
    price_df = pd.read_csv("price.csv", parse_dates=["date"])

    duplicates = price_df.duplicated(subset=['date', 'deal_id'], keep=False)
    if duplicates.any():
        print("Warning: Duplicate (date, deal_id) entries found in price_df!")
        print(price_df[duplicates])

    all_trading_dates = sorted(price_df['date'].unique())

    # 2) Generate orders (using next/previous *trading* days)
    orders_df = generate_orders_from_deals(
        deals_csv_path="deals.csv",
        all_trading_dates=all_trading_dates,
        shares_on_announce=100,
        shares_on_amendment=50
    )


    # 3) Run the backtest
    portfolio_values_df = backtest(
        orders_df=orders_df,
        price_df=price_df,
        initial_capital=1_000_000
    )

    # 4) (Optional) Plot the result
    plt.figure(figsize=(12, 6))
    plt.plot(portfolio_values_df.index, portfolio_values_df['value'], label='Portfolio Value')
    plt.title('M&A Strategy Portfolio Value Over Time')
    plt.xlabel('Date')
    plt.ylabel('Value (USD)')
    plt.legend()
    plt.grid(True)
    plt.show()
