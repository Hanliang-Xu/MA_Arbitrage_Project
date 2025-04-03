import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Optional

############################################
# Special Price Record Selection
############################################
# Define the special set for which the second record is used if available
special_set = {(845, 'target'), (64, 'target')}

def select_record(group: pd.DataFrame) -> pd.Series:
    """
    For each group (keyed by (date, deal_id, leg)), return:
      - The second record if (deal_id, leg) is in the special set and the group has at least 2 records.
      - Otherwise, return the first record.
    """
    _, deal_id, leg = group.name
    if (deal_id, leg) in special_set:
        if len(group) >= 2:
            return group.iloc[1]
        else:
            return group.iloc[0]
    else:
        return group.iloc[0]

############################################
# Data Loading
############################################
def load_deals(deals_csv_path: str) -> pd.DataFrame:
    """
    Load deals data from CSV and convert date columns.
    """
    deals_df = pd.read_csv(deals_csv_path, parse_dates=['Announce Date', 'Completion/Termination Date'])
    deals_df = deals_df[deals_df["Payment Type"].str.strip() == "Stock"]

    return deals_df

def load_prices(prices_csv_path: str) -> pd.DataFrame:
    """
    Load prices data from CSV, sort, and apply custom record selection.
    """
    price_df = pd.read_csv(prices_csv_path, parse_dates=['date'])
    # Sort by date, deal_id, leg, and price so that the first record is the lowest price
    price_df = price_df.sort_values(by=['date', 'deal_id', 'leg', 'price'], ascending=True)
    # For each (date, deal_id, leg), select the record using the custom function
    price_df = price_df.groupby(['date', 'deal_id', 'leg'], group_keys=False).apply(select_record).reset_index(drop=True)
    # If there are still duplicates, keep the first occurrence
    price_df = price_df.groupby(['date', 'deal_id', 'leg'], as_index=False).first()
    return price_df

############################################
# Order Generation (Long-Short Capital Matching)
############################################
def generate_orders(
    deals_df: pd.DataFrame,
    price_df: pd.DataFrame,
    capital_each_side: float = 10000
) -> pd.DataFrame:
    """
    Generate entry and exit orders for each deal such that:
      - On the entry day (the first valid trading day on/after the announce date for both legs),
        the strategy goes long the target and short the acquirer.
      - On the exit day (the last valid trading day on/before the completion date for both legs),
        the positions are reversed.
    
    The number of shares is computed to match a nominal capital allocation on each side.
    
    Parameters
    ----------
    deals_df : pd.DataFrame
        DataFrame containing deals (with columns 'deal_id', 'Announce Date', 'Completion/Termination Date').
    price_df : pd.DataFrame
        DataFrame containing prices (with columns 'date', 'deal_id', 'leg', 'price').
    capital_each_side : float, optional
        Nominal capital allocated per side (default is 10000).
        
    Returns
    -------
    pd.DataFrame
        Orders DataFrame with columns: 'date', 'deal_id', 'shares', 'leg', and 'action'.
    """
    orders = []
    # Group prices by deal_id and leg for quick lookup
    grouped_prices = price_df.groupby(['deal_id', 'leg'])
    
    for _, deal in deals_df.iterrows():
        deal_id = deal['deal_id']
        announce_date = deal['Announce Date']
        completion_date = deal['Completion/Termination Date']
        
        # Retrieve prices for both legs; skip deal if either is missing
        if (deal_id, 'target') in grouped_prices.groups:
            target_prices = grouped_prices.get_group((deal_id, 'target'))
        else:
            print(f"DEBUG: missing pricees for deal_id {deal_id}")
            # raise ValueError(f"Price not found")
            continue
        if (deal_id, 'acquirer') in grouped_prices.groups:
            acquirer_prices = grouped_prices.get_group((deal_id, 'acquirer'))
        else:
            print(f"DEBUG: missing prices for deal_id {deal_id}")
            # raise ValueError(f"Price not found")
            continue
        
        # Determine entry dates: for each leg, take the first date on/after the announce date,
        # then use the later one so that both legs trade on the same day.
        target_entry_df = target_prices[target_prices['date'] >= announce_date]
        acquirer_entry_df = acquirer_prices[acquirer_prices['date'] >= announce_date]
        if target_entry_df.empty or acquirer_entry_df.empty:
            continue
        target_entry_date = target_entry_df['date'].min()
        acquirer_entry_date = acquirer_entry_df['date'].min()
        entry_date = max(target_entry_date, acquirer_entry_date)
        
        # Get the entry prices for each leg on the entry date
        target_entry_price_series = target_prices[target_prices['date'] == entry_date]['price']
        acquirer_entry_price_series = acquirer_prices[acquirer_prices['date'] == entry_date]['price']
        if target_entry_price_series.empty or acquirer_entry_price_series.empty:
            continue
        target_entry_price = target_entry_price_series.iloc[0]
        acquirer_entry_price = acquirer_entry_price_series.iloc[0]
        
        # Determine exit dates: for each leg, take the last date on/before the completion date,
        # then choose the earlier date to ensure both legs exit on the same day.
        target_exit_df = target_prices[target_prices['date'] <= completion_date]
        acquirer_exit_df = acquirer_prices[acquirer_prices['date'] <= completion_date]
        if target_exit_df.empty or acquirer_exit_df.empty:
            continue
        target_exit_date = target_exit_df['date'].max()
        acquirer_exit_date = acquirer_exit_df['date'].max()
        exit_date = min(target_exit_date, acquirer_exit_date)
        
        # Calculate shares for each leg based on the nominal capital
        if target_entry_price <= 0 or np.isnan(target_entry_price):
            continue
        shares_target = int(capital_each_side // target_entry_price)
        if shares_target == 0:
            continue
        
        if acquirer_entry_price <= 0 or np.isnan(acquirer_entry_price):
            continue
        shares_acquirer = int(capital_each_side // acquirer_entry_price)
        if shares_acquirer == 0:
            continue
        
        # Append entry orders (long target, short acquirer)
        orders.append({
            'date': entry_date,
            'deal_id': deal_id,
            'shares': shares_target,  # Buy target
            'leg': 'target',
            'action': 'entry'
        })
        orders.append({
            'date': entry_date,
            'deal_id': deal_id,
            'shares': -shares_acquirer,  # Sell acquirer
            'leg': 'acquirer',
            'action': 'entry'
        })
        
        # Append exit orders (reverse the positions)
        orders.append({
            'date': exit_date,
            'deal_id': deal_id,
            'shares': -shares_target,  # Sell target
            'leg': 'target',
            'action': 'exit'
        })
        orders.append({
            'date': exit_date,
            'deal_id': deal_id,
            'shares': shares_acquirer,  # Buy acquirer
            'leg': 'acquirer',
            'action': 'exit'
        })
    
    orders_df = pd.DataFrame(orders)
    orders_df['date'] = pd.to_datetime(orders_df['date'])
    orders_df.sort_values(by='date', inplace=True)
    return orders_df

############################################
# API Function: generate_orders_from_deals
############################################
def generate_orders_from_deals(
    deals_csv_path: str,
    prices_csv_path: str,
    capital_each_side: float = 10000
) -> pd.DataFrame:
    """
    Convenience wrapper that loads deals and prices from CSV files and generates orders.
    
    Parameters
    ----------
    deals_csv_path : str
        Path to the deals CSV file.
    prices_csv_path : str
        Path to the prices CSV file.
    capital_each_side : float, optional
        Nominal capital allocation per side (default is 10000).
        
    Returns
    -------
    pd.DataFrame
        Orders DataFrame.
    """
    deals_df = load_deals(deals_csv_path)
    price_df = load_prices(prices_csv_path)
    return generate_orders(deals_df, price_df, capital_each_side)

# Example usage:
if __name__ == "__main__":
    orders_df = generate_orders_from_deals("deals.csv", "price_stock_deals.csv", capital_each_side=30000)
    print("Number of generated orders:", len(orders_df))
    print(orders_df.head(10))
