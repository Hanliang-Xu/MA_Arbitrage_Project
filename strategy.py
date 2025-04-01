import pandas as pd
from datetime import datetime
from typing import List, Optional

############################################
# Helper Functions for Date Shifting
############################################
def get_next_trading_day(date: datetime, trading_dates: List[datetime]) -> Optional[datetime]:
    """
    Return the earliest trading day strictly after `date`.
    If none is found, return None.
    """
    for d in trading_dates:
        if d > date:
            return d
    return None

def get_previous_trading_day(date: datetime, trading_dates: List[datetime]) -> Optional[datetime]:
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
# Deal Loading
############################################
def load_deals(deals_csv_path: str) -> pd.DataFrame:
    """
    Load deals data from a CSV and convert relevant date columns to datetime.
    
    Parameters
    ----------
    deals_csv_path : str
        Path to the deals CSV file.
        
    Returns
    -------
    pd.DataFrame
        DataFrame containing deals with converted date columns.
    """
    deals_df = pd.read_csv(deals_csv_path)
    deals_df = deals_df[deals_df["Payment Type"].str.strip() == "Cash"]
    
    date_columns = ["Announce Date", "Amendment Date", "Completion/Termination Date"]
    for col in date_columns:
        deals_df[col] = pd.to_datetime(deals_df[col], errors='coerce')
    return deals_df

############################################
# Order Generation (Strategy)
############################################
def generate_orders(
    deals_df: pd.DataFrame,
    trading_dates: List[datetime],
    shares_on_announce: int = 100,
    shares_on_amendment: int = 50
) -> pd.DataFrame:
    """
    Generate trading orders based on deal events.
    
    The strategy is:
      - Buy `shares_on_announce` shares on the first valid trading day after the Announce Date.
      - (Optionally) Buy additional shares on the Amendment Date.
      - Sell all shares on the last valid trading day before the Completion/Termination Date.
    
    Parameters
    ----------
    deals_df : pd.DataFrame
        DataFrame containing deal information.
    trading_dates : List[datetime]
        Sorted list of valid trading dates.
    shares_on_announce : int, optional
        Number of shares to buy at the announce event (default is 100).
    shares_on_amendment : int, optional
        Number of shares to buy on amendment (default is 50). [Currently unused.]
        
    Returns
    -------
    pd.DataFrame
        Orders DataFrame with columns 'date', 'deal_id', and 'shares'.
    """
    orders = []
    for _, deal in deals_df.iterrows():
        # Filter to M&A deals only
        if deal.get("Deal Type", "") != "M&A":
            continue

        deal_id = deal['deal_id']
        announce_date = deal.get("Announce Date", pd.NaT)
        # amend_date = deal.get("Amendment Date", pd.NaT)  # Unused for now.
        completion_date = deal.get("Completion/Termination Date", pd.NaT)

        # Buy on the first valid trading day after Announce Date
        if pd.notna(announce_date):
            buy_date = get_next_trading_day(announce_date, trading_dates)
            if buy_date is not None:
                orders.append({
                    "date": buy_date,
                    "deal_id": deal_id,
                    "shares": shares_on_announce
                })

        # Sell all shares on the last valid trading day before Completion Date
        if pd.notna(completion_date):
            # Here we call get_previous_trading_day twice if needed
            sell_date = get_previous_trading_day(
                get_previous_trading_day(completion_date, trading_dates),
                trading_dates
            )
            if sell_date is not None:
                orders.append({
                    "date": sell_date,
                    "deal_id": deal_id,
                    "shares": -shares_on_announce
                })

    orders_df = pd.DataFrame(orders)
    orders_df["date"] = pd.to_datetime(orders_df["date"])
    orders_df.sort_values(by="date", inplace=True)
    return orders_df

def generate_orders_from_deals(
    deals_csv_path: str,
    trading_dates: List[datetime],
    shares_on_announce: int = 100,
    shares_on_amendment: int = 50
) -> pd.DataFrame:
    """
    Load deals from CSV and generate trading orders.
    
    This is a convenience wrapper that combines `load_deals` and `generate_orders`.
    
    Parameters
    ----------
    deals_csv_path : str
        Path to the deals CSV file.
    trading_dates : List[datetime]
        Sorted list of valid trading dates.
    shares_on_announce : int, optional
        Number of shares to buy on announcement.
    shares_on_amendment : int, optional
        Number of shares to buy on amendment.
        
    Returns
    -------
    pd.DataFrame
        Orders DataFrame.
    """
    deals_df = load_deals(deals_csv_path)
    return generate_orders(deals_df, trading_dates, shares_on_announce, shares_on_amendment)
