import pandas as pd
from datetime import datetime
from typing import List, Optional

############################################
# Helper Functions for Date Shifting
############################################
def get_next_trading_day(date: datetime, trading_dates: List[datetime]) -> Optional[datetime]:
    for d in trading_dates:
        if d > date:
            return d
    return None

def get_previous_trading_day(date: datetime, trading_dates: List[datetime]) -> Optional[datetime]:
    prev = None
    for d in trading_dates:
        if d < date:
            prev = d
        else:
            break
    return prev

############################################
# Deal Loading with Implied Probability Estimation
############################################
def estimate_implied_probability(deal) -> Optional[float]:
    try:
        offer_price = extract_offer_price(deal["Cash Terms"])
        arb_spread = float(deal["Arb Spread (Gross)"].strip('%')) / 100
        if offer_price is None or pd.isna(deal["Fallback Price"]):
            return None

        target_price = offer_price * (1 - arb_spread)
        fallback_price = deal["Fallback Price"]

        p = (target_price - fallback_price) / (offer_price - fallback_price)
        return max(0.0, min(p, 1.0))
    except:
        return None
    
def compute_fallback_price(deal_id, announce_date, price_history_df, window=10) -> Optional[float]:
    try:
        mask = (price_history_df["deal_id"] == deal_id) & (price_history_df["date"] < announce_date)
        pre_announce_prices = price_history_df[mask].sort_values("date").tail(window)["price"]
        if pre_announce_prices.empty:
            return None
        return pre_announce_prices.mean()
    except:
        return None
    
def extract_offer_price(cash_terms: str) -> Optional[float]:
    """
    Extract the per-share offer price from the 'Cash Terms' string.
    Ex: "157.0000/sh." → 157.0
    """
    if not isinstance(cash_terms, str):
        return None
    try:
        if "/sh" in cash_terms:
            return float(cash_terms.split("/")[0].replace(",", ""))
        else:
            return None  # skip total value formats like "26000.0000 Mln"
    except:
        return None


def load_deals(deals_csv_path: str, price_history_df: pd.DataFrame, min_prob_threshold: float = 0.75) -> pd.DataFrame:
    deals_df = pd.read_csv(deals_csv_path)
    deals_df = deals_df[deals_df["Payment Type"].str.strip() == "Cash"]

    for col in ["Announce Date", "Completion/Termination Date"]:
        deals_df[col] = pd.to_datetime(deals_df[col], errors='coerce')

    # Merge fallback prices
    deals_df["Fallback Price"] = deals_df.apply(
        lambda row: compute_fallback_price(row["deal_id"], row["Announce Date"], price_history_df),
        axis=1
    )

    deals_df["Implied Prob"] = deals_df.apply(estimate_implied_probability, axis=1)
    deals_df = deals_df[deals_df["Implied Prob"] >= min_prob_threshold]

    return deals_df

############################################
# Order Generation (Probability-Aware)
############################################
def generate_orders(
    deals_df: pd.DataFrame,
    trading_dates: List[datetime],
    shares_on_announce: int = 100,
    scale_with_probability: bool = True
) -> pd.DataFrame:
    orders = []

    for _, deal in deals_df.iterrows():
        if deal.get("Deal Type", "") != "M&A":
            continue

        deal_id = deal["deal_id"]
        p = deal.get("Implied Prob", 1.0)

        announce_date = deal.get("Announce Date", pd.NaT)
        completion_date = deal.get("Completion/Termination Date", pd.NaT)

        # Adjust shares based on probability
        adjusted_shares = int(shares_on_announce * p) if scale_with_probability else shares_on_announce
        if adjusted_shares == 0:
            continue

        # Buy after announcement
        if pd.notna(announce_date):
            buy_date = get_next_trading_day(announce_date, trading_dates)
            if buy_date is not None:
                orders.append({
                    "date": buy_date,
                    "deal_id": deal_id,
                    "shares": adjusted_shares
                })

        # Sell before completion
        if pd.notna(completion_date):
            sell_date = get_previous_trading_day(
                get_previous_trading_day(completion_date, trading_dates),
                trading_dates
            )
            if sell_date is not None:
                orders.append({
                    "date": sell_date,
                    "deal_id": deal_id,
                    "shares": -adjusted_shares
                })

    orders_df = pd.DataFrame(orders)
    orders_df["date"] = pd.to_datetime(orders_df["date"])
    orders_df.sort_values(by="date", inplace=True)
    return orders_df

def generate_orders_from_deals(
    deals_csv_path: str,
    trading_dates: List[datetime],
    price_history_df: pd.DataFrame,
    shares_on_announce: int = 100,
    min_prob_threshold: float = 0.75,
    scale_with_probability: bool = True
) -> pd.DataFrame:
    deals_df = load_deals(deals_csv_path, price_history_df, min_prob_threshold)
    return generate_orders(
        deals_df,
        trading_dates,
        shares_on_announce,
        scale_with_probability
    )