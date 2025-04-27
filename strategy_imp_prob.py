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
def extract_offer_price(cash_terms: str) -> Optional[float]:
    if not isinstance(cash_terms, str):
        return None
    try:
        if "/sh" in cash_terms:
            price_per_share = float(cash_terms.split("/")[0].replace(",", ""))
            return price_per_share
        else:
            print(f"Skipping non-per-share offer: {cash_terms}")
            return None
    except Exception as e:
        print(f"Failed to parse cash_terms '{cash_terms}': {e}")
        return None

def compute_fallback_price(deal_id, announce_date, price_history_df) -> Optional[float]:
    try:
        price_rows = price_history_df[
            (price_history_df["deal_id"] == deal_id) &
            (price_history_df["date"] >= announce_date)
        ].sort_values("date")

        if price_rows.empty:
            print(f"No price found for deal_id {deal_id} on or after announce date {announce_date}")
            return None

        first_row = price_rows.iloc[0]
        actual_date = first_row["date"]

        if actual_date != announce_date:
            print(f"Using fallback price for deal_id {deal_id} from {actual_date.date()} instead of announce date {announce_date.date()}")

        return first_row["price"]

    except Exception as e:
        print(f"Error getting fallback price for deal_id {deal_id}: {e}")
        return None

def estimate_implied_probability(deal) -> Optional[float]:
    try:
        offer_price = extract_offer_price(deal["Cash Terms"])
        if offer_price is None:
            print(f"Invalid cash terms for deal_id {deal['deal_id']}: {deal['Cash Terms']}")
            return None
        arb_spread = float(deal["Arb Spread (Gross)"]) / 100
        if pd.isna(deal["Arb Spread (Gross)"]):
            print(f"NaN Arb Spread for deal_id {deal['deal_id']}")
        fallback_price = deal["Fallback Price"]
        
        if offer_price is None or pd.isna(fallback_price):
            if offer_price is None:
                print(f"Offer price is None for deal_id {deal['deal_id']}")
            if pd.isna(fallback_price):
                print(f"Fallback price is NaN for deal_id {deal['deal_id']}")
            return None

        target_price = offer_price * (1 - arb_spread)
        p = (target_price - fallback_price) / (offer_price - fallback_price)
        return max(0.0, min(p, 1.0))
    except Exception as e:
        print(f"Error estimating implied probability for deal_id {deal['deal_id']}: {e}")
        return None

def load_deals(deals_csv_path: str, price_history_df: pd.DataFrame, min_prob_threshold: float = 0.75) -> pd.DataFrame:
    deals_df = pd.read_csv(deals_csv_path)
    deals_df = deals_df[deals_df["Payment Type"].str.strip() == "Cash"]

    # Ensure date columns are datetime
    for col in ["Announce Date", "Completion/Termination Date"]:
        deals_df[col] = pd.to_datetime(deals_df[col], errors='coerce')

    # Ensure deal_id is the same dtype as in price_history_df
    deals_df["deal_id"] = deals_df["deal_id"].astype(price_history_df["deal_id"].dtype)

    # Compute fallback price
    deals_df["Fallback Price"] = deals_df.apply(
        lambda row: compute_fallback_price(row["deal_id"], row["Announce Date"], price_history_df),
        axis=1
    )
    print(f"Fallback Price — min: {deals_df['Fallback Price'].min():.2f}, max: {deals_df['Fallback Price'].max():.2f}")


    deals_df["Arb Spread (Gross)"] = deals_df["Arb Spread (Gross)"].replace(r'^\s*$', pd.NA, regex=True)
    deals_df["Arb Spread (Gross)"] = deals_df["Arb Spread (Gross)"].fillna(0.0)
    deals_df["Arb Spread (Gross)"] = deals_df["Arb Spread (Gross)"].astype(float)

    print(f"Arb Spread (Gross) — min: {deals_df['Arb Spread (Gross)'].min():.2f}%, max: {deals_df['Arb Spread (Gross)'].max():.2f}%")


    # Compute implied probabilities
    deals_df["Implied Prob"] = deals_df.apply(estimate_implied_probability, axis=1)

    print(f"Loaded {len(deals_df)} deals")
    print(f"Max Implied Probability: {deals_df['Implied Prob'].max():.2%}")
    print(f"Min Implied Probability: {deals_df['Implied Prob'].min():.2%}")
    print(f"Deals with Implied Probabilities: {deals_df['Implied Prob'].notna().sum()}")
    print(f"Deals with Fallback Prices: {deals_df['Fallback Price'].notna().sum()}")
    print(f"Deals with NaN Fallback Prices: {deals_df['Fallback Price'].isna().sum()}")
    print(f"Deals with NaN Implied Probabilities: {deals_df['Implied Prob'].isna().sum()}")
    print(f"Deals with NaN Cash Terms: {deals_df['Cash Terms'].isna().sum()}")
    print(f"Deals with NaN Arb Spread: {deals_df['Arb Spread (Gross)'].isna().sum()}")
    deals_df = deals_df[deals_df["Implied Prob"] >= min_prob_threshold]
    print(f"Deals after filtering (p ≥ {min_prob_threshold}): {len(deals_df)}")

    return deals_df

############################################
# Order Generation
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

        adjusted_shares = int(shares_on_announce * p) if scale_with_probability else shares_on_announce
        if adjusted_shares == 0:
            continue

        if pd.notna(announce_date):
            buy_date = get_next_trading_day(announce_date, trading_dates)
            if buy_date is not None:
                orders.append({
                    "date": buy_date,
                    "deal_id": deal_id,
                    "shares": adjusted_shares
                })

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

    if orders_df.empty:
        print("No orders were generated — check probability thresholds or data.")
        return orders_df  

    if "date" in orders_df.columns:
        orders_df["date"] = pd.to_datetime(orders_df["date"])
        orders_df.sort_values(by="date", inplace=True)
    else:
        print("'date' column missing in orders — this should not happen if orders are formatted properly.")
        return pd.DataFrame()

    return orders_df

############################################
# Entry Point for Strategy Usage
############################################
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
