import pandas as pd

def prepare_events_schedule(ma_df):
    """
    Create a schedule of M&A events, sorted by Announce Date.
    Returns a DataFrame of essential columns.
    """
    # Make sure we have them as datetime
    ma_df["Announce Date"] = pd.to_datetime(ma_df["Announce Date"])
    ma_df["Completion/Termination Date"] = pd.to_datetime(ma_df["Completion/Termination Date"])

    # Create or copy event_id as index
    events = ma_df.copy()
    events["event_id"] = events.index
    
    # Sort by announce date
    events.sort_values(by="Announce Date", inplace=True)
    events.reset_index(drop=True, inplace=True)
    
    # Return only essential columns
    return events[["event_id", "Target Ticker", "Announce Date", "Completion/Termination Date"]]

def main():
    deals_df = pd.read_excel("MA_deals_largest_100_past_20_years.xlsx", sheet_name=0)

    sorted_events = prepare_events_schedule(deals_df)

    print(sorted_events)

if __name__ == "__main__":
    main()
