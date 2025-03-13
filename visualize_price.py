import pandas as pd
import wrds
from datetime import timedelta

def filter_only_us(excel_file_name):    
    # Read the Excel file into a DataFrame
    deals_df = pd.read_excel(excel_file_name, sheet_name=0)
    
    # Filter for rows where 'Target Ticker' ends with "US"
    us_deals_df = deals_df[deals_df['Target Ticker'].str.endswith("US", na=False)]
    print(f"Found {us_deals_df.shape[0]} deals with US tickers.")

    return us_deals_df

def process_rows(us_deals_df):
    processed_rows = []
    # Connect to WRDS
    db = wrds.Connection()

    # Loop through each row in the filtered DataFrame
    for idx, row in us_deals_df.iterrows():
        ticker_full = row["Target Ticker"]
        # Parse the ticker: take only the first part (e.g., "PXD" from "PXD US")
        ticker = ticker_full.split()[0]
        
        # Extract the Announce Date and Completion/Termination Date
        announce_date = pd.to_datetime(row["Announce Date"])
        completion_date = pd.to_datetime(row["Completion/Termination Date"])
        # End date is one day after the completion date (exclusive in our query)
        end_date = completion_date + timedelta(days=1)
        
        # Format dates as strings for the SQL query
        start_date_str = announce_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        print(f"Fetching CRSP data for {ticker_full} (parsed as {ticker}) "
              f"from {start_date_str} to {end_date_str}...")
        
        # Build the SQL query for the given ticker and date range.
        query = f"""
            SELECT date, prc
            FROM crsp.dsf 
            WHERE date BETWEEN '{start_date_str}' AND '{end_date_str}'
              AND permno IN (
                  SELECT permno FROM crsp.dsenames 
                  WHERE ticker = '{ticker}'
              )
        """
        
        try:
            data = db.raw_sql(query, date_cols=['date'])
        except Exception as e:
            print(f"Error fetching data for {ticker_full} (parsed as {ticker}): {e}")
            continue
        
        if data.empty:
            print(f"No historical data found for {ticker_full} (parsed as {ticker}). Skipping this row.")
            continue
        
        # Convert the price column to absolute value (CRSP may store negatives)
        data["prc"] = data["prc"].abs()
        
        # Convert the price history to a list of dictionaries (each with date and prc)
        price_history = data[['date', 'prc']].to_dict(orient='records')
        
        # Create a copy of the original row and append the price history
        new_row = row.copy()
        new_row["Price History"] = price_history
        processed_rows.append(new_row)
    
    # Close the WRDS connection
    db.close()

    return processed_rows

def expand_price_history(ma_df):
    """
    Expands the 'Price History' column of the M&A dataframe
    into a single long DataFrame of daily prices.
    
    Returns:
        merged_prices_df (pd.DataFrame): 
            columns = [deal_id, target_ticker, date, price]
    """
    all_rows = []
    for _, row in ma_df.iterrows():
        # Use the deal_id from the main (deals) DataFrame
        deal_id = row["deal_id"]
        ticker = row["Target Ticker"]
        price_history = row["Price History"]  # list of dicts: [{'date':..., 'prc':...}, ...]

        # Expand price history, attaching the same deal_id
        for ph in price_history:
            all_rows.append({
                "deal_id": deal_id,
                "target_ticker": ticker,
                "date": ph["date"],
                "price": ph["prc"],
            })
    
    merged_prices_df = pd.DataFrame(all_rows)
    return merged_prices_df

def main():
    # Filter down to only US deals
    us_deals_df = filter_only_us("MA_deals_largest_100_past_20_years.xlsx")    
    processed_rows = process_rows(us_deals_df)
    
    if not processed_rows:
        print("No rows with valid price data were found.")
        return
    
    # Create a final DataFrame from the processed rows
    final_df = pd.DataFrame(processed_rows)
    
    # ---- ADD UNIQUE IDENTIFIERS HERE ----
    # Assign a new integer ID for each deal
    final_df.reset_index(drop=True, inplace=True)
    final_df["deal_id"] = final_df.index + 1  # e.g., 1, 2, 3, ...
    
    # Save the deals file with a unique ID column
    final_df.to_csv("deals.csv", index=False)
    
    # Expand the price history, carrying the same deal_id over
    expanded_df = expand_price_history(final_df)
    # Save the expanded price data with the same deal_id
    expanded_df.to_csv("price.csv", index=False)

if __name__ == "__main__":
    main()
