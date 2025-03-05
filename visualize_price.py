import pandas as pd
import wrds
from datetime import timedelta

def main():
    # Replace with the path to your Excel file containing deal info
    excel_file = "MA_deals_largest_100_past_20_years.xlsx"
    
    # Read the Excel file into a DataFrame
    deals_df = pd.read_excel(excel_file, sheet_name=0)
    
    # Filter for rows where 'Target Ticker' ends with "US"
    us_deals_df = deals_df[deals_df['Target Ticker'].str.endswith("US", na=False)]
    print(f"Found {us_deals_df.shape[0]} deals with US tickers.")
    
    # Connect to WRDS
    db = wrds.Connection()
    
    # List to store rows (as Series) that have valid price histories
    processed_rows = []
    
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
        
        print(f"Fetching CRSP data for {ticker_full} (parsed as {ticker}) from {start_date_str} to {end_date_str}...")
        
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
    
    if not processed_rows:
        print("No rows with valid price data were found.")
        return
    
    # Create a final DataFrame from the processed rows
    final_df = pd.DataFrame(processed_rows)
    
    # Save the final DataFrame to a CSV file named "with_price.csv"
    final_df.to_csv("with_price.csv", index=False)
    print("Final DataFrame saved to 'with_price.csv'.")
    
if __name__ == "__main__":
    main()
