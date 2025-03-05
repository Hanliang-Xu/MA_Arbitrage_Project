import pandas as pd

def main():
    # Replace 'my_deals.xlsx' with the actual path to your Excel file
    excel_file = "MA_deals_largest_100_past_20_years.xlsx"
    
    # Read the Excel file into a DataFrame (adjust sheet_name if needed)
    df = pd.read_excel(excel_file, sheet_name=0)
    
    # Filter rows where the 'Target Ticker' ends with "US"
    # Adjust 'Target Ticker' to match your Excel column header if necessary.
    us_stocks = df[df['Target Ticker'].str.endswith("US", na=False)]
    
    # Count how many companies have a ticker ending in "US"
    count_us = us_stocks.shape[0]
    
    print(f"Number of companies with US ending: {count_us}")
    
    # Optionally, print the unique tickers with "US" ending:
    print("Tickers:")
    print(us_stocks['Target Ticker'].unique())

if __name__ == "__main__":
    main()
