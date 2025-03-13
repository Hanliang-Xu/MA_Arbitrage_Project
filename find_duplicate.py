import pandas as pd

def find_duplicate_target_tickers(file_path="price.csv"):
    # Load CSV file
    df = pd.read_csv(file_path)

    # Count occurrences of unique combinations of first three columns
    duplicate_groups = df.groupby(['deal_id', 'target_ticker', 'date']).size()

    # Filter out groups that appear more than once
    duplicated_tickers = duplicate_groups[duplicate_groups > 1].index.get_level_values('target_ticker').unique().tolist()

    return duplicated_tickers

if __name__ == "__main__":
    result = find_duplicate_target_tickers()
    print(result)
