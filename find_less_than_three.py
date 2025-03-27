import pandas as pd

# Load the CSV file
df = pd.read_csv("price.csv")

# Initialize variables
previous_ticker = None
count = 0
row_numbers = []

# Iterate through the dataframe
for index, row in df.iterrows():
    ticker = row["target_ticker"]
    
    if ticker == previous_ticker:
        count += 1
    else:
        if count <= 3:
            row_numbers.append(index + 1)  # Adding 1 to match row number in CSV (1-based index)
        count = 1  # Reset count for new ticker
    
    
    
    previous_ticker = ticker

# Output the row numbers
print("Row numbers:", row_numbers)
