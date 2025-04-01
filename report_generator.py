import pandas as pd

def save_portfolio_report_csv(portfolio_values_df: pd.DataFrame, file_path: str = "daily_portfolio_report.csv"):
    """
    Save the portfolio DataFrame to a CSV file.
    """
    portfolio_values_df.to_csv(file_path)
    print(f"Portfolio report saved to {file_path}")

def save_portfolio_report_html(portfolio_values_df: pd.DataFrame, file_path: str = "daily_portfolio_report.html"):
    """
    Save the portfolio DataFrame to an HTML file.
    """
    html_content = portfolio_values_df.to_html()
    with open(file_path, "w") as f:
        f.write(html_content)
    print(f"Portfolio report saved to {file_path}")
