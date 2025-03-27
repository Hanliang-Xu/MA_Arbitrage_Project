import pandas as pd
import matplotlib.pyplot as plt
from strategy import generate_orders_from_deals
from backtester import backtest
from stats_utils import compute_cagr, compute_sharpe_ratio, compute_max_drawdown

def main():
    # 1) Load price data and extract trading dates
    price_df = pd.read_csv("price.csv", parse_dates=["date"])
    trading_dates = sorted(price_df['date'].unique())

    # 2) Generate orders from deals using the strategy module
    orders_df = generate_orders_from_deals(
        deals_csv_path="deals.csv",
        trading_dates=trading_dates,
        shares_on_announce=100,
        shares_on_amendment=50
    )

    # 3) Run the backtest using the backtester module
    portfolio_values_df = backtest(
        orders_df=orders_df,
        price_df=price_df,
        initial_capital=1_000_000
    )

    # 4) Compute performance statistics using the stats_utils module
    portfolio_series = portfolio_values_df['value']
    start_date = portfolio_series.index.min().strftime("%Y-%m-%d")
    end_date = portfolio_series.index.max().strftime("%Y-%m-%d")
    cagr = compute_cagr(portfolio_series, start_date, end_date)
    sharpe = compute_sharpe_ratio(portfolio_series)
    max_dd = compute_max_drawdown(portfolio_series)

    print("CAGR:", cagr)
    print("Sharpe Ratio:", sharpe)
    print("Max Drawdown:", max_dd)

    # 5) Visualize the portfolio performance
    plt.figure(figsize=(12, 6))
    plt.plot(portfolio_values_df.index, portfolio_values_df['value'], label='Portfolio Value', linewidth=2)
    plt.title('M&A Strategy Portfolio Value Over Time')
    plt.xlabel('Date')
    plt.ylabel('Value (USD)')
    plt.legend()
    plt.grid(True)
    plt.show()

if __name__ == "__main__":
    main()
