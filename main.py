import pandas as pd
import matplotlib.pyplot as plt
from strategy import generate_orders_from_deals
from backtester import backtest
from stats_utils import compute_cagr, compute_sharpe_ratio, compute_max_drawdown
from report_generator import save_portfolio_report_csv, save_portfolio_report_html

def main():
    # 1) Load price data and extract trading dates
    price_df = pd.read_csv("price.csv", parse_dates=["date"])
    
    trading_dates = sorted(price_df['date'].unique())

    # 2) Generate orders from deals using the strategy module
    orders_df = generate_orders_from_deals(
        deals_csv_path="deals.csv",
        trading_dates=trading_dates,
        shares_on_announce=300,
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

    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(14, 6), sharex=True)

    # Plot 1: Total Portfolio Value
    axes[0].plot(portfolio_values_df.index, portfolio_values_df['value'], label='Portfolio Value', linewidth=2)
    axes[0].set_title('Portfolio Value Over Time')
    axes[0].set_xlabel('Date')
    axes[0].set_ylabel('USD')
    axes[0].grid(True)
    axes[0].legend()

    # Plot 2: Invested Capital
    axes[1].plot(portfolio_values_df.index, portfolio_values_df['invested_capital'], label='Invested Capital', linewidth=2, color='orange')
    axes[1].set_title('Invested Capital Over Time')
    axes[1].set_xlabel('Date')
    axes[1].set_ylabel('USD')
    axes[1].grid(True)
    axes[1].legend()

    plt.tight_layout()
    plt.show()

    # 6) Generate a daily portfolio document
    save_portfolio_report_csv(portfolio_values_df, "daily_portfolio_report.csv")
    save_portfolio_report_html(portfolio_values_df, "daily_portfolio_report.html")


if __name__ == "__main__":
    main()
