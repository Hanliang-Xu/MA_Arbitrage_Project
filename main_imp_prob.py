import pandas as pd
import matplotlib.pyplot as plt
from strategy_imp_prob import generate_orders_from_deals  # your current module
from backtester import backtest
from stats_utils import compute_cagr, compute_sharpe_ratio, compute_max_drawdown
from report_generator import save_portfolio_report_csv, save_portfolio_report_html

def main():
    price_df = pd.read_csv("price.csv", parse_dates=["date"])
    trading_dates = sorted(price_df['date'].unique())

    orders_df = generate_orders_from_deals(
        deals_csv_path="deals.csv",
        trading_dates=trading_dates,
        price_history_df=price_df,
        shares_on_announce=300,
        min_prob_threshold=0.75,
        scale_with_probability=True
    )

    # 3) Run backtest on generated orders
    portfolio_values_df = backtest(
        orders_df=orders_df,
        price_df=price_df,
        initial_capital=1_000_000
    )

    # 4) Compute and print performance metrics
    portfolio_series = portfolio_values_df['value']
    start_date = portfolio_series.index.min().strftime("%Y-%m-%d")
    end_date = portfolio_series.index.max().strftime("%Y-%m-%d")

    cagr = compute_cagr(portfolio_series, start_date, end_date)
    sharpe = compute_sharpe_ratio(portfolio_series)
    max_dd = compute_max_drawdown(portfolio_series)

    print("Performance Report:")
    print(f"- CAGR:           {cagr:.2%}")
    print(f"- Sharpe Ratio:   {sharpe:.2f}")
    print(f"- Max Drawdown:   {max_dd:.2%}")

    # 5) Plot portfolio performance
    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(14, 6), sharex=True)

    axes[0].plot(portfolio_values_df.index, portfolio_values_df['value'], label='Portfolio Value', linewidth=2)
    axes[0].set_title('Portfolio Value Over Time')
    axes[0].set_xlabel('Date')
    axes[0].set_ylabel('USD')
    axes[0].grid(True)
    axes[0].legend()

    axes[1].plot(portfolio_values_df.index, portfolio_values_df['invested_capital'], label='Invested Capital', color='orange', linewidth=2)
    axes[1].set_title('Invested Capital Over Time')
    axes[1].set_xlabel('Date')
    axes[1].set_ylabel('USD')
    axes[1].grid(True)
    axes[1].legend()

    plt.tight_layout()
    plt.savefig("portfolio_performance_imp_prob.png")
    plt.show()

    # 6) Export daily portfolio report
    save_portfolio_report_csv(portfolio_values_df, "daily_portfolio_report_imp_prob.csv")
    save_portfolio_report_html(portfolio_values_df, "daily_portfolio_report_imp_prob.html")

if __name__ == "__main__":
    main()
