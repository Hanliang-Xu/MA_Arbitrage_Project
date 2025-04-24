import numpy as np
import pandas as pd

def compute_sharpe_ratio(portfolio_values, risk_free_rate=0.0):
    # Assuming portfolio_values is a pandas Series of daily portfolio value
    daily_returns = portfolio_values.pct_change().dropna()
    excess_returns = daily_returns - risk_free_rate / 252
    return np.sqrt(252) * excess_returns.mean() / excess_returns.std()

def compute_max_drawdown(portfolio_values):
    # Calculate the maximum drawdown
    cum_max = portfolio_values.cummax()
    drawdown = (portfolio_values - cum_max) / cum_max
    return drawdown.min()
