"""
@author: Bowen Zhang
@software: pycharm
@file: test.py
@time: 2023/8/31 9:24
@desc:
"""

import datetime

import pandas as pd

import data.bstock_utils as bs
import numpy as np

import strategy.base as strat

from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split


stock = "sh.601166"
data = bs.get_csv_data(stock,"price")
prices = data['open'].values.reshape(-1,1)

# Normalize the data
scaler = MinMaxScaler()
normalized_prices = scaler.fit_transform(prices)

# Create sequences of 30 days as input and predict the 31st day
X = []
y = []
for i in range(30, len(normalized_prices)):
    X.append(normalized_prices[i - 30:i])
    y.append(normalized_prices[i])

X, y = np.array(X), np.array(y)

# Split data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)