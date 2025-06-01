import pandas as pd

# Load data
df = pd.read_csv("market_data_raw.csv")
df["datetime"] = pd.to_datetime(df["currentdate"] + " " + df["currenttime"], format="%m/%d/%Y %H:%M:%S")
df.drop(columns=["Unnamed: 0"], inplace=True, errors="ignore")
df = df.sort_values(by=["symbol", "datetime"])

# Parameters
target_pct = 0.005   # +0.5%
stop_pct = 0.0025    # -0.25%

# Prepare to log trades
all_trades = []

# Loop over each symbol
for symbol in df["symbol"].unique():
    symbol_data = df[df["symbol"] == symbol]
    
    # Loop over each trading day
    for date in symbol_data["currentdate"].unique():
        day_data = symbol_data[symbol_data["currentdate"] == date].copy()
        day_data = day_data.sort_values("datetime")

        # Skip if there's not enough data
        if day_data.empty or len(day_data) < 2:
            continue

        # Get the opening candle
        opening_candle = day_data.iloc[0]
        opening_high = opening_candle["high"]
        opening_low = opening_candle["low"]
        opening_time = opening_candle["datetime"].time()

        buy_trade = None
        sell_trade = None
        buy_trade_executed = False
        sell_trade_executed = False

        for _, row in day_data.iterrows():
            time = row["datetime"]
            close = row["close"]
            high = row["high"]
            low = row["low"]

            # Skip the opening candle
            if time.time() == opening_time:
                continue

            # BUY ENTRY
            if not buy_trade_executed and buy_trade is None and high > opening_high:
                entry_price = close
                buy_trade = {
                    "symbol": symbol, "date": date, "direction": "BUY",
                    "entry_time": time, "entry_price": entry_price,
                    "target_price": entry_price * (1 + target_pct),
                    "stop_price": entry_price * (1 - stop_pct)
                }

            # SELL ENTRY
            if not sell_trade_executed and sell_trade is None and low < opening_low:
                entry_price = close
                sell_trade = {
                    "symbol": symbol, "date": date, "direction": "SELL",
                    "entry_time": time, "entry_price": entry_price,
                    "target_price": entry_price * (1 - target_pct),
                    "stop_price": entry_price * (1 + stop_pct)
                }

            

            # TARGET / STOP CHECK
            for trade in [buy_trade, sell_trade]:
                if trade and "exit_time" not in trade:
                    if trade["direction"] == "BUY":
                        if close >= trade["target_price"]:
                            trade["exit_time"] = time
                            trade["exit_price"] = trade["target_price"]
                            trade["exit_reason"] = "target_hit"
                            pnl_raw = trade["target_price"] - trade["entry_price"]
                            trade["pnl"] = round(pnl_raw / trade["entry_price"], 8)
                            all_trades.append(trade)
                            buy_trade = None  # Reset after processing
                            buy_trade_executed = True
                        elif close <= trade["stop_price"]:
                            trade["exit_time"] = time
                            trade["exit_price"] = trade["stop_price"]
                            trade["exit_reason"] = "stop_loss"
                            pnl_raw = trade["stop_price"] - trade["entry_price"]
                            trade["pnl"] = round(pnl_raw / trade["entry_price"], 8)
                            all_trades.append(trade)
                            buy_trade = None  # Reset after processing
                            buy_trade_executed = True

                    else:  # SELL
                        if close <= trade["target_price"]:
                            trade["exit_time"] = time
                            trade["exit_price"] = trade["target_price"]
                            trade["exit_reason"] = "target_hit"
                            pnl_raw = trade["entry_price"] - trade["target_price"]
                            trade["pnl"] = round(pnl_raw / trade["entry_price"], 8)
                            all_trades.append(trade)
                            sell_trade = None  # Reset after processing
                            sell_trade_executed = True
                        elif close >= trade["stop_price"]:
                            trade["exit_time"] = time
                            trade["exit_price"] = trade["stop_price"]
                            trade["exit_reason"] = "stop_loss"
                            pnl_raw = trade["entry_price"] - trade["stop_price"]
                            trade["pnl"] = round(pnl_raw / trade["entry_price"], 8)
                            all_trades.append(trade)
                            sell_trade = None
                            sell_trade_executed = True

            # TIME EXIT at 15:15
            if time.time().strftime("%H:%M") == "15:15":
                for trade in [buy_trade, sell_trade]:
                    if trade and "exit_time" not in trade:
                        trade["exit_time"] = time
                        trade["exit_price"] = close
                        trade["exit_reason"] = "time_exit"
                        pnl_raw = (close - trade["entry_price"]
                                            if trade["direction"] == "BUY"
                                            else trade["entry_price"] - close)
                        trade["pnl"] = round(pnl_raw / trade["entry_price"], 8)
                        all_trades.append(trade)
                        if trade["direction"] == "BUY":
                            buy_trade = None
                            buy_trade_executed = True
                        else:
                            sell_trade = None
                            sell_trade_executed = True

                break

    print(f"Done for symbol: {symbol}")

print("All trades processed.")
# Final results

results_df = pd.DataFrame(all_trades)
print(results_df[["symbol", "date", "direction", "entry_time", "entry_price", "exit_time", "exit_price", "exit_reason", "pnl"]])

print("Writing to file")
# Optional: save to CSV
results_df.to_csv("Selected_ORB_strategy.csv", index=False)


