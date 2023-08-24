import pandas as pd

input_filepath = 'D:/Hokudai_Stuff/Job Hunting stuff/Entry essays/network_log.csv'
output_filepath = 'D:/Hokudai_Stuff/Job Hunting stuff/Entry essays/' + 'output_Q1.csv'

# Convert log file into dataframe
# ログファイル を　dataframe に変更
df = pd.read_csv(input_filepath, header=None)
print(df.head(10))

# Create a list to keep track of timed out servers　
# pingがタイムアウトしたサーバーを記録するためにリスト作成
servers_timedout = []

# Create an output dataframe to store start and end times for timeout
# アウトプットを記録するために 空のdataframeを作成
output_df = pd.DataFrame(columns=['IPv4 Address', 'Timeout Start', 'Timeout End', 'Timeout Duration'])
output_index = 0

# Scan logs
# ログファイル　を　スキャン
for x in df.index:

    # For when server is timed out　
    # サーバーがタイムアウトした時
    if df.iloc[x, 2] == '-' and df.iloc[x, 1] not in servers_timedout:

        # Update list of timed out servers
        # リスト更新
        servers_timedout.append(df.iloc[x, 1])

        # Add information to output dataframe
        # 情報をアウトプットdataframeに追加
        output_df.loc[output_index, 'IPv4 Address'] = df.iloc[x, 1]
        output_df.loc[output_index, 'Timeout Start'] = df.iloc[x, 0]
        output_index = output_index + 1

    # For when timed out server reconnects
    # タイムアウトしたサーバーがまた繋がる時
    if df.iloc[x, 2] != '-' and df.iloc[x, 1] in servers_timedout:

        # Update list of timed out servers
        # リスト更新
        servers_timedout.remove(df.iloc[x, 1])

        # Add information to output dataframe at the correct instance/address
        # 情報を正しい位置に追加
        for y in output_df.index:
            if output_df.loc[y, 'IPv4 Address'] == df.iloc[x, 1] and pd.isnull(output_df.loc[y, 'Timeout End']) == True:
                output_df.loc[y, 'Timeout End'] = df.iloc[x, 0]


# Calculate Timeout Duration
# 故障期間を計算
for y in output_df.index:

    Timeout_Start = output_df.loc[y, 'Timeout Start']
    Timeout_End = output_df.loc[y, 'Timeout End']

    if pd.isnull(Timeout_End) == True:
        output_df.loc[y, 'Timeout Duration'] = 'Undefined'

    else:
        # Calculate Number of Years
        # 年を計算
        years = Timeout_End//(10**10) - Timeout_Start//(10**10)

        # Calculate Number of Months
        # 月を計算
        months = Timeout_End//(10**8) % (10**2) - Timeout_Start//(10**8) % (10**2)
        if months <= 0 and years >= 1:
            months = 12 + months
            years = years - 1

        # Calculate Number of Days
        # 日を計算
        days = Timeout_End//(10**6) % (10**2) - Timeout_Start//(10**6) % (10**2)
        if days <= 0 and months >= 1:
            days_30 = {4, 6, 9, 11}

            # Account for February
            # 2月の場合
            if Timeout_End//(10**6) % (10**2) == 2:

                # Account for Leap Year
                # うるう年の場合
                if Timeout_End//(10**8) % (10**2) % 4 != 0:
                    days = 28 + days
                    months = months - 1
                else:
                    days = 29 + days
                    months = months - 1

            # Account for Months with 30 days
            # 月に30日がある場合
            if Timeout_End // (10**6) % (10**2) in days_30:
                days = 30 + days
                months = months - 1

            # Rest of the months with 31 days
            # 他の月の場合
            else:
                days = 31 + days
                months = months - 1

        # Calculate Number of Hours
        # 時を計算
        hours = Timeout_End//(10**4) % (10**2) - Timeout_Start//(10**4) % (10**2)
        if hours <= 0 and days >= 1:
            hours = 24 + hours
            days = days - 1

        # Calculate Number of Minutes
        # 分を計算
        minutes = Timeout_End//(10**2) % (10**2) - Timeout_Start//(10**2) % (10**2)
        if minutes <= 0 and hours >= 1:
            minutes = 60 + minutes
            hours = hours - 1

        # Calculate Number of Seconds
        # 秒を計算
        seconds = Timeout_End % (10**2) - Timeout_Start % (10**2)
        if seconds <= 0 and minutes >= 1:
            seconds = 60 + minutes
            minutes = minutes - 1

        output_df.loc[y, 'Timeout Duration'] = seconds + minutes*(10**2) + hours*(10**4) + days*(10**6) + months*(10**8) + years*(10**10)

        # Fill Timeout Duration with leading zeros
        # 14桁になるまで前をゼロで埋める
        output_df['Timeout Duration'] = output_df['Timeout Duration'].map('{:0>14}'.format)


# print(servers_timedout)
print(output_df)

# Export dataframe to csv
# アウトプットのdataframe を csv ファイルに保存
output_df.to_csv(output_filepath, index=None)
