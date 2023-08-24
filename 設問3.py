import pandas as pd

input_filepath = 'D:/Hokudai_Stuff/Job Hunting stuff/Entry essays/network_log.csv'
output_filepath = 'D:/Hokudai_Stuff/Job Hunting stuff/Entry essays/' + 'output_Q3.csv'

# Convert log file into dataframe
# ログファイル を　dataframe に変更
df = pd.read_csv(input_filepath, header=None)
print(df.head(10))

# Create a list to keep track of servers
# 各サーバーのIPアドレスを記録するためにリスト作成
servers_list = []
# Create a list to keep track of timed out servers
# pingがタイムアウトしたサーバーを記録するためにリスト作成
servers_timedout = []
# Create a list to keep track of overloaded servers
# 過負荷したサーバーを記録するためにリスト作成
servers_overloaded = []


# Number of instances from which average is calculated
# 直近　m回
m = 10
# Overload ping limit (ms)
# tミリ秒を超えたら過負荷したとみなす
t = 50

# Create an output dataframe to store instances of ping for each IP address
# 各PINGを記録するために 空のdataframeを作成
index_list = list(range(m + 1))
pinginstances_df = pd.DataFrame(index=index_list)
print(pinginstances_df)


# Create an output dataframe to store start and end times for overload
# 過負荷したサーバーを記録するために 空のdataframeを作成
overload_df = pd.DataFrame(columns=['IPv4 Address', 'Overload Start', 'Overload End', 'Instances', 'Overload Duration'])
print(overload_df)
output_index = 0

# Scan logs (latest m times)
# ログファイル　を　スキャン
for x in df.index:
    IPv4_Address = df.iloc[x, 1]
    Ping = df.iloc[x, 2]

    # Update existing instances for existing IP addresses
    # PING記録のdataframeに既存しているアドレスのPING回数を更新
    if Ping != '-' and IPv4_Address in servers_list:

        # Update count number
        # 回数を更新
        pinginstances_df.loc[0, IPv4_Address] = pinginstances_df.loc[0, IPv4_Address] + 1
        count = pinginstances_df.loc[0, IPv4_Address]

        # Update ping value to instances
        # PING値を更新
        if count%m == 0:
            pinginstances_df.loc[m, IPv4_Address] = Ping
        else:
            pinginstances_df.loc[count%m, IPv4_Address] = Ping

        # Calculate Average Ping
        # 平均応答時間を計算
        total_ping = 0
        total_instance = m
        for y in list(range(1, m+1)):
            if pinginstances_df.loc[y, IPv4_Address] == 0:
                total_instance -= 1
            else:
                total_ping = total_ping + int(pinginstances_df.loc[y, IPv4_Address])

        average_ping = total_ping/total_instance

        # When server is overloaded for multiple instances
        # サーバーがすでに過負荷した場合
        if average_ping >= t and IPv4_Address in servers_overloaded:
            # Update information of overloaded servers
            # 過負荷したサーバーの情報を更新
            for y in overload_df.index:
                if overload_df.loc[y, 'IPv4 Address'] == IPv4_Address and pd.isnull(overload_df.loc[y, 'Overload End']) == True:
                    overload_df.loc[y, 'Instances'] += 1
        # For when server is overloaded for the first time
        # サーバーが過負荷した状態ではない
        elif average_ping >= t and IPv4_Address not in servers_overloaded:
            # Update list of overloaded servers
            # 過負荷したサーバーのリストを更新
            servers_overloaded.append(IPv4_Address)
            # Add information to output dataframe
            # 過負荷したサーバーの情報をdataframeに追加
            overload_df.loc[output_index, 'IPv4 Address'] = IPv4_Address
            overload_df.loc[output_index, 'Overload Start'] = df.iloc[x, 0]
            overload_df.loc[output_index, 'Instances'] = 1
            output_index = output_index + 1

        # For when overloaded server falls becomes normal
        # 過負荷したサーバーが正常に戻った場合
        elif average_ping < t and IPv4_Address in servers_overloaded:

            # Add information to output dataframe at the correct instance/address
            # 過負荷したサーバーの情報を正しい位置に追加
            for y in overload_df.index:

                if overload_df.loc[y, 'IPv4 Address'] == IPv4_Address and pd.isnull(overload_df.loc[y, 'Overload End']) == True:
                    overload_df.loc[y, 'Overload End'] = df.iloc[x, 0]

                    # Update list of overloaded servers
                    # 過負荷したサーバーのリストを更新
                    servers_overloaded.remove(IPv4_Address)

    # Add new IP address to df
    # PING記録のdataframeに新アドレスを追加
    elif Ping != '-' and IPv4_Address not in servers_list:
        pinginstances_df[IPv4_Address] = 0
        pinginstances_df.loc[0, IPv4_Address] = 1
        pinginstances_df.loc[1, IPv4_Address] = Ping
        # Update server list
        # サーバーリスト更新
        servers_list.append(IPv4_Address)

# Calculate Overload Duration
# 過負荷期間を計算
for y in overload_df.index:

    Overload_Start = overload_df.loc[y, 'Overload Start']
    Overload_End = overload_df.loc[y, 'Overload End']

    # Server still overloaded
    # サーバーがまだ過負荷状態
    if pd.isnull(Overload_End):
        overload_df.loc[y, 'Overload Duration'] = 'Server_Overload'

    else:
        # Calculate Number of Years
        # 年を計算
        years = Overload_End // (10 ** 10) - Overload_Start // (10 ** 10)

        # Calculate Number of Months
        # 月を計算
        months = Overload_End // (10 ** 8) % (10 ** 2) - Overload_Start // (10 ** 8) % (10 ** 2)
        if months <= 0 and years >= 1:
            months = 12 + months
            years = years - 1

        # Calculate Number of Days
        # 日を計算
        days = Overload_End // (10 ** 6) % (10 ** 2) - Overload_Start // (10 ** 6) % (10 ** 2)
        if days <= 0 and months >= 1:
            days_30 = {4, 6, 9, 11}

            # Account for February
            # 2月の場合
            if Overload_End // (10 ** 6) % (10 ** 2) == 2:

                # Account for Leap Year
                # うるう年の場合
                if Overload_End // (10 ** 8) % (10 ** 2) % 4 != 0:
                    days = 28 + days
                    months = months - 1
                else:
                    days = 29 + days
                    months = months - 1

            # Account for Months with 30 days
            # 月に30日がある場合
            if Overload_End // (10 ** 6) % (10 ** 2) in days_30:
                days = 30 + days
                months = months - 1

            # Rest of the months with 31 days
            # 他の月の場合
            else:
                days = 31 + days
                months = months - 1

        # Calculate Number of Hours
        # 時を計算
        hours = Overload_End // (10 ** 4) % (10 ** 2) - Overload_Start // (10 ** 4) % (10 ** 2)
        if hours <= 0 and days >= 1:
            hours = 24 + hours
            days = days - 1

        # Calculate Number of Minutes
        # 分を計算
        minutes = Overload_End // (10 ** 2) % (10 ** 2) - Overload_Start // (10 ** 2) % (10 ** 2)
        if minutes <= 0 and hours >= 1:
            minutes = 60 + minutes
            hours = hours - 1

        # Calculate Number of Seconds
        # 秒を計算
        seconds = Overload_End % (10 ** 2) - Overload_Start % (10 ** 2)
        if seconds <= 0 and minutes >= 1:
            seconds = 60 + minutes
            minutes = minutes - 1

        overload_df.loc[y, 'Overload Duration'] = seconds + minutes*(10**2) + hours*(10**4) + days*(10**6) + months*(10**8) + years*(10**10)

        # Fill Overload Duration with leading zeros
        # 14桁になるまで前をゼロで埋める
        overload_df['Overload Duration'] = overload_df['Overload Duration'].map('{:0>14}'.format)


print(pinginstances_df)
print(overload_df)

# Export dataframe to csv
# アウトプットのdataframe を csv ファイルに保存
overload_df.to_csv(output_filepath, index=None)