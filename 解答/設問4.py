import pandas as pd

input_filepath = 'D:/Hokudai_Stuff/Job Hunting stuff/Entry essays/network_log.csv'
subnetkey_filepath = 'D:/Hokudai_Stuff/Job Hunting stuff/Entry essays/subnetkey.csv'
output_filepath1 = 'D:/Hokudai_Stuff/Job Hunting stuff/Entry essays/' + 'output_Q4_1.csv'
output_filepath2 = 'D:/Hokudai_Stuff/Job Hunting stuff/Entry essays/' + 'output_Q4_2.csv'

# Convert log file into dataframe
# ログファイル を　dataframe に変更
df = pd.read_csv(input_filepath, header=None)
print(df.head(10))

# Convert subnet key into dataframe
# サブネットの情報を　dataframe に変更
subnetkey_df = pd.read_csv(subnetkey_filepath, header=None, index_col=0)
# Replace NaN with zeros in dataframe
# dataframe 内のNaNを0に変換
subnetkey_df = subnetkey_df.fillna(0)

# Create a list to keep track of timed out servers
# サーバーとサブネットを記録するためにリストを作成
servers_timedout = []
servers_down = []
subnet_down = []

# Create an output dataframe to store start and end times for timeout
# pingがタイムアウトしたサーバーを記録するために 空のdataframeを作成
timeout_df = pd.DataFrame(columns=['IPv4 Address', 'Timeout Start', 'Timeout End', 'Instances', 'Timeout Duration'])
print(timeout_df)
timeout_index = 0

# Create an output dataframe to store start and end times for subnet downs
# 故障したサブネットを記録するために 空のdataframeを作成
subnet_down_df = pd.DataFrame(columns=['Subnet', 'Timeout Start', 'Timeout End', 'Instances', 'Timeout Duration'])
subnet_down_index = 0

# Number of timeouts to be considered server down
# それ以上連続してタイムアウトした場合故障とみなす
N = 2

# Scan logs
# ログファイル　を　スキャン
for x in df.index:
    IPv4_Address = df.iloc[x, 1]
    Ping = df.iloc[x, 2]

    # When server is timed out for multiple instances
    # サーバーがすでにタイムアウトした場合
    if Ping == '-' and IPv4_Address in servers_timedout:

        # Update dataframe of timed out servers
        # dataframeを更新
        for y in timeout_df.index:
            if timeout_df.loc[y, 'IPv4 Address'] == IPv4_Address and pd.isnull(timeout_df.loc[y, 'Timeout End']) == True:
                timeout_df.loc[y, 'Instances'] += 1
                print(timeout_df)
            # Update list of servers down
            # リスト更新
            if timeout_df.loc[y, 'Instances'] >= N and IPv4_Address not in servers_down:
                servers_down.append(IPv4_Address)
                print(f'servers down: {servers_down}')
                # Check if down servers are part of subnet
                # 新たなサブネットが故障しているかを確認
                for z in subnetkey_df.index:
                    l = subnetkey_df.loc[z].tolist()
                    l = [a for a in l if a != 0]

                    # If part of subnet
                    # 新たなサブネットが故障している場合
                    if set(l).issubset(servers_down) == True and z not in subnet_down:
                        # Update list of down subnets
                        # リスト更新
                        subnet_down.append(z)
                        # Add information to subnet dataframe
                        # dataframeを更新
                        subnet_down_df.loc[subnet_down_index, 'Subnet'] = z
                        subnet_down_df.loc[subnet_down_index, 'Timeout Start'] = df.iloc[x, 0]
                        subnet_down_df.loc[subnet_down_index, 'Instances'] = 1
                        subnet_down_index = subnet_down_index + 1
                        print(subnet_down_df)

    # For when server is timed out for the first time
    # サーバーがタイムアウトしていない場合
    elif Ping == '-' and IPv4_Address not in servers_timedout:
        # Update list of timed out servers
        # リスト更新
        servers_timedout.append(IPv4_Address)
        print(f'added: {servers_timedout}')

        # Special case if N = 1
        # N=1の場合
        if N == 1:
            servers_down.append(IPv4_Address)
        # Add information to timeout dataframe
        # dataframeに情報を追加
        timeout_df.loc[timeout_index, 'IPv4 Address'] = IPv4_Address
        timeout_df.loc[timeout_index, 'Timeout Start'] = df.iloc[x, 0]
        timeout_df.loc[timeout_index, 'Instances'] = 1
        timeout_index = timeout_index + 1
        print(timeout_df)

    # For when timed out server reconnects
    # サーバーが復活した時
    elif Ping != '-' and IPv4_Address in servers_timedout:

        # Add information to timeout dataframe at the correct instance/address
        # 情報をdataframeの正しい位置に追加
        for y in timeout_df.index:

            # When the number of disconnect counts exceed N
            # タイムアウト回数がN以上
            if timeout_df.loc[y, 'Instances'] >= N and timeout_df.loc[y, 'IPv4 Address'] == IPv4_Address and pd.isnull(timeout_df.loc[y, 'Timeout End']) == True:
                timeout_df.loc[y, 'Timeout End'] = df.iloc[x, 0]

                # Update list of timed out servers
                # リスト更新
                servers_timedout.remove(IPv4_Address)
                print(f'condition fufilled removed: {servers_timedout}')
                # Update list of downed servers
                # リスト更新
                servers_down.remove(IPv4_Address)

                # Check if down servers are part of subnet
                # 新たなサブネットが故障しなくなったかを確認
                for z in subnetkey_df.index:
                    l = subnetkey_df.loc[z].tolist()
                    l = [a for a in l if a != 0]

                    # If part of subnet
                    # 新たなサブネットが故障しなくなった場合
                    if set(l).issubset(servers_down) == False and z in subnet_down:
                        # Update list of down subnets
                        # リスト更新
                        subnet_down.append(z)
                        # Add information to subnet dataframe
                        # リスト更新
                        for xy in subnet_down_df.index:
                            if subnet_down_df.loc[xy, 'Subnet'] == z and pd.isnull(subnet_down_df.loc[xy, 'Timeout End']) == True:
                                subnet_down_df.loc[xy, 'Timeout End'] = df.iloc[x, 0]

            # When the number of disconnect counts does NOT exceed N
            # タイムアウト回数 < N
            elif timeout_df.loc[y, 'IPv4 Address'] == IPv4_Address and pd.isnull(timeout_df.loc[y, 'Timeout End']) == True:
                timeout_df = timeout_df.drop(index=y)

                # Update list of timed out servers
                # リスト更新
                servers_timedout.remove(IPv4_Address)
                print(f'condition not fufilled removed: {servers_timedout}')

    print(servers_timedout)

# Create function to calculate Timeout Duration
# 故障期間を計算する機能を作る
def calc_duration(df):

    for y in df.index:

        Timeout_Start = df.loc[y, 'Timeout Start']
        Timeout_End = df.loc[y, 'Timeout End']

        if pd.isnull(Timeout_End) == None:
            df.loc[y, 'Timeout Duration'] = 'Duration_undefined'

        else:
            # Calculate Number of Years
            years = Timeout_End//(10**10) - Timeout_Start//(10**10)

            # Calculate Number of Months
            months = Timeout_End//(10**8) % (10**2) - Timeout_Start//(10**8) % (10**2)
            if months <= 0 and years >= 1:
                months = 12 + months
                years = years - 1

            # Calculate Number of Days
            days = Timeout_End//(10**6) % (10**2) - Timeout_Start//(10**6) % (10**2)
            if days <= 0 and months >= 1:
                days_30 = {4, 6, 9, 11}

                # Account for February
                if Timeout_End//(10**6) % (10**2) == 2:

                    # Account for Leap Year
                    if Timeout_End//(10**8) % (10**2) % 4 != 0:
                        days = 28 + days
                        months = months - 1
                    else:
                        days = 29 + days
                        months = months - 1

                # Account for Months with 30 days
                elif Timeout_End // (10**6) % (10**2) in days_30:
                    days = 30 + days
                    months = months - 1

                # Rest of the months with 31 days
                else:
                    days = 31 + days
                    months = months - 1

            # Calculate Number of Hours
            hours = Timeout_End//(10**4) % (10**2) - Timeout_Start//(10**4) % (10**2)
            if hours <= 0 and days >= 1:
                hours = 24 + hours
                days = days - 1

            # Calculate Number of Minutes
            minutes = Timeout_End//(10**2) % (10**2) - Timeout_Start//(10**2) % (10**2)
            if minutes <= 0 and hours >= 1:
                minutes = 60 + minutes
                hours = hours - 1

            # Calculate Number of Seconds
            seconds = Timeout_End % (10**2) - Timeout_Start % (10**2)
            if seconds <= 0 and minutes >= 1:
                seconds = 60 + minutes
                minutes = minutes - 1

            df.loc[y, 'Timeout Duration'] = seconds + minutes*(10**2) + hours*(10**4) + days*(10**6) + months*(10**8) + years*(10**10)

            # Fill Timeout Duration with leading zeros
            df['Timeout Duration'] = df['Timeout Duration'].map('{:0>14}'.format)
    return df

# Calculate duration
# 期間を計算
calc_duration(timeout_df)
calc_duration(subnet_down_df)


print(timeout_df)
print(subnet_down_df)

# Export dataframe to csv
# アウトプットのdataframe を csv ファイルに保存
timeout_df.to_csv(output_filepath1, index=None)
subnet_down_df.to_csv(output_filepath2, index=None)