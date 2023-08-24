import pandas as pd


input_filepath = 'D:/Hokudai_Stuff/Job Hunting stuff/Entry essays/network_log.csv'
output_filepath = 'D:/Hokudai_Stuff/Job Hunting stuff/Entry essays/' + 'output_Q2.csv'

# Convert log file into dataframe
df = pd.read_csv(input_filepath, header=None)
print(df.head(10))

# Create a list to keep track of timed out servers
servers_timedout = []

# Create an output dataframe to store start and end times for timeout
timeout_df = pd.DataFrame(columns=['IPv4 Address', 'Timeout Start', 'Timeout End', 'Instances', 'Timeout Duration'])
print(timeout_df)
timeout_index = 0

# Number of timeouts to be considered server down
N = 2

# Scan logs
for x in df.index:
    IPv4_Address = df.iloc[x, 1]
    Ping = df.iloc[x, 2]
    # For when server is timed out for the first time
    if Ping == '-' and IPv4_Address not in servers_timedout:

        # Update list of timed out servers
        servers_timedout.append(IPv4_Address)
        print(f'added: {servers_timedout}')
        # Add information to timeout dataframe
        timeout_df.loc[timeout_index, 'IPv4 Address'] = IPv4_Address
        timeout_df.loc[timeout_index, 'Timeout Start'] = df.iloc[x, 0]
        timeout_df.loc[timeout_index, 'Instances'] = 1
        timeout_index = timeout_index + 1
        print(timeout_df)
        # timeout_df = timeout_df.append(pd.DataFrame([df.iloc[x, 0]], columns='Timeout Start'))

    # When server is timed out for multiple instances
    elif Ping == '-' and IPv4_Address in servers_timedout:

        # Update list of timed out servers
        for y in timeout_df.index:
            if timeout_df.loc[y, 'IPv4 Address'] == IPv4_Address and pd.isnull(timeout_df.loc[y, 'Timeout End']) == True:
                timeout_df.loc[y, 'Instances'] += 1

    # For when timed out server reconnects
    elif Ping != '-' and IPv4_Address in servers_timedout:

        # Add information to timeout dataframe at the correct instance/address
        for y in timeout_df.index:

            # When the number of disconnect counts exceed N
            if timeout_df.loc[y, 'Instances'] >= N and timeout_df.loc[y, 'IPv4 Address'] == IPv4_Address and pd.isnull(timeout_df.loc[y, 'Timeout End']) == True:
                timeout_df.loc[y, 'Timeout End'] = df.iloc[x, 0]

                # Update list of timed out servers
                servers_timedout.remove(IPv4_Address)
                print(f'condition fufilled removed: {servers_timedout}')

        # When the number of disconnect counts does NOT exceed N
            elif timeout_df.loc[y, 'IPv4 Address'] == IPv4_Address and pd.isnull(timeout_df.loc[y, 'Timeout End']) == True:
                timeout_df = timeout_df.drop(index=y)

            # Update list of timed out servers
                servers_timedout.remove(IPv4_Address)
                print(f'condition not fufilled removed: {servers_timedout}')

    print(servers_timedout)
# Calculate Timeout Duration
for y in timeout_df.index:

    Timeout_Start = timeout_df.loc[y, 'Timeout Start']
    Timeout_End = timeout_df.loc[y, 'Timeout End']
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

    timeout_df.loc[y, 'Timeout Duration'] = seconds + minutes*(10**2) + hours*(10**4) + days*(10**6) + months*(10**8) + years*(10**10)

# Fill Timeout Duration with leading zeros
timeout_df['Timeout Duration'] = timeout_df['Timeout Duration'].map('{:0>14}'.format)


# print(servers_timedout)
print(timeout_df)

# Export dataframe to csv
timeout_df.to_csv(output_filepath, index=None)