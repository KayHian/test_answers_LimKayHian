import pandas as pd


input_filepath = 'D:/Hokudai_Stuff/Job Hunting stuff/Entry essays/network_log.csv'
output_filepath = 'D:/Hokudai_Stuff/Job Hunting stuff/Entry essays/' + 'output_Q3.csv'

# Convert log file into dataframe
df = pd.read_csv(input_filepath, header=None)
print(df.head(10))

# Create a list to keep track of servers
servers_list = []
# Create a list to keep track of timed out servers
servers_timedout = []
# Create a list to keep track of overloaded servers
servers_overloaded = []


# Number of instances from which average is calculated
m = 10
# Overload ping limit (ms)
t = 50

# Create an output dataframe to store instances of ping for each IP address
index_list = list(range(m + 1))
pinginstances_df = pd.DataFrame(index=index_list)
print(pinginstances_df)


# Create an output dataframe to store start and end times for overload
overload_df = pd.DataFrame(columns=['IPv4 Address', 'Overload Start', 'Overload End', 'Instances', 'Overload Duration'])
print(overload_df)
output_index = 0

# Scan logs (latest m times)
for x in df.index:
    IPv4_Address = df.iloc[x, 1]
    Ping = df.iloc[x, 2]

    # Update existing instances for existing IP addresses
    if Ping != '-' and IPv4_Address in servers_list:

        # Update count number
        print(pinginstances_df)
        pinginstances_df.loc[0, IPv4_Address] = pinginstances_df.loc[0, IPv4_Address] + 1
        count = pinginstances_df.loc[0, IPv4_Address]
        print(count)
        # Update ping value to instances
        if count%m == 0:
            pinginstances_df.loc[m, IPv4_Address] = Ping
        else:
            pinginstances_df.loc[count%m, IPv4_Address] = Ping

        # Calculate Average Ping
        total_ping = 0
        total_instance = m
        for y in list(range(1, m+1)):
            if pinginstances_df.loc[y, IPv4_Address] == 0:
                total_instance -= 1
            else:
                total_ping = total_ping + int(pinginstances_df.loc[y, IPv4_Address])
            # print(f'total ping: {total_ping}')

        average_ping = total_ping/total_instance
        print(average_ping)

        # When server is overloaded for multiple instances
        if average_ping >= t and IPv4_Address in servers_overloaded:
            # Update list of timed out servers
            for y in overload_df.index:
                if overload_df.loc[y, 'IPv4 Address'] == IPv4_Address and pd.isnull(overload_df.loc[y, 'Overload End']) == True:
                    overload_df.loc[y, 'Instances'] += 1
                # For when server is overloaded for the first time
        elif average_ping >= t and IPv4_Address not in servers_overloaded:
            # Update list of timed out servers
            servers_overloaded.append(IPv4_Address)
            print(f'added: {servers_overloaded}')
            # Add information to output dataframe
            overload_df.loc[output_index, 'IPv4 Address'] = IPv4_Address
            overload_df.loc[output_index, 'Overload Start'] = df.iloc[x, 0]
            overload_df.loc[output_index, 'Instances'] = 1
            output_index = output_index + 1
            print(overload_df)
            # overload_df = overload_df.append(pd.DataFrame([df.iloc[x, 0]], columns='Overload Start'))

        # For when overloaded server falls becomes normal
        elif average_ping < t and IPv4_Address in servers_overloaded:

            # Add information to output dataframe at the correct instance/address
            for y in overload_df.index:

                # When the number of disconnect counts exceed N
                if overload_df.loc[y, 'IPv4 Address'] == IPv4_Address and pd.isnull(overload_df.loc[y, 'Overload End']) == True:
                    overload_df.loc[y, 'Overload End'] = df.iloc[x, 0]

                    # Update list of timed out servers
                    servers_overloaded.remove(IPv4_Address)
                    print(f'condition fufilled removed: {servers_overloaded}')

    # Add new IP address to df
    elif Ping != '-' and IPv4_Address not in servers_list:
        pinginstances_df[IPv4_Address] = 0
        pinginstances_df.loc[0, IPv4_Address] = 1
        pinginstances_df.loc[1, IPv4_Address] = Ping
        # Update server list
        servers_list.append(IPv4_Address)
        print(f'servers list: {servers_list}')

# Calculate Overload Duration
for y in overload_df.index:

    Overload_Start = overload_df.loc[y, 'Overload Start']
    Overload_End = overload_df.loc[y, 'Overload End']

    if pd.isnull(Overload_End):
        overload_df.loc[y, 'Overload Duration'] = 'Server_Overload'

    else:
        # Calculate Number of Years
        years = Overload_End//(10**10) - Overload_Start//(10**10)

        # Calculate Number of Months
        months = Overload_End//(10**8) % (10**2) - Overload_Start//(10**8) % (10**2)
        if months <= 0 and years >= 1:
            months = 12 + months
            years = years - 1

        # Calculate Number of Days
        days = Overload_End//(10**6) % (10**2) - Overload_Start//(10**6) % (10**2)
        if days <= 0 and months >= 1:
            days_30 = {4, 6, 9, 11}

            # Account for February
            if Overload_End//(10**6) % (10**2) == 2:

                # Account for Leap Year
                if Overload_End//(10**8) % (10**2) % 4 != 0:
                    days = 28 + days
                    months = months - 1
                else:
                    days = 29 + days
                    months = months - 1

            # Account for Months with 30 days
            elif Overload_End // (10**6) % (10**2) in days_30:
                days = 30 + days
                months = months - 1

            # Rest of the months with 31 days
            else:
                days = 31 + days
                months = months - 1

        # Calculate Number of Hours
        hours = Overload_End//(10**4) % (10**2) - Overload_Start//(10**4) % (10**2)
        if hours <= 0 and days >= 1:
            hours = 24 + hours
            days = days - 1

        # Calculate Number of Minutes
        minutes = Overload_End//(10**2) % (10**2) - Overload_Start//(10**2) % (10**2)
        if minutes <= 0 and hours >= 1:
            minutes = 60 + minutes
            hours = hours - 1

        # Calculate Number of Seconds
        seconds = Overload_End % (10**2) - Overload_Start % (10**2)
        if seconds <= 0 and minutes >= 1:
            seconds = 60 + minutes
            minutes = minutes - 1

        overload_df.loc[y, 'Overload Duration'] = seconds + minutes*(10**2) + hours*(10**4) + days*(10**6) + months*(10**8) + years*(10**10)
        overload_df['Overload Duration'] = overload_df['Overload Duration'].map('{:0>14}'.format)


print(pinginstances_df)
print(overload_df)

# Export dataframe to csv
overload_df.to_csv(output_filepath, index=None)