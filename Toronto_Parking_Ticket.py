import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
import matplotlib


def get_df(fid):
    dataframe = pd.read_csv(fid, error_bad_lines=False, usecols=[1, 2, 3, 4, 5, 7, 10],
                            dtype={'time_of_infraction': str})  # Note: skip bad lines
    pat = r"\s\bAV\b"
    dataframe.location2 = dataframe.location2.str.replace(pat, ' AVE')
    # dataframe.location2 = dataframe.location2 + ' ,Toronto'

    # update date and time formats
    dataframe.date_of_infraction = pd.to_datetime(dataframe.date_of_infraction, format='%Y%m%d')
    dataframe['year_of_infraction'] = pd.to_datetime(dataframe.date_of_infraction, format='%Y%m%d').dt.year
    dataframe['month_of_infraction'] = pd.to_datetime(dataframe.date_of_infraction, format='%Y%m%d').dt.month
    dataframe['day_of_infraction'] = pd.to_datetime(dataframe.date_of_infraction, format='%Y%m%d').dt.day
    dataframe['quarter_of_infraction'] = pd.to_datetime(dataframe.date_of_infraction, format='%Y%m%d').dt.quarter
    dataframe['weekday_of_infraction'] = pd.to_datetime(dataframe.date_of_infraction,
                                                        format='%Y%m%d').dt.weekday_name

    dataframe.drop(['date_of_infraction'], axis=1, inplace=True)  # no longer useful

    dataframe.time_of_infraction = pd.to_datetime(dataframe['time_of_infraction'].astype(str), format='%H%M',
                                                  errors='coerce')
    dataframe['hour_of_infraction'] = pd.to_datetime(dataframe.time_of_infraction, format='%Y%m%d').dt.hour
    return dataframe


def processTicketDS(directory, dataSet, num_of_dataset):
    # TAG_NUMBER_MASKED   First three (3) characters masked with asterisks
    #  DATE_OF_INFRACTION  Date the infraction occurred in YYYYMMDD format
    #  INFRACTION_CODE Applicable Infraction code (numeric)
    #  INFRACTION_DESCRIPTION  Short description of the infraction
    #  SET_FINE_AMOUNT Amount of set fine applicable (in dollars)
    #  TIME_OF_INFRACTION  Time the infraction occurred  in HHMM format (24-hr clock)
    #  LOCATION1   Code to denote proximity (see table below)
    #  LOCATION2   Street address
    #  LOCATION3   Code to denote proximity (optional)
    #  LOCATION4   Street address (optional)
    #  PROVINCE    Province or state code of vehicle licence plate

    #  Proximity Code Table
    #  PROXIMITY CODE  DESCRIPTION
    #  AT  At
    #  NR  Near
    #  OPP Opposite
    #  R/O Rear of
    #  N/S North Side
    #  S/S South Side
    #  E/S East Side
    #  W/S West Side
    #  N/O North of
    #  S/O South of
    #  E/O East of
    #  W/O West of

    # ---------------------
    # Load the data frame

    print('Processing ', dataSet, '...')

    outFile = dataSet[:len(dataSet) - 4] + '_Summary.xls'
    if num_of_dataset == 1:
        df = get_df(open(directory + dataSet))
    else:
        dfs = []
        for i in range(1, num_of_dataset + 1):
            dfs.append(get_df(open(directory + dataSet[:len(dataSet) - 4] + '_' + str(i) + '.csv')))
        df = pd.concat(dfs)

    writer = pd.ExcelWriter(outFile)

    data = df.groupby(['infraction_code', 'infraction_description']).size().reset_index().rename(
        columns={0: 'count'}).drop_duplicates(subset='infraction_code', keep="last")
    data.to_excel(writer, 'code_description')

    # Distribution of fines
    data = df.groupby('infraction_code').agg({'set_fine_amount': ['count', 'sum', 'max']})
    data.to_excel(writer, 'Distribution')

    # Infractions by month
    data = df.groupby(['month_of_infraction']).agg(
        {'set_fine_amount': ['count', 'sum', 'max', 'min']})
    data.to_excel(writer, 'Per_month')

    # Infractions by quarter
    data = df.groupby(['quarter_of_infraction']).agg(
        {'set_fine_amount': ['count', 'sum', 'max', 'min']})
    data.to_excel(writer, 'Per_quarter')

    # Infractions by day
    data = df.groupby(['day_of_infraction']).agg(
        {'set_fine_amount': ['count', 'sum']})
    data.to_excel(writer, 'Per_day')

    # Infractions by weekday
    data = df.groupby(['weekday_of_infraction']).agg(
        {'set_fine_amount': ['count', 'sum']})
    data.to_excel(writer, 'Per_weekday')

    # Infractions by hour
    data = df.groupby(['hour_of_infraction']).agg(
        {'set_fine_amount': ['count', 'sum']})
    data.to_excel(writer, 'Per_hour')

    data = df.groupby(['weekday_of_infraction', 'hour_of_infraction', 'infraction_code']).agg(
        {'set_fine_amount': ['count', 'sum', 'max', 'min']})
    data.to_excel(writer, 'week_hour_code')

    # TODO Holiday Have Significant Impact on Number of Tickets Issued Everyday
    # TODO Weather Condition

    # Streets with the highest revenue
    data = df.groupby(['location2']).sum().set_fine_amount.sort_values(ascending=False)
    data = data[:100]
    data.to_excel(writer, 'top_streets')  # Number of Tickets Issued Decrease Slowly in Recent Years

    # The Number of Tickets with Hefty Fines Increased Dramatically in 2014

    writer.save()


directory = '../Datasets/Toronto_Parking_Tickets/CSVs/'

processTicketDS(directory, 'Parking_Tags_data_2008.csv', 1)
processTicketDS(directory, 'Parking_Tags_data_2009.csv', 1)
processTicketDS(directory, 'Parking_Tags_data_2010.csv', 1)
processTicketDS(directory, 'Parking_Tags_data_2011.csv', 1)
processTicketDS(directory, 'Parking_Tags_Data_2012.csv', 1)
processTicketDS(directory, 'Parking_Tags_Data_2013.csv', 1)
processTicketDS(directory, 'Parking_Tags_Data_2014.csv', 4)
processTicketDS(directory, 'Parking_Tags_Data_2015.csv', 3)
processTicketDS(directory, 'Parking_Tags_Data_2016.csv', 4)

directory = './'
files = os.listdir(directory)

tickets = []
for file in files:
    if file.endswith('.xls'):
        print(file)
        xl = pd.ExcelFile(file)
        dfs = {}
        for sh in range(0, len(xl.sheet_names)):
            if 0 < sh < 8:
                dfs[xl.sheet_names[sh]] = xl.parse(sh, skiprows=2).values
            else:
                dfs[xl.sheet_names[sh]] = xl.parse(sh, skiprows=0).values
        tickets.append(dfs)

years = [2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016]
months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
weekdays = ['Fri', 'Mon', 'Sat', 'Sun', 'Thur', 'Tues', 'Wed']
weekdaysF = ['Friday', 'Monday', 'Saturday', 'Sunday', 'Thursday', 'Tuesday', 'Wednesday']

total_revenue = []
total_tickets = []
total_large_fines = []
for year in range(0, len(tickets)):
    total_tickets.append(tickets[year]['Distribution'][:, 1].sum())
    total_revenue.append(tickets[year]['Distribution'][:, 2].sum())
    total_large_fines.append(
        tickets[year]['Distribution'][np.where(tickets[year]['Distribution'][:, 3] > 200), 1].sum())

print('total revenue (CAD): \n \t', total_revenue)
print('Y2Y revenue increase (%): \n \t', np.round(np.append([0], np.diff(total_revenue)) / total_revenue * 100, 2))

print('total ticket: \n \t', total_tickets)
print('Y2Y ticket increase (%): \n \t', np.round(np.append([0], np.diff(total_tickets)) / total_tickets * 100, 2))

print('total large ticket: \n \t', total_large_fines)
print('Y2Y large fine increase (%): \n \t',
      np.round(np.append([0], np.diff(total_large_fines)) / total_large_fines * 100, 2))

plt.figure(figsize=[16, 8])
plt.ylabel('# of Tickets')
plt.xlabel('Infraction type')
for year in range(0, len(tickets)):
    plt.scatter(tickets[year]['Distribution'][:, 0], tickets[year]['Distribution'][:, 1])

shown = []
for year in range(0, len(tickets)):
    sorted_ticket = np.sort(tickets[year]['Distribution'][:, 1])[::-1]
    for j in range(0, 11):
        L = np.where(tickets[year]['Distribution'][:, 1] == sorted_ticket[j])[0][0]
        if tickets[year]['Distribution'][L, 0] in shown:
            continue
        else:
            shown.append(tickets[year]['Distribution'][L, 0])
            idx = np.where(tickets[year]['code_description'][:, 0] == tickets[year]['Distribution'][L, 0])
            plt.text(tickets[year]['Distribution'][L, 0], tickets[year]['Distribution'][L, 1],
                     tickets[year]['code_description'][idx, 1][0][0], fontsize=8)

plt.legend(years, ncol=3, loc=0)

plt.figure(figsize=[16, 8])
plt.subplot(2, 1, 1)
plt.ylabel('# of Tickets')
plt.xticks(range(1, 13), months)
for year in range(0, len(tickets)):
    plt.plot(range(1, 13), tickets[year]['Per_month'][:, 1])
plt.legend(years, ncol=3, loc=8)

plt.subplot(2, 1, 2)
plt.ylabel('Total Revenue')
plt.xticks(range(1, 13), months)
for year in range(0, len(tickets)):
    plt.plot(range(1, 13), tickets[year]['Per_month'][:, 2])
plt.legend(years, ncol=3, loc=8)
plt.suptitle('Revenue and number of tickets per month for years between 2008 and 2016')

plt.figure(figsize=[16, 8])
plt.subplot(2, 2, 1)
plt.ylabel('# of Tickets')
plt.xticks(range(1, 5), ['Q1', 'Q2', 'Q3', 'Q4'])
for year in range(0, len(tickets)):
    plt.plot(range(1, 5), tickets[year]['Per_quarter'][:, 1])
plt.legend(years, ncol=3, loc=1)

plt.subplot(2, 2, 2)
plt.ylabel('Total Revenue')
plt.xticks(range(1, 5), ['Q1', 'Q2', 'Q3', 'Q4'])
for year in range(0, len(tickets)):
    plt.plot(range(1, 5), tickets[year]['Per_quarter'][:, 2])
plt.legend(years, ncol=3, loc=4)

plt.subplot(2, 2, 3)
plt.ylabel('# of Tickets')
plt.xticks(range(1, 8), weekdays)
for year in range(0, len(tickets)):
    plt.plot(range(1, 8), tickets[year]['Per_weekday'][:, 1])
plt.legend(years, ncol=3, loc=4)

plt.subplot(2, 2, 4)
plt.ylabel('Total Revenue')
plt.xticks(range(1, 8), weekdays)
for year in range(0, len(tickets)):
    plt.plot(range(1, 8), tickets[year]['Per_weekday'][:, 2])
plt.legend(years, ncol=3, loc=4)

plt.suptitle('Revenue and number of tickets per quarter and weekday for years between 2008 and 2016')

plt.figure(figsize=[16, 8])
plt.subplot(2, 2, 1)
plt.ylabel('# of Tickets')
for year in range(0, len(tickets)):
    plt.plot(range(1, 32), tickets[year]['Per_day'][:, 1])
plt.legend(years, ncol=3, loc=8)

plt.subplot(2, 2, 2)
plt.ylabel('Total Revenue')
for year in range(0, len(tickets)):
    plt.plot(range(1, 32), tickets[year]['Per_day'][:, 2])
plt.legend(years, ncol=3, loc=8)

plt.subplot(2, 2, 3)
plt.ylabel('# of Tickets')
for year in range(0, len(tickets)):
    plt.plot(range(1, 25), tickets[year]['Per_hour'][:, 1])
plt.legend(years, ncol=3, loc=2)

plt.subplot(2, 2, 4)
plt.ylabel('Total Revenue')
for year in range(0, len(tickets)):
    plt.plot(range(1, 25), tickets[year]['Per_hour'][:, 2])
plt.legend(years, ncol=3, loc=2)

day_hr_infraction = pd.DataFrame()
for year in range(0, len(tickets)):
    for day in range(0, len(weekdaysF)):
        day_idx0 = np.where(tickets[year]['week_hour_code'][0:, :] == weekdaysF[day])[0][0]
        if day == len(weekdaysF) - 1:
            day_idx1 = len(tickets[year]['week_hour_code'][0:, :])
        else:
            day_idx1 = np.where(tickets[year]['week_hour_code'][0:, :] == weekdaysF[day + 1])[0][0]
        for hr in range(0, 24):
            hr_idx0 = np.where(tickets[year]['week_hour_code'][day_idx0:day_idx1, 1] == hr)[0][0] + day_idx0
            if hr == 23:
                hr_idx1 = day_idx1
            else:
                hr_idx1 = np.where(tickets[year]['week_hour_code'][day_idx0:day_idx1, 1] == (hr + 1))[0][0] + day_idx0
            max_count_idx = np.argmax(tickets[year]['week_hour_code'][hr_idx0:hr_idx1, 3]) + hr_idx0  # count
            max_count_code = tickets[year]['week_hour_code'][max_count_idx, 2]
            idx = np.where(tickets[year]['code_description'][:, 0] == max_count_code)
            max_count_desc = tickets[year]['code_description'][idx, 1]

            # print(max_count_idx, max_count_code, max_count_desc)
            day_hr_infraction = day_hr_infraction.append(
                {'year': years[year], 'day': weekdaysF[day], 'hour': hr, 'code': max_count_code,
                 'desc': max_count_desc[0][0]}, ignore_index=True)

plt.figure(figsize=[16, 8])
colors = matplotlib.cm.rainbow(np.linspace(0, 1, len(years)))

lebels = day_hr_infraction.desc.unique()
plt.subplot(2, 4, 1)
plt.ylim([0, len(lebels)])
plt.xlim([0, 24])
plt.axis('off')

for i in range(0, len(lebels)):
    plt.text(0, i, str(i) + ':' + lebels[i], fontsize=8)

for day in range(0, len(weekdaysF)):
    for year in range(0, len(tickets)):
        plt.subplot(2, 4, day + 2)
        plt.xlim([0, 24])
        hours = day_hr_infraction[
            (day_hr_infraction.day == weekdaysF[day]) & (day_hr_infraction.year == years[year])].hour.values
        descp = day_hr_infraction[
            (day_hr_infraction.day == weekdaysF[day]) & (day_hr_infraction.year == years[year])].desc.values
        loc2 = np.array([-1] * 24)
        for i in range(0, len(lebels)):
            loc2[np.where(lebels[i] == descp[range(0, 24)])[0]] = i
        plt.plot(range(0, 24), loc2, c=colors[year])
        plt.title(weekdaysF[day])
        plt.ylabel('Most common infractions')
plt.suptitle('The most common infraction per hour for years between 2008 and 2016')
plt.subplot(2, 4, 1)
for i in range(0, len(tickets)):
    plt.text(20, 2 + i, str(2008 + i), color=colors[i], fontsize=8)

top_streets = pd.DataFrame()
for year in range(0, len(tickets)):
    for street in range(0, 10):
        top_streets = top_streets.append({'year': year + 2008, 'location': tickets[year]['top_streets'][street, 0],
                                          'amount': tickets[year]['top_streets'][street, 1]}, ignore_index=True)

dfs = []
for year in range(2008, 2017):
    dfs.append(top_streets[top_streets['year'] == year].drop('year', 1).set_index('location').rename(
        columns={'amount': str(year)}))

dfs_final = pd.DataFrame()
for year in range(0, len(dfs)):
    dfs_final = dfs_final.join(dfs[year], how='outer')

dfs_final.plot.barh(stacked=True, figsize=[16, 8])
plt.xlabel('Total fine')
plt.suptitle('Streets with the largest total fine for years between 2008 and 2016')

plt.show()
