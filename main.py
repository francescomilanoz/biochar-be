import pandas as pd
import numpy as np
from flask import Flask, request
from flask_cors import CORS, cross_origin
import datetime
import time
from collections import OrderedDict
from collections import defaultdict
import calendar
from dateutil.relativedelta import relativedelta

# how to run?
# first, run export FLASK_APP=python-be
# then, run flask run

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

# how many points per second to consider. 1 will be accurate enough for most cases. Please note that increasing this will also increase (exponentially) the computation time
ACCURACY = 1

OFF_LIMIT = 60
CORRECT_RANGE_START = 350
CORRECT_RANGE_END = 1000

def difference_in_days(str_date1, str_date2):
    date1 = datetime.datetime.strptime(str_date1, "%d/%m/%Y")
    date2 = datetime.datetime.strptime(str_date2, "%d/%m/%Y")
    return (date2 - date1).days

def days_in_month(date):
    # Calculate the number of days in the month of the given date
    _, last_day = calendar.monthrange(date.year, date.month)
    return last_day - date.day + 1

def generate_month_map(start_date, end_date):
    current_date = start_date
    month_map = defaultdict(int)

    while current_date <= end_date:
        month_key = f"{current_date.year}/{current_date.month:02d}"

        # Calculate the number of days remaining in the current month
        if current_date.month == end_date.month and current_date.year == end_date.year:
            days_remaining = end_date.day - current_date.day + 1
        else:
            days_remaining = days_in_month(current_date)

        # If the month is not in the map, add it with the remaining days
        if month_key not in month_map:
            month_map[month_key] = days_remaining
        else:
            month_map[month_key] += days_remaining

        # Move to the next month
        current_date = current_date.replace(day=1) + relativedelta(months=1)

    return month_map

@app.route('/index')
@cross_origin()
def request_active_days():
    start_date = request.args.get('dal')
    end_date = request.args.get('al')
    return anaylse_file(start_date, end_date)

def interpolate_points(point1, point2, num_points):
    real_points = num_points
    if(num_points < 0):
        return []
    if(num_points < 1):
        real_points = 1
    else:
        real_points = int(num_points)

    x_values = np.linspace(point1[0], point2[0], real_points)
    y_values = np.linspace(point1[1], point2[1], real_points)
    interpolated_points = list(zip(x_values, y_values))
    return interpolated_points

def anaylse_file(start_date, end_date):
    print(f"Downloading file with dates {start_date} - {end_date}")
    # input example: dal=05/10/2022&al=06/10/2023
    try:
        # read data from LOTTOB-SYSTEMA for dates before 04/09/2023, then use BIOKW-SYSTEMA
        dataframe_treshold = '04/09/2023'
        formatted_dataframe_treshold = time.strptime('04/09/2023', "%d/%m/%Y")
        formatted_start_date = time.strptime(start_date, "%d/%m/%Y")
        formatted_end_date = time.strptime(end_date, "%d/%m/%Y")

        # print('mese ', formatted_start_date.tm_mon)

        should_use_LOTTOB = formatted_start_date < formatted_dataframe_treshold
        should_use_BIOKW = formatted_end_date > formatted_dataframe_treshold

        if should_use_LOTTOB and should_use_BIOKW:
            df1 = pd.read_csv(f'http://46.232.136.242:5080/SalviGraficiCelle/grafici_csv.jsp?impianto=LOTTOB&macchina=SYSTEMA&sensore=t_bassa&intervallo=9999&dal={start_date}&al={dataframe_treshold}', sep=';')
            df2 = pd.read_csv(f'http://46.232.136.242:5080/SalviGraficiCelle/grafici_csv.jsp?impianto=BIOKW&macchina=SYSTEMA&sensore=t_bassa&intervallo=9999&dal={dataframe_treshold}&al={end_date}', sep=';')
            df = pd.concat([df1, df2], axis=0)
        elif should_use_LOTTOB:
            df = pd.read_csv(f'http://46.232.136.242:5080/SalviGraficiCelle/grafici_csv.jsp?impianto=LOTTOB&macchina=SYSTEMA&sensore=t_bassa&intervallo=9999&dal={start_date}&al={end_date}', sep=';')
        else:
            df = pd.read_csv(f'http://46.232.136.242:5080/SalviGraficiCelle/grafici_csv.jsp?impianto=BIOKW&macchina=SYSTEMA&sensore=t_bassa&intervallo=9999&dal={start_date}&al={end_date}', sep=';')

        print(df)

    except pd.errors.EmptyDataError:
        # File is empty, return 0
        return dict(productive_days=0, unproductive_days=0, off_days=0, on_days=0, total_days=difference_in_days(start_date, end_date), start_date=start_date, end_date=end_date, monthly_productive_days=OrderedDict(), monthly_off_days=OrderedDict(), monthly_unproductive_days=OrderedDict(), monthly_on_days=OrderedDict(), error=1)
    print("File downloaded!")

    df['DataOra'] = pd.to_datetime(df['DataOra'], format='%d/%m/%Y %H:%M:%S')
    productive_minutes = 0
    off_minutes = 0

    perc_printed = 0

    # monthly data
    monhtly_productive_minutes = OrderedDict()
    monthly_off_minutes = OrderedDict()

    print("Analysing file...")
    for i in range(len(df)):
        if(int(i/len(df) * 100) > perc_printed):
            perc_printed = int(i/len(df) * 100)
            print(f"Analysed {perc_printed}%.")

        if(i == 0):
            continue

        current_value = df.iloc[i].Valore
        previous_value = df.iloc[i-1].Valore
        current_time = df.iloc[i].DataOra.timestamp()
        previous_time = df.iloc[i-1].DataOra.timestamp()

        # monthly data
        current_month = df.iloc[i].DataOra.month
        current_year = df.iloc[i].DataOra.year
        current_monthly_key = f"{current_year}/{current_month:02d}"

        # if both points are below OFF_LIMIT, it means that in the entire period the machine was off. We can add the total time to off_minutes, without the need of interpolating
        if(current_value < OFF_LIMIT and previous_value < OFF_LIMIT):
            off_minutes = off_minutes + int(current_time - previous_time)

            # monthly data
            monthly_off_minutes[current_monthly_key] = monthly_off_minutes.get(current_monthly_key, 0) + int(current_time - previous_time)
            continue

        # if both points are above 1000, or both points are below 350, it means that in the entire period the machine was not in the correct range. We can skip without interpolating
        if((current_value > CORRECT_RANGE_END and previous_value > CORRECT_RANGE_END) or (current_value < CORRECT_RANGE_START and previous_value < CORRECT_RANGE_START)):
            continue

        # if both points are in the correct range, it means that the machine operated in the correct range for the entire time. We can add the total time to productive_minutes, without the need of interpolating
        if((current_value >= CORRECT_RANGE_START and previous_value >= CORRECT_RANGE_START) and (current_value <= CORRECT_RANGE_END and previous_value <= CORRECT_RANGE_END)):
            productive_minutes = productive_minutes + int(current_time - previous_time)

            # monthly data
            monhtly_productive_minutes[current_monthly_key] = monhtly_productive_minutes.get(current_monthly_key, 0) + int(current_time - previous_time)

            continue

        # in the (rare) case one point is in the range, but the other one no, we need to interpolate the points in order to increase the accuracy
        interpolated_points = interpolate_points((int(current_time), current_value), (int(previous_time), previous_value), int(current_time - previous_time) * ACCURACY)
        for j in interpolated_points:
            if(j[1] >= CORRECT_RANGE_START and j[1] <= CORRECT_RANGE_END):
                productive_minutes = productive_minutes + (1/ACCURACY)

                # monthly data
                monhtly_productive_minutes[current_monthly_key] = monhtly_productive_minutes.get(current_monthly_key, 0) + (1/ACCURACY)

            elif(j[1] < OFF_LIMIT):
                off_minutes = off_minutes + (1/ACCURACY)

                # monthly data
                monthly_off_minutes[current_monthly_key] = monthly_off_minutes.get(current_monthly_key, 0) + (1/ACCURACY)

    productive_days = round(productive_minutes/60/60/24, 2)
    total_days = difference_in_days(start_date, end_date) + 1
    print(f"Total days: {total_days}")
    off_days = round(off_minutes/60/60/24, 2)
    on_days = round(total_days - off_days, 2)
    unproductive_days = round(total_days - off_days - productive_days, 2)

    # monthly data
    monthly_productive_days = OrderedDict()
    for key, value in monhtly_productive_minutes.items():
        monthly_productive_days[key] = round(value/60/60/24, 2)

    monthly_off_days = OrderedDict()
    for key, value in monthly_off_minutes.items():
        monthly_off_days[key] = round(value/60/60/24, 2)

    monthly_total_days = generate_month_map(datetime.datetime.strptime(start_date, "%d/%m/%Y"), datetime.datetime.strptime(end_date, "%d/%m/%Y"))

    monthly_unproductive_days = OrderedDict()
    for key, value in monthly_total_days.items():
        monthly_unproductive_days[key] = round(monthly_total_days.get(key, 0) - monthly_off_days.get(key, 0) - monthly_productive_days.get(key, 0), 2)

    monthly_on_days = OrderedDict()
    for key, value in monthly_total_days.items():
        monthly_on_days[key] = round(monthly_total_days.get(key, 0) - monthly_off_days.get(key, 0), 2)


    # It can happen that some of those values overflows of at max 0.99 the total days. Limiting them here
    if productive_days > total_days:
        productive_days = total_days

    if off_days > total_days:
        off_days = total_days

    if on_days > total_days:
        on_days = total_days

    if unproductive_days > total_days:
        unproductive_days = total_days

    if on_days < 0:
        on_days = 0

    if unproductive_days < 0:
        unproductive_days = 0

    string_result = f"The machine was active for {productive_days} days in the period {start_date} - {end_date}"
    print(string_result)
    return dict(productive_days=productive_days, unproductive_days=unproductive_days, off_days=off_days, on_days=on_days, total_days=total_days, start_date=start_date, end_date=end_date, monthly_productive_days=monthly_productive_days, monthly_off_days=monthly_off_days, monthly_unproductive_days=monthly_unproductive_days, monthly_on_days=monthly_on_days, error=0)

USERNAME = 'admin'
PASSWORD = 'test'

@app.route('/authenticate')
@cross_origin()
def authenticate():
    username = request.args.get('username')
    password = request.args.get('password')
    print('Authenticating...')
    if (username == USERNAME and password == PASSWORD):
        print('Authenticated succesfully!')
        return 'true'
    print('Wrong username or password.')
    return 'false'