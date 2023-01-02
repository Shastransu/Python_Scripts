import pandas as pd
import datetime,warnings
import holidays
import argparse,asyncio,json
pd.options.mode.chained_assignment = None
warnings.filterwarnings("ignore")

#Start year and end year taking as argument
async def evaluate(startyear,endyear):
    result = {'start_year':'','end_year':''}
    result['start_year'] = startyear
    result['end_year'] = endyear
    return result

#Created a datframe with start to end plus one month of extra data.
#Data frame consist of The Day- date Number i.e 1,2,..31, day Name - Sun, Mon
#The week is starting from 1 for Jan1, ISO week is general week
#Day of week - day 1 for Mon, 2 for Tues
#The Month - Integer value of month, Month name - Jan, Feb
#Quarter - Q1,Q2, ..
#last of year shows last day of the current year, first of month shows first date of current month
#The Day of year - Out of 365/366 the date number
#Is weekend - Check date is on saturday or Sunday

def create_date_table(result):
    start_year = int(result['start_year'])
    end_year = int(result['end_year'])

    #--------------------------------------------------------------------------
    # Taking input year to starting month i.e Jan to last Month of end year i.e dec

    start_year_str = '{}-1-1'.format(start_year)
    end_year_str = '{}-12-31'.format(end_year)

    #----------------------------------------------------------------------------
    #Converting string to datetime(Timestamp) format

    start_year_dt = datetime.datetime.strptime(start_year_str,'%Y-%m-%d')
    end_year_dt = datetime.datetime.strptime(end_year_str,'%Y-%m-%d')

    df = pd.DataFrame({"Date":pd.date_range(start_year_dt,end_year_dt+datetime.timedelta(days=31))})
    df["TheDay"] = df.Date.dt.day
    df["TheDayName"] = df.Date.dt.strftime("%A")
    df["TheWeek"] = df.Date.apply(lambda x:int(x.strftime("%U")) if (x.strftime("%A") == "Sunday" and x.month_name() == "January") else int(x.strftime("%U"))+1)
    df["TheISOWeek"] = df.Date.dt.week
    df["TheDayofWeek"] = df.Date.dt.strftime("%w")
    df["TheMonth"] = df.Date.dt.month
    df["TheMonthName"] = df.Date.dt.month_name()
    df["TheQuarter"] = df.Date.dt.quarter
    df["Year_half"] = (df.TheQuarter+1)//2
    df["TheYear"] = df.Date.dt.year
    df["TheFirstOfMonth"] = df.Date.dt.strftime("%Y-%m-01")
    df["TheLastOfYear"] = df.Date.dt.strftime("%Y-12-31")
    df["TheDayOfYear"] = df.Date.dt.dayofyear
    df['IsWeekend'] = df.Date.apply(lambda x: "Y" if ((x.strftime("%A") == "Saturday") or (x.strftime("%A")  == "Sunday")) else 'N')
    df['IsHoliday']=''
    return df

#Created Holiday list dataframe, which includes holiday name and date.


def holiday_list(result):
    start_year = int(result['start_year'])
    end_year = int(result['end_year'])

    dict_ ={"Date":[],"HolidayName":[]}
    for x in range(start_year, end_year+2):
        us_holidays = holidays.US(years=x)

        for key, val in sorted(us_holidays.items()):
            dict_['Date'].append(str(key))
            dict_['HolidayName'].append(val)
    df_holiday_list= pd.DataFrame.from_dict(dict_,orient='index').transpose()
    return df_holiday_list

#-----------------------------------------------------------------
#Changing data tyope of date to str so that able to merge the two dataframe(df,df_holiday_list)
#make IsHoliday column as Y in orginal datframe if the date present in oroginal dataframe present if holiday dataframe

def merge_table(table_create):
    dates = pd.to_datetime(table_create['Date'])
    dates_str = dates.dt.strftime("%Y-%m-%d")
    table_create['Date'] = dates_str

    for x in range(0, len(dates_str)):
        if dates_str[x] in df_holiday_list['Date'].values:
            table_create['IsHoliday'].loc[x] = 'Y'
        else:
            table_create['IsHoliday'].loc[x] = 'N'

    table_create = pd.merge(table_create,df_holiday_list,on='Date',how='left')

    #------------------------------------------------
    #Created column for Business day. If a date lie on weekend or Holiday , it can't be business day

    table_create['IsBusinessDay'] = ''

    for x in range(0,len(table_create)):
        if (table_create['IsWeekend'].loc[x]=='Y') or (table_create['IsHoliday'].loc[x]== 'Y'):
            table_create['IsBusinessDay'].loc[x]= 'N'
        else:
            table_create['IsBusinessDay'].loc[x] = 'Y'

    #-------------------------------------------------
    #After merge again change date type column to Timestamp

    dates = pd.to_datetime(table_create['Date'])
    table_create['Date'] = dates


    return table_create

#--------------------------------------------------------------
#creating a dataframe in which only business day data will be there
#reset the index

def bus_day_df(merged_table_create):
    df_new = merged_table_create[merged_table_create['IsBusinessDay'] == 'Y']
    df_new.reset_index(drop=True,inplace=True)
    return df_new

#---------------------------------------------------------------------
#Creted a common function which handle t+business day
def next_bus_day(iter,addn_date):
    debug_flag = False
    #Taking out the date
    dt = iter.Date

    if debug_flag == True:
        print(dt,"Date")

    def iter_bus_day(date_,iter_over):
        debug_flag = False
        new_index = df_new.index
        if debug_flag == True:
            print(len(new_index),"length of new index")

        #Give the index for the date value which is greater than the date
        index_1 = df_new['Date'] > date_
        if debug_flag == True:
            print(len(index_1),"length of new Index_1")

        #Writing index value value to list,it will take the first value
        #Except block for last date

        try:
            indx1 = new_index[index_1].tolist()[0]
        except:
            indx1 = len(index_1)

        #For example df_new has 100 value and input has been given for extra 14 days of current date
        #Suppose current date come for 88 index , now +13 index of the data is 101, which is not present
        #so for these type data , it will return None
        #For other it will give current date plus 13th index value

        if indx1 >= len(df_new) - iter_over:
            return None
        return df_new['Date'].loc[indx1+iter_over]

    return iter_bus_day(dt,addn_date-1)

#Sample for inserting data to mongo db

async def insertmodels_excel_tomongodb():
    #Mongo connection
    #db = <making connection>.mongo.client
    df_models_mongo = merge_table_create.to_json(orient = 'records',date_format = 'iso')
    colname = 'Calendar_Table'

    try:
        col1 = db[colname]
        await col1.insert_many(json.loads(df_models_mongo))
        print(f"Pushed")
    except Exception as e:
        print(str(e))

if __name__ == '__main__':
    parser = argparse.ArgumentParser('Calendar Table generate')
    parser.add_argument('-startyear',help='Start of the year',required=True)
    parser.add_argument('-endyear', help='End of year', required=True)
    args = parser.parse_args()
    loop = asyncio.get_event_loop()
    result = asyncio.run(evaluate(args.startyear,args.endyear))

    debug_flag = False
    table_create = create_date_table(result)
    if debug_flag == True:
        table_create.to_excel('table_create.xlsx',index= False)

    df_holiday_list = holiday_list(result)
    if debug_flag == True:
        df_holiday_list.to_excel('df_holiday_list.xlsx',index=False)

    merged_table_create = merge_table(table_create)
    if debug_flag == True:
        merged_table_create.to_excel('merged_table_create.xlsx',index=False)

    df_new = bus_day_df(merged_table_create)
    if debug_flag == True:
        df_new.to_excel('df_new.xlsx',index=False)

#-------------------------------------------------------------------
#Created a lambda function to create t+no of days
    merged_table_create['t+14'] = merged_table_create.apply(lambda x:next_bus_day(x,14), axis=1)
    #merged_table_create['t+5'] = merged_table_create.apply(lambda x: next_bus_day(x, 5), axis=1)

    merged_table_create.to_excel('Final_Calendar.xlsx',index=False)