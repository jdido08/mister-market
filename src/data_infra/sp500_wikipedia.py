############################
#### sp500 constituents ####
############################

def get_sp_500_constituents_historical():
    #data at https://github.com/fja05680/sp500
    start_date = datetime.datetime(1996, 1, 2).strftime("%Y-%m-%d") #start date from https://github.com/fja05680/sp500
    end_date = datetime.datetime(2020, 12, 4).strftime("%Y-%m-%d") #this potentially needs to be updated if the underlying csv updates
    date_df = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name='date')

    df = pd.read_csv('S&P 500 Historical Components & Changes(12-04-2020).csv')
    df['date'] = pd.to_datetime(df['date'])

    df = pd.merge(date_df, df, on='date', how='left')
    df = df.sort_values(by='date')
    df = df.fillna(method='ffill')

    #write historicals to big query
    write_to_db(df, "raw_data.sp_500_constituents", "replace")



def get_sp_500_constituents_today():
    data = pd.read_html('https://en.wikipedia.org/wiki/List_of_S&P_500_companies')

    # Get current S&P table and set header column
    df = data[0].iloc[1:,[0]] #get certain columns in first table on wiki page
    columns = ['ticker']
    df.columns = columns

    tickers_list = list(set(df['ticker'])) #remove any accidnetal duplicates
    tickers_list = sorted(tickers_list)
    tickers_str = ','.join(tickers_list)

    #reformat
    date = datetime.datetime.combine(datetime.date.today(), datetime.time(), tzinfo=datetime.timezone.utc)


    df = pd.DataFrame(columns= {'date', 'tickers'}) #crate empty dataframe
    df = df.append({'date':date, 'tickers':tickers_str}, ignore_index = True)

    return df

def get_sp500_constituents():
    sql = "SELECT * FROM " + table_id + " Order by date  asc"
    sp_500_constituents_df = read_db(sql) #get existing sp_500_constituents

    #get dates from end of existing sp_500_constituents dataframe and today
    start_date = sp_500_constituents_df['date'].max() + datetime.timedelta(days=1) #find first date which there is no data for
    end_date = datetime.datetime.combine(datetime.date.today(), datetime.time(), tzinfo=start_date.tzinfo)
    date_df = pd.date_range(start=start_date, end=end_date).to_frame(index=False, name='date')

    #get sp_500_constituents for today
    sp_500_constituents_today_df = get_sp_500_constituents_today()

    #append sp_500_constituents dataframe with constituent from today
    sp_500_constituents_today_df = pd.merge(date_df, sp_500_constituents_today_df, on='date', how='left')
    sp_500_constituents_df = pd.concat([sp_500_constituents_df, sp_500_constituents_today_df], ignore_index=True)

    sp_500_constituents_df = sp_500_constituents_df.sort_values(by='date') #order by dates
    sp_500_constituents_df = sp_500_constituents_df.drop_duplicates(subset=['date']) #drop any duplicates dates'; this unccessary now
    sp_500_constituents_df = sp_500_constituents_df.fillna(method='ffill') #fill forward for any misisng dataframes

    #upload new sp_500_constituents list
    write_to_db(sp_500_constituents_df, "raw_data.sp_500_constituents", "replace")

    print("sp_500_constituents updated!")
