#wall_street = market
# country
# currency
# start_date
# dict of market_days


# class market:

    #class variables
        #name
        #country
        #currecny
        #index
        #start date
        #dict of market_days
        #dict of companies

    # init method or constructor
    #def __init__(self, country,currency,index,start_date):
        #self.country = country
        #self.currency = currency
        #self.index = index
        #self.start_date = start_date
        #self.date = {} #create empty dictionary; these will include market days
        #self.corp = {} #create empty dictionary; value pair will be 'ticker':'company class'#maybe not ready for this yet


    #def set_market_data(date1, date2) # returns all market_data between dates given
        #check if dates are in range

        #calcs date range between dates given
        #for date in date range       ---> eventually compute all of these in parrallel
            #data = market_day(date, self.country, self.currency, self.index) --> pass along date but also market attributes like country, currency, index
            #self.dates[date] = data


    #future ideas
    #def set_company_data(index):
        #given index --> pull company idenifiers and put into list tickers

        #for ticker in ticker: # --> eventually try to parrallelize
            #co = company(ticker)
            #corp[ticker] = co

    #def calc_market_changes(date1,date2) #function that calcs market differences






# functions  ---> just messing around here
#wall_street.date['3/10/2021'].growth --> whats growth curve for wall street
#wall_street.date['3/10/2021'].growth.get_zero(2,5) --> returns growth rate 2z5
#wall_street.date['3/10/2021'].growth.zeros['3'] --> returns 3 year growth rate zero 0z3
#wall_street.date['3/10/2021'].growth.forwards['3'] --> --> returns 2f3 growth rate ?
#wall_street.date['3/10/2021'].get_price('4/10/2021') --> returns the market priced in for some day in future at that day in time
#wall_street.date['3/10/2021'].get_price() --> returns the market price of that day
#wall_street.date['3/10/2021'].get_return() --> return daily return

#wall_street.start_date --> returns earliest date
#wall_street.country --> returns usa
#wall_street.currency --> returns dollar
#wall_street.returns(date1, date2) --> return market returns between two dates
#wall_street.returns() --> returns long term cumulative returns


#wall_street.change(date1, date2) --> return attribution
