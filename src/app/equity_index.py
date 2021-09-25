#once i stand up a company class --> will probably want to revist this setup


# class index -- class for equity indexes

    #class vars
        # price #--> given by market etf / index
        # constituents --> list of companies
        # market_cap #calcualted using company constituents
        # earnings_ttm #calcualted as sum of all company constituents earnings
        # dividends_ttm #calcualted as sum of all company constituents dividends
        # buybacks_ttm #calcualted as sum of all company constituents buybacks
        # divisor #calcualted as market_cap / price
        # eps_ttm #earnings per share = earnings_ttm / divisor
        # dps_ttm #dividends per share = dividends_ttm / divisor
        # bps_ttm #buybacks per share = buybacks_ttm / divisor
        # pps_ttm #payout per share = buyback + dividends

    #def __init__(self, date, country, index_name):
        #self.date = date
        #self.country = country
        #self.index = index_name
        #self.constituents = set_constituents()
        #self.price = set_price()

    #set_constituents(self):
        #get list of company identifers (e.g. tickers) for that day

    #set_price(self): # set data from database

    #set_market_cap(self) #
        #index_market_cap = 0
        #for ticker in self.constituents:
            #ticker_price = #get from db
            #shares_outstanding = #get from db
            #ticker_market_cap = price * shares_outstanding
            #index_market_cap += ticker_market_cap

    #etc.. something like the above for market_cap, earnings_ttm, etc..

    #calc_xps measures
