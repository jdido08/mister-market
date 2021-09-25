# market_day:

#market_day should provide all the price and fundamental discounted into markets for a particular day
# e.g. market_day.price['treasury']['10_year'] --> gives yield on 10 year for that day
# e.g. market_day.growth
# e.g. market_day.inflation

    #class variables
        #date
        #country
        #inflation --> type curve
        #risk_premium --> type curve
        #real_yields --> type curve
        #index --> primary equity index
        #sectors --> dict of indices
        #price --> price of primary index
        #eps --> eps of primary index
        #payout ratio --> payout ratio of primary index

    #def __init__(self, date, country, currency, index):
        #self.date = date
        #self.country = country
        #self.currency = currency
        #self.index = index


    #def calc_fundamentals()
        #self.nominal_rfr = nominal_rfr(self.date,self.country)
        #self.real_rfr = real_rfr(self.date, self.country) --> this would largely be the same as nominal_rfr
        #self.inflation = inflation(self.date, self.country, self.nominal_rfr, self.real_frf)

        #self.index = equity_index(self.date,self.country, self.index) #how do i specify primary equity index here
        #self.price = self.index.price
        #self.eps = self.index.eps
        #self.payout_ratio = self.index.payout_ratio

        #self.growth = growth(self.date, self.country, etc...) #need to calculate growth last

        #next step is to calc market ytm for inflation and ry


    #circle back on this
    #def get_price_sources() --> # based on country (and maybe date) set config file to
        #maybe though this should go in the individual fundamental files; i think so because calcing each can be unique
