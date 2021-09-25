#import config.pu

# class nominal_rfr (curve)

    #inherits attributes and functions of curve.py class
        #vars: ytm curve, zero curve, forward curve
        #main functions: set ytm, set zero, set 1_year_forward_curve, bootstrap, interpolate

    #class vars
        #date
        #country
        #market_yields = {} dictionary --> ttm(years)
        #yields --> type curve
        #zeros --> type curve
        #1y_forwards --> type curve

    #def __init__(self, date, country):
        #self.date = date
        #self.country = country

    #def set_market_yields(self) # --> not sure what to call this but
        #get needed data to construct the cruve based on the country (and/or date maybe);
            #going to assume here also that other countrie work like the US
            #if thats not the case then i'll make need changes in the future

        #uses app config file to get right database to pull from
        #db = config[self.country]['nominal_frf']
        #use sqlalchemy --> heres how to setup a connection; probably want to do this elswhere in the progrom in main: https://cloud.google.com/sql/docs/mysql/samples/cloud-sql-mysql-sqlalchemy-create-tcp
        #query database for nominal_rfr yields on that day
        #sql = 'SELECT * FROM ' + db + ' WHERE DATE = ' + self.date

        #get market yields into correct form
        #depending on native yield type might, may need to convert annual effective form
        #actuall i think we want to be in continous compounding form --> see hull

        #set market yields
        #the form of the data we want is a dictionary with key value pairs like:
        #ttm(years):yield(continous compounding) --

    #def set_yields(self):
        #self.yields = set_yield_curve(self.market_yields)

    #def set_zeros(self):
        #self.zeros = set_zero_curve(self.yields)

    #don't need to worry about figuring out fowards yet







        #set the nominal rfr rate for a country e.g. usa
            # get treasury rates --> eventually will do this by getting bonds prices but initially going to use constant maturity rates
