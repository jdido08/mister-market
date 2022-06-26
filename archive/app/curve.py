

# class curve

    #class vars
        # ytm curve -- type scipy interpolate --> need to look into this a bit more
        # zero curve -- type scipy interpolate
        # forward curve -- type scipu interpolate
        # average --> single complex weighted average; this has to be set externally once all market pieces are known
        # curve length
        # time horizon (maybe different from curve length)

    #init
        # set curve length
        # set time horizon (maybe different from curve length)
        # init parameters need to be date and databse (treasury, tips, etc.. )

    #other functions:

    #see link: https://www.quantandfinancial.com/2012/08/yield-curve-bootstrapping.html
    # def set_yield_curve(dict): #there can be alot of improvements with this function so best to get basic thing setup then circle back
        #the form of the data we want is a dictionary with key value pairs like:
        #ttm(years):yield(annual effective)
        #goal is to take any set of market_yields and be able to construct a yield curve for a time period
            #interpolate between ytm points
            #determine terminal yield curve (e.g 30 years)
        #set ytm curve -->  output should be  scipy.interpolate class


    # get_yield(ttm): #given ttm --> returns correspng ytm
        #check to make sure ttm is is range
        #return self.yield(ttm)

    # set_zero_curve(dict)
        #

    # set_1_year_forward_curve

    # interpolate curve -- take set of values and create curve given curve term structure length e.g. 30 years

    # bootstrap -- derive zero rates using ytm curve
        # function should handle dictionary with maturity and value

    # get_zero(time) # returns spot to year zero from zero cruve

    # get_1_year_fowards(year1, year2)

    #i think these are actuall forwards
    # get_zero(year1, year2) #returns year1_z_year2 zero using forward rate cruve
    # get_forward(year1,year2) -- returns year1_f_year2 forward

    #
