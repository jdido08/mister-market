import fred_data
import wikipedia_data

### Google Cloud Functions ###
def update_raw_data_sp500_prices(request):
    fred_data.update_sp500_prices()

def update_raw_data_treasury_yields(request):
    fred_data.update_treasury_yields()

def update_raw_data_tips_yields(request):
    fred_data.update_tips_yields()

def update_raw_data_sp500_constituents(request):
    wikipedia_data.update_sp500_constituents()




### Local Functions ####
def update_raw_data_sp500_prices():
    fred_data.update_sp500_prices()

def update_raw_data_treasury_yields():
    fred_data.update_treasury_yields()

def update_raw_data_tips_yields():
    fred_data.update_tips_yields()

def update_raw_data_sp500_constituents():
    wikipedia_data.update_sp500_constituents()


### TEST ###
update_raw_data_sp500_prices()
# update_raw_data_treasury_yields()
# update_raw_data_tips_yields()
# update_raw_data_sp500_constituents()

#def main()

    #wall_street = market("usa","dollar","sp500","1/1/1900") #we might also want to specify equity index here
    #wall_street.set_company_data()
    #wall_street.set_market_data()

    #shanghai = market("china","yuan","1/1/2000")


#update data scripts
