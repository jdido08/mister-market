from data_functions import *
from calc_functions import *

#runs daily at 22:00 (10 PM) EST to capture the constituents for that day
def update_sp500_constituents_cloud_function(request):
    update_sp500_constituents()
    return "finished update_sp500_constituents"

#runs daily at 06:00 (6 AM) EST
def update_sp500_prices_cloud_function(request):
    update_sp500_prices()
    return "finished update_sp500_prices"

#runs daily at 06:00 (6 AM) EST
def update_treasury_yields_cloud_function(request):
    update_treasury_yields()
    return "finished update_treasury_yields"

#runs daily at 06:00 (6 AM) EST
def update_tips_yields_cloud_function(request):
    update_tips_yields()
    return "finished update_tips_yields"

# runs weekly on Saturdays at 01:00 (1 AM) EST
def reset_company_status_cloud_function(request):
    reset_company_status()
    return "finished reset_company_status"

# runs weekly on Saturdays at every minute from 02:00 (2 AM) to 17:00 (5 PM) EST
def update_company_data_cloud_function(request):
    update_company_data()
    return "finished update_company_data"

# runs ???
def update_market_measures_cloud_function(request):
    update_market_measures()
    return "finished update_market_measures"
