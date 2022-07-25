from data_functions import *

def update_sp500_constituents_cloud_function(request):
    update_sp500_constituents()

def update_sp500_prices_cloud_function(request):
    update_sp500_prices()

def update_treasury_yields_cloud_function(request):
    update_treasury_yields()

def update_tips_yields_cloud_function(request):
    update_tips_yields()

def reset_company_status_cloud_function(request):
    reset_company_status()

def update_company_data_cloud_function(request):
    update_company_data()
