from test.py import access_secret_version

def run_test(request):
    access_secret_version("mister-market-project", "alphavantage_special_key", "1")
