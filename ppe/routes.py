from flask import render_template
from ppe import app
from ppe.models import Market

@app.route("/")
def homepage():
    market = Market.query.order_by(Market.date.desc()).first()
    market_data = [
        {"Market":"Price", "Lvl": "$" + str(round(market.sp500_close,2))},
        {"Market":"Cash Flows", "Lvl": "$" + str(round(market.dps_ttm,2))},
        {"Market":"Growth", "Lvl":str(round((100*market.growth_rate),2))+"%"},
        {"Market":"Last Updated", "Lvl":market.date.strftime("%Y-%m-%d")}
    ]
    return render_template("index.html", rows=market_data)

@app.route("/docs")
def docs():
    return render_template("index.html", title="docs page")

@app.route("/about")
def about():
    return render_template("index.html", title="about page")
