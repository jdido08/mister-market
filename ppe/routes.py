from flask import render_template
from ppe import app
from ppe.models import Market

@app.route("/")
def homepage():
    market = Market.query.order_by(Market.date).first()
    market_data = [
        {"Market":"Price", "Lvl":market.sp500_close},
        {"Market":"Cash Flows", "Lvl":market.dps_ttm},
        {"Market":"Growth", "Lvl":market.growth_rate}, 
        {"Market":"Last Updated", "Lvl":market.date}
    ]
    return render_template("index.html", rows=market_data)

@app.route("/docs")
def docs():
    return render_template("index.html", title="docs page")

@app.route("/about")
def about():
    return render_template("index.html", title="about page")
