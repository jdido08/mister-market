from ppe import db

class Market(db.Model):
    __tablename__ = 'market_measures'
    date = db.Column(db.DateTime, primary_key=True)
    marketcap = db.Column(db.Float)
    dividends_ttm = db.Column(db.Float)
    non_gaap_earnings_ttm = db.Column(db.Float)
    sp500_close = db.Column(db.Float)
    dps_ttm = db.Column(db.Float)
    non_gaap_eps_ttm = db.Column(db.Float)
    payout_ratio = db.Column(db.Float)
    risk_free_rates = db.Column(db.String)
    risk_premium_rates = db.Column(db.String)
    growth_rate = db.Column(db.Float)

    # def __repr__(self):
    #     return f"Market('{self.date}', '{self.sp500_close}', '{self.growth_rate}')"
