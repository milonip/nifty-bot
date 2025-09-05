"""
EV-based strike selection.
Combine direction prob + magnitude quantiles + current IV/quotes to estimate expected P&L.
Constraints: spread, freshness, max lots, max spend.
TODO: Implement after models + quotes.
"""
def suggest_strikes(today_date) -> dict:
    return {"candidates": [], "reason": "Models/quotes not wired yet."}
