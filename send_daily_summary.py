
import os
from datetime import datetime, timedelta
import duckdb
from etl.utils import WAREHOUSE_PATH, TZ

try:
    import pytz
    tz = pytz.timezone(TZ)
except Exception:
    tz = None

try:
    from sms.sms import send_sms
except Exception:
    def send_sms(to, body):
        print(f"[SMS MOCK] to={to} body={body}")

def summary_text():
    now = datetime.now(tz) if tz else datetime.utcnow()
    y = (now - timedelta(days=1)).date()
    con = duckdb.connect(WAREHOUSE_PATH)
    sales = con.execute("SELECT COALESCE(SUM(amount),0) FROM etl.sales WHERE date::date = ?", [y]).fetchone()[0]
    expenses = con.execute("SELECT COALESCE(SUM(amount),0) FROM etl.expenses WHERE date::date = ?", [y]).fetchone()[0]
    con.close()
    profit = sales - expenses
    return f"Daily Summary ({y}): Sales {sales:,.0f} | Expenses {expenses:,.0f} | Profit {profit:,.0f}"

if __name__ == "__main__":
    recipients = os.getenv("SMS_RECIPIENTS", "").split(",")
    msg = summary_text()
    sent = 0
    for r in [x.strip() for x in recipients if x.strip()]:
        send_sms(r, msg); sent += 1
    if sent == 0:
        print("No recipients configured. Set SMS_RECIPIENTS=0300xxxxxxx,03xxxxxxxxx in .env")
    else:
        print(f"Sent summary to {sent} recipient(s)")
