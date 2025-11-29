from textual.app import App, ComposeResult
from textual.containers import Container
from textual.widgets import Header, Footer, DataTable, Static
from textual.binding import Binding
import psycopg2
from datetime import datetime

# Database Config (Localhost because we are running outside Docker)
DB_CONFIG = {
    "dbname": "fraud_detection_db",
    "user": "admin",
    "password": "password",
    "host": "localhost",
    "port": "5432"
}

class FraudMonitorApp(App):
    CSS = """
    Screen {
        layout: vertical;
    }
    DataTable {
        height: 1fr;
        border: solid green;
    }
    .fraud-header {
        text-align: center;
        color: red;
        text-style: bold;
        padding: 1;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("r", "refresh_table", "Force Refresh"),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("🚨 LIVE FRAUD DETECTION FEED 🚨", classes="fraud-header")
        yield DataTable()
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.cursor_type = "row"
        table.zebra_stripes = True
        
        # Define Columns
        table.add_columns("Time", "Txn ID", "User ID", "Amount ($)", "Status")

        # Set interval to refresh data every 2 seconds
        self.set_interval(2.0, self.refresh_table)

    def refresh_table(self) -> None:
        table = self.query_one(DataTable)
        
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            query = "SELECT timestamp, transaction_id, user_id, amount FROM fraud_alerts ORDER BY timestamp DESC LIMIT 20"
            
            # Fetch data
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
            conn.close()

            # Clear and repopulate
            table.clear()
            for row in rows:
                ts, txn_id, user_id, amount = row
                
                # Format the row
                fmt_time = ts.strftime("%H:%M:%S")
                fmt_amount = f"${float(amount):,.2f}"
                
                # Add to table
                table.add_row(fmt_time, txn_id, str(user_id), fmt_amount, "SUSPICIOUS")

        except Exception as e:
            # If DB connection fails (e.g. container down)
            pass

if __name__ == "__main__":
    app = FraudMonitorApp()
    app.run()