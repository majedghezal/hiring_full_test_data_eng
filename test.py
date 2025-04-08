import unittest
import pandas as pd
import sqlite3
from fullETL import FullPipline

DB_PATH = "test_retail_transactions.db"
CSV_PATH = "retail_15_01_2022.csv"
DATE_STR = "15/01/2022"

class TestFullPipeline(unittest.TestCase):

    def setUp(self):
        """Initialize fresh database before test"""
        self.pipeline = FullPipline(DB_PATH)
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    id TEXT PRIMARY KEY,
                    transaction_date TEXT,
                    category TEXT,
                    name TEXT,
                    quantity INTEGER,
                    amount_excl_tax REAL,
                    amount_inc_tax REAL
                )
            ''')
            conn.execute("DELETE FROM transactions")
            conn.commit()

    def test_correction_tax_amount(self):
        df = pd.DataFrame([
            {'id': '4579', 'category': 'SELL', 'description': 'Apple iPhone 14', 'quantity': 3,
             'amount_excl_tax': 2500.0, 'amount_inc_tax': 3300.0},
            {'id': '7948', 'category': 'SELL', 'description': 'Dell XPS 13', 'quantity': 2,
             'amount_excl_tax': 2000.0, 'amount_inc_tax': 2500.0}  #
        ])

        transformed = self.pipeline.transform(df, DATE_STR)

        self.assertEqual(transformed.iloc[0]['amount_inc_tax'], 3000.0)
        self.assertEqual(transformed.iloc[1]['amount_inc_tax'], 2400.0)

    def test_negative_quantity(self):
        df = pd.DataFrame([
            {'id': '4587', 'category': 'SELL', 'description': 'Levis Jeans', 'quantity': 6,
             'amount_excl_tax': 300.0, 'amount_inc_tax': 360.0},
            {'id': '7589', 'category': 'BUY', 'description': 'Levis Jeans', 'quantity': -5,
             'amount_excl_tax': 250.0, 'amount_inc_tax': 300.0}

        ])
        with self.assertRaises(Exception) as context:
            self.pipeline.transform(df, DATE_STR)

        print(str(context.exception))
        self.assertIn("Negative quantities", str(context.exception))



    def test_missing_required_value(self):
        df = pd.DataFrame([
            {'id': '6589', 'category': None, 'description': 'USB Cable', 'quantity': 1,
             'amount_excl_tax': 5.0, 'amount_inc_tax': 6.0}
        ])

        with self.assertRaises(Exception) as context:
            self.pipeline.transform(df, DATE_STR)

        print(str(context.exception))
        self.assertIn("Missing value in required column: ", str(context.exception))

    def test_column_mismatch(self):
        df = pd.DataFrame([
            {'category': 'SELL', 'description': 'Fitbit Charge 5', 'quantity': 1,
             'amount_excl_tax': 10.0, 'amount_inc_tax': 12.0}
        ])
        with self.assertRaises(Exception) as context:
            self.pipeline.transform(df, DATE_STR)

        print(str(context.exception))
        self.assertIn("Missing required column(s): id", str(context.exception))

    def test_conflicting_duplicate_ids(self):
        df = pd.DataFrame([
            {'id': '1715', 'category': 'BUY', 'description': 'Ray-Ban Sunglasses', 'quantity': 6,
             'amount_excl_tax': 600.0, 'amount_inc_tax': 720.0},
            {'id': '1715', 'category': 'SELL', 'description': 'Headphones', 'quantity': 3,
             'amount_excl_tax': 150.0, 'amount_inc_tax': 180.0}
        ])

        with self.assertRaises(Exception) as context:
            self.pipeline.transform(df, DATE_STR)

        print(str(context.exception))
        self.assertIn("Duplicate IDs found", str(context.exception))

    def test_run_pipeline_loads_expected_row_count(self):
        """Test pipeline execution with the given csv file retail_15_01_2022"""

        self.pipeline.run_pipeline(CSV_PATH)

        with sqlite3.connect(DB_PATH) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM transactions WHERE transaction_date = '2022-01-15'"
            ).fetchone()[0]

        self.assertEqual(count, 54)
        print(f"{count} transactions in 2022-01-15. Correct ")
        # Testing if in the generated database it contains 54 transactions in 2022-01-15

if __name__ == '__main__':
    unittest.main()