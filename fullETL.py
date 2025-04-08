import sqlite3
import pandas as pd
from datetime import datetime
import os
import re

class FullPipline:

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path)

    def extract(self, csv_path: str) -> pd.DataFrame:
        """Extract data from the CSV file"""
        try:
            df = pd.read_csv(csv_path)
            return df
        except Exception as e:
            raise Exception(f"Extraction failed: {str(e)}")

    def transform(self, df: pd.DataFrame, transaction_date: str) -> pd.DataFrame:
        """Transform data to match database schema"""
        try:
            # Validate required columns before accessing them.
            required_columns = ['id', 'category', 'description', 'quantity', 'amount_excl_tax', 'amount_inc_tax']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required column(s): {', '.join(missing_columns)}")


            # Check for null values in columns
            for col in required_columns:
                if df[col].isnull().any():
                    raise ValueError(f"Missing value in required column: {col}")

            # Rename columns to match database schema
            df = df.rename(columns={
                'description': 'name'
            })

            # Parse and assign date
            parsed_date = datetime.strptime(transaction_date, "%d/%m/%Y").date()
            df['transaction_date'] = parsed_date

            # Reorder columns to match database schema
            df = df[['id', 'transaction_date', 'category', 'name',
                     'quantity', 'amount_excl_tax', 'amount_inc_tax']]

            # Check for duplicate IDs in the same file/batch
            if df['id'].duplicated().any():
                raise ValueError("Duplicate IDs found in the same file")

            # Check for negative quantities
            if (df['quantity'] < 0).any():
                raise ValueError("Negative quantities are not accepted")

            # If incorrect tax, update to correct +20% value
            df['expected_inc_tax'] = (df['amount_excl_tax'] * 1.2).round(2)
            incorrect_tax = df['amount_inc_tax'].round(2) != df['expected_inc_tax']
            if incorrect_tax.any():
                print(f"Fixing incorrect tax for {incorrect_tax.sum()} row(s)")
                df.loc[incorrect_tax, 'amount_inc_tax'] = df.loc[incorrect_tax, 'expected_inc_tax']

            df = df.drop(columns=['expected_inc_tax'])

            return df
        except Exception as e:
            raise Exception(f"Transformation failed: {str(e)}")

    def load(self, df: pd.DataFrame) -> None:
        """Load data into SQLite database, skipping duplicates"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get existing IDs
                existing_ids = pd.read_sql("SELECT id FROM transactions", conn)['id'].tolist()

                # Filter out duplicates
                new_records = df[~df['id'].isin(existing_ids)]

                if len(new_records) < len(df):
                    print(f"Note: Skipped {len(df) - len(new_records)} duplicate transactions")

                # Write to database
                new_records.to_sql('transactions', conn, if_exists='append', index=False)

        except Exception as e:
            raise Exception(f"Loading failed: {str(e)}")

    def run_pipeline(self, csv_path: str) -> None:

        # Extract date from filename, retail_15_01_2022.csv → "15/01/2022"
        filename = os.path.basename(csv_path)
        match = re.search(r'retail_(\d{2})_(\d{2})_(\d{4})', filename)
        if not match:
            raise ValueError("Filename must include date in format retail_DD_MM_YYYY.csv")
        day, month, year = match.groups()
        transaction_date = f"{day}/{month}/{year}"

        """Execute Full pipeline"""
        try:
            df = self.extract(csv_path)
            df = self.transform(df, transaction_date)
            self.load(df)
            print("Full pipeline completed successfully")
        except Exception as e:
            print(f"ETL pipeline failed: {str(e)}")
            raise


if __name__ == "__main__":
    csv_path = "retail_15_01_2022.csv"
    db_path = "retail.db"

    pipeline = FullPipline(db_path)
    pipeline.run_pipeline(csv_path)

