import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
import os


class ETLService:

    def __init__(self, tgl):
        """
        Initialize ETLService with PostgreSQL connection string and CSV path.
        """
        self.tgl = tgl
        self.db_conn_str = 'postgresql+psycopg2://admin:admin@194.233.76.36:5432/postgres'
        self.base1 = '/opt/airflow/data/base1.csv'
        self.base2 = '/opt/airflow/data/base2.csv'
        self.merchant = '/opt/airflow/data/merchant.csv'
        self.engine = None
        try:
            self.engine = create_engine(self.db_conn_str)

            # Create schemas if they don't exist
            with self.engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))

        except Exception as e:
            raise Exception(f"Failed to connect to database or create schemas {self.db_conn_str}: {e}")

    def remove0num(self, row):
        row = list(row)
        del row[2]
        return ''.join(row)
    
    def load_bronze(self):
        """Load raw CSV data into bronze_banking table in bronze schema."""
        print(os.getcwd())
        try:
            for i in [self.base1, self.base2, self.merchant]:
                if not Path(i).is_file():
                    raise FileNotFoundError(f"CSV file not found at {i}")
                df = pd.read_csv(i, index_col = 0)
                df.to_sql(Path(i).stem, self.engine, if_exists='replace', index=False, schema='bronze')
            print("Bronze layer loaded successfully.")
            return "Bronze done"
        except Exception as e:
            raise Exception(f"Error loading bronze layer: {e}")

    def transform_silver(self, isFirst=True):
        """Transform bronze data into silver_banking table in silver schema with cleaning and standardization."""
        try:
            trx = pd.read_sql('SELECT * FROM bronze.bronze_base2', self.engine)
            acc = pd.read_sql('SELECT * FROM bronze.bronze_base1', self.engine)
            merchant = pd.read_sql('SELECT * FROM bronze.bronze_merchant', self.engine)

            # Standardize column names
            acc['address'] = acc['address'].replace({'\n':' '}, regex=True)
            acc['phone'] = acc['phone'].replace(r'[-+() ]', '', regex=True)
            acc['phone'] = acc['phone'].apply(lambda x: self.remove0num(x) if x[2] == '0' else x)

            # Convert dates to datetime
            acc['birthdate'] = pd.to_datetime(acc['birthdate'])
            acc['phone'] = acc['phone'].astype('int')
            trx['date'] = pd.to_datetime(trx['date'])

            # Save to silver
            if isFirst:
                trx.to_sql('silver_transaction', self.engine, if_exists='replace', index=False, schema='silver')
                acc.to_sql('silver_account', self.engine, if_exists='replace', index=False, schema='silver')
                merchant.to_sql('silver_merchant', self.engine, if_exists='replace', index=False, schema='silver')
            else:
                trx.to_sql('silver_transaction', self.engine, if_exists='append', index=False, schema='silver')
                
            print("Silver layer transformed successfully.")
            return "Silver done"
        except Exception as e:
            raise Exception(f"Error transforming silver layer: {e}")

    def transform_gold(self):
        """Transform silver data into dimensional model in gold schema."""
        try:
            df = pd.read_sql('SELECT * FROM silver.silver_banking', self.engine)

            # Dimension: Customer
            dim_customer = df[[
                'Customer_ID', 'First_Name', 'Last_Name', 'Age', 'Gender',
                'Address', 'City', 'Contact_Number', 'Email'
            ]].drop_duplicates()
            dim_customer['CustomerKey'] = range(1, len(dim_customer) + 1)
            dim_customer.to_sql('dim_customer', self.engine, if_exists='replace', index=False, schema='gold')

            # Dimension: Account
            dim_account = df[[
                'Account_Type', 'Account_Balance', 'Date_Of_Account_Opening'
            ]].drop_duplicates()
            dim_account['AccountKey'] = range(1, len(dim_account) + 1)
            dim_account.to_sql('dim_account', self.engine, if_exists='replace', index=False, schema='gold')

            # Dimension: Transaction Type
            dim_transaction_type = df[['Transaction_Type']].drop_duplicates()
            dim_transaction_type['TransactionTypeKey'] = range(1, len(dim_transaction_type) + 1)
            dim_transaction_type.to_sql('dim_transaction_type', self.engine, if_exists='replace', index=False, schema='gold')

            # Dimension: Branch
            dim_branch = df[['Branch_ID']].drop_duplicates()
            dim_branch['BranchKey'] = range(1, len(dim_branch) + 1)
            dim_branch.to_sql('dim_branch', self.engine, if_exists='replace', index=False, schema='gold')

            # Dimension: Loan
            dim_loan = df[['Loan_ID', 'Loan_Amount', 'Loan_Type', 'Interest_Rate',
                           'Loan_Term', 'Approval_Rejection_Date', 'Loan_Status']].drop_duplicates()
            dim_loan['LoanKey'] = range(1, len(dim_loan) + 1)
            dim_loan.to_sql('dim_loan', self.engine, if_exists='replace', index=False, schema='gold')

            # Dimension: Card
            dim_card = df[['CardID', 'Card_Type', 'Credit_Limit', 'Credit_Card_Balance',
                           'Minimum_Payment_Due', 'Payment_Due_Date',
                           'Last_Credit_Card_Payment_Date', 'Rewards_Points']].drop_duplicates()
            dim_card['CardKey'] = range(1, len(dim_card) + 1)
            dim_card.to_sql('dim_card', self.engine, if_exists='replace', index=False, schema='gold')

            # Dimension: Feedback
            dim_feedback = df[['Feedback_ID', 'Feedback_Date', 'Feedback_Type',
                               'Resolution_Status', 'Resolution_Date']].drop_duplicates()
            dim_feedback['FeedbackKey'] = range(1, len(dim_feedback) + 1)
            dim_feedback.to_sql('dim_feedback', self.engine, if_exists='replace', index=False, schema='gold')

            # Fact: Transactions
            fact_df = df.copy()
            print(fact_df.shape)

            fact_df = fact_df.merge(dim_customer[['CustomerKey', 'Customer_ID']], on='Customer_ID', how='left')
            print(fact_df.shape)
            fact_df = fact_df.merge(dim_account[['AccountKey', 'Account_Type']], on='Account_Type', how='left')
            print(fact_df.shape)
            fact_df = fact_df.merge(dim_transaction_type[['TransactionTypeKey', 'Transaction_Type']], on='Transaction_Type', how='left')
            fact_df = fact_df.merge(dim_branch[['BranchKey', 'Branch_ID']], on='Branch_ID', how='left')
            fact_df = fact_df.merge(dim_loan[['LoanKey', 'Loan_ID']], on='Loan_ID', how='left')
            fact_df = fact_df.merge(dim_card[['CardKey', 'CardID']], on='CardID', how='left')
            fact_df = fact_df.merge(dim_feedback[['FeedbackKey', 'Feedback_ID']], on='Feedback_ID', how='left')

            fact_transactions = fact_df[[
                'TransactionID', 'CustomerKey', 'AccountKey', 'TransactionTypeKey',
                'BranchKey', 'LoanKey', 'CardKey', 'FeedbackKey',
                'Transaction_Date', 'Transaction_Type', 'Transaction_Amount',
                'Account_Balance_After_Transaction', 'Anomaly'
            ]]
            fact_transactions.to_sql('fact_transactions', self.engine, if_exists='replace', index=False, schema='gold')

            print("Gold layer created successfully.")
            return "Gold done"
        except Exception as e:
            raise Exception(f"Error transforming gold layer: {e}")

    def close_conn(self):
        """Dispose the SQLAlchemy engine."""
        if self.engine:
            self.engine.dispose()
            print("Database connection closed.")


if __name__ == '__main__':
    etl = ETLService()
    etl.load_bronze()
    etl.transform_silver()