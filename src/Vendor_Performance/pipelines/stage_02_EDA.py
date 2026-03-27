import sqlite3
from pathlib import Path
import time
import pandas as pd
from typing import Optional
from sqlalchemy import create_engine, exc
from src.Vendor_Performance.constants import CONFIG_FILE_PATH
# from src.Vendor_Performance.entity.config_entity import DataIngestionConfig, InventoryConfig
from src.Vendor_Performance.utils.common import read_yaml, create_directories
from src.Vendor_Performance import logger
from src.Vendor_Performance.pipelines.stage_01_data_ingestion import ingest_db



def create_connection() -> Optional[sqlite3.Connection]:
    try:
        config = read_yaml(CONFIG_FILE_PATH)

        # obtaining the database file from the config.yaml file
        db_path = config.inventory_data_db.database_name

        conn = sqlite3.connect(db_path)
        logger.info(f"Connected to database: {db_path}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database connection failed: {e}")
        return None


def Query(table_name: str, conn, column_name: list = "*", vendor_number: int = None):

    if isinstance(column_name, list):
        column_str = ", ".join(column_name)
    else:
        # This handles the default "*" or a single string
        column_str = column_name
    
    # query logic
    if not vendor_number:
        query = f"SELECT {column_str} FROM {table_name}"
        params = None
    else:
        query = f"SELECT {column_str} FROM {table_name} WHERE VendorNumber= {vendor_number}"
        # params = (vendor_number,)
        
    return pd.read_sql_query(query, conn)

def list_tables(tables: list, conn):
    try: 
        for t in tables['name']:
            query = f"Select count(*) as count from {t}"
            print('-'*50, f'{t}', '-'*50)
            print(f"Count of Records: {pd.read_sql(query, conn)['count'][0]}")
            print(
                pd.read_sql(f"SELECT * from {t} limit 10", conn)
            )
        logger.info(f"Table Loaded Successfully")

    except Exception as e:
        logger.error(f"Unable to list tables: {e}")
		

def get_vendor_summary(conn):
    try:
        query = """
        WITH sales_summary AS (
            SELECT 
                VendorNo, 
                Brand, 
                SUM(SalesDollars) AS TotalSalesDollar,
                SUM(SalesPrice) AS TotalSalesPrice,
                SUM(SalesQuantity) AS TotalSalesQuantity,
                SUM(ExciseTax) AS TotalExciseTax
            FROM sales
            GROUP BY VendorNo, Brand
        ),

        purchase_summary AS (
            SELECT 
                p.VendorName, 
                p.VendorNumber, 
                p.Brand,
                p.Description, 
                MAX(p.PurchasePrice) AS PurchasePrice,  
                MAX(pp.Price) AS ActualPrice,
                MAX(pp.Volume) AS Volume,
                SUM(p.Quantity) AS TotalPurchasedQuantity,  
                SUM(p.Dollars) AS TotalPurchaseDollar 
            FROM purchases p 
            JOIN purchase_prices pp
                ON p.Brand = pp.Brand
            WHERE p.PurchasePrice > 0
            GROUP BY p.VendorName, p.VendorNumber, p.Brand
        ),

        vendor_invoice_summary AS (
            SELECT 
                VendorNumber, 
                SUM(Freight) AS FreightCost 
            FROM vendor_invoice 
            GROUP BY VendorNumber
        )

        SELECT 
            ps.VendorName,
            ps.VendorNumber,
            ps.Brand,
            ps.Description,
            ps.PurchasePrice,
            ps.ActualPrice,
            ps.Volume,

            ps.TotalPurchasedQuantity,
            ps.TotalPurchaseDollar,

            ss.TotalSalesDollar,
            ss.TotalSalesPrice,
            ss.TotalSalesQuantity,
            ss.TotalExciseTax,

            vis.FreightCost

        FROM purchase_summary ps

        LEFT JOIN sales_summary ss
            ON ps.VendorNumber = ss.VendorNo
            AND ps.Brand = ss.Brand

        LEFT JOIN vendor_invoice_summary vis
            ON ps.VendorNumber = vis.VendorNumber

        ORDER BY ps.TotalPurchaseDollar DESC;

        """
        config = read_yaml(CONFIG_FILE_PATH)

        # obtaining the database file from the config.yaml file
        # summary = config.summary.summary_name
        
        df = pd.read_sql_query(query, conn)

        logger.info("Vendor sales summary created successfully")
        return df

    except Exception as e:
        logger.error(f"Unable to create vendor_sales_summary: {e}")
        return None

           
def clean_data(df):
    try:
        logger.info(f"Shape: {df.shape}")
        logger.info(f"Data types:\n{df.dtypes}")
        logger.info(f"Missing values:\n{df.isnull().sum()}")

        # Fix datatype
        df['Volume'] = df['Volume'].astype('float64')

        # Fill nulls
        df.fillna(0, inplace=True)

        # Strip spaces (IMPORTANT: assign back)
        df['VendorName'] = df['VendorName'].str.strip()
        df['Description'] = df['Description'].str.strip()

        # Feature engineering
        df['GrossProfit'] = df['TotalSalesDollar'] - df['TotalPurchaseDollar']
        df['ProfitMargin'] = (df['GrossProfit'] / df['TotalSalesDollar']) * 100
        df['StockTurnOver'] = df['TotalSalesQuantity'] / df['TotalPurchasedQuantity']
        df['SalestoPruchaseRatio'] = df['TotalSalesDollar'] / df['TotalPurchaseDollar']

        logger.info("Data cleaned successfully")
        return df

    except Exception as e:
        logger.exception("Error while cleaning data")
        return None


def final_summary(conn, df):
    try:
        config = read_yaml(CONFIG_FILE_PATH)
        summary_table = config.summary.summary_name
        db_path = config.inventory_data_db.database_name
        cursor = conn.cursor()
        
        query = """
        Create Table new_vendor_sales_summary (
        VendorNumber INT,
        VendorName VARCHAR(100),
        Brand INT,
        Description VARCHAR(100),
        PurchasePrice DECIMAL(10,2),
        ActualPrice DECIMAL(10,2),
        Volume,
        TotalPurchasedQuantity INT,
        TotalPurchaseDollar DECIMAL(15, 2),
        TotalSalesQuantity INT,
        TotalSalesDollar DECIMAL(15, 2),
        TotalSalesPrice DECIMAL(15, 2),
        TotalExciseTax DECIMAL(15, 2),
        FreightCost DECIMAL(15, 2),
        GrossProfit DECIMAL(15, 2),
        ProfitMargin DECIMAL(15, 2),
        StockTurnOver DECIMAL(15, 2),
        SalestoPruchaseRatio DECIMAL(15, 2),
        PRIMARY KEY (VendorNumber, Brand)
        );

        """

        # ingest_db(df=df, table_name=summary_table, engine=conn, db_path=db_path)
        df.to_sql(summary_table, conn, if_exists='replace', index=False)

        logger.info("Final summary table created and data inserted successfully")

    except Exception as e:
        logger.exception("Error while creating final summary")