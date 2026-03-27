import os
from pathlib import Path
import time
from src.Vendor_Performance.constants import CONFIG_FILE_PATH
from src.Vendor_Performance.entity.config_entity import DataIngestionConfig, InventoryConfig
from src.Vendor_Performance.utils.common import read_yaml, create_directories
from src.Vendor_Performance import logger
from src.Vendor_Performance.config.configuration import ConfigurationManager, Inventory_DB
# from src.Vendor_Performance.config.ingestion_db import load_raw_data
from src.Vendor_Performance.pipelines.stage_01_data_ingestion import load_raw_data
from src.Vendor_Performance.pipelines.stage_02_EDA import (clean_data, create_connection, Query,
														    list_tables,  get_vendor_summary, final_summary)

if __name__== "__main__":
	config = ConfigurationManager()
	inventory_data_config = config.get_inventory_data_config()
	inventory = Inventory_DB(config=inventory_data_config)
	inventory.create_inventory_db_file()

	
	start = time.time()
	
	try:
		load_raw_data()
		end = time.time()
		logger.info(f"Data ingested successfully in {end - start:.2f}s")

	except Exception as e:
		logger.error(f"Data ingestion failed: {e}")
	
	try:
		conn =create_connection()
		tables = Query(conn=conn, table_name='sqlite_master')
		list_tables(tables=tables, conn=conn)

	except Exception as e:
		logger.error(f"connection failed: {e}")

	try:
		conn = create_connection()

		if not conn:
			raise Exception("Connection failed")

		# STEP 1: Summary
		summary = get_vendor_summary(conn)

		if summary is None:
			raise Exception("Summary creation failed")

		logger.info("Vendor sales summary created")

		# STEP 2: Cleaning
		cleaned_dataset = clean_data(summary)

		if cleaned_dataset is None:
			raise Exception("Data cleaning failed")

		logger.info("Data cleaning completed")

		# STEP 3: Final table
		final_summary(conn=conn, df=cleaned_dataset)

		logger.info("Final summary ingested into database successfully")

	except Exception as e:
		logger.exception(f"Pipeline failed: {e}")

	finally:
		if conn:
			conn.close()

	

	
		