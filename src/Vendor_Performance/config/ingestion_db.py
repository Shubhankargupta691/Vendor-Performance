# import os
# from pathlib import Path
# import time
# import pandas as pd
# from sqlalchemy import create_engine
# from src.Vendor_Performance.constants import CONFIG_FILE_PATH
# # from src.Vendor_Performance.entity.config_entity import DataIngestionConfig, InventoryConfig
# from src.Vendor_Performance.utils.common import read_yaml, create_directories
# from src.Vendor_Performance import logger

# # Ingest data to Database
# def ingest_db(df, table_name, db_path, engine):
#     try:
#         if os.path.exists(db_path):
#             df.to_sql(table_name, con=engine, if_exists='replace', index = False)
#             return True
#         else:
#             logger.info("Database does not exist. Creating new database...")
#     except Exception as e:
#         print(f"Error inserting data: {e}")
#         return False
    


# def load_raw_data():
#     # Obtaining Data from the config.yaml file
#     config = read_yaml(CONFIG_FILE_PATH)
#     f = config.data_ingestion.root_dir
    
#     # obtaining the extension from the config.yaml file
#     ext = config.data_ingestion.file_extension

#     # obtaining the database file from the config.yaml file
#     db_path = config.inventory_data_db.database_name

#     start = time.time()
#     for file in os.listdir(f):

#         if file.endswith(ext):
#             # Joining files path
#             file_path = os.path.join(f, file)
            
#             # 2. Read the file
#             df = pd.read_csv(file_path)

#             logger.info(f"Displaying File Shape {file}: {df.shape}")
            
#             # engine is initialized
#             engine = create_engine(f'sqlite:///{db_path}')

#             logger.info(f"Ingesting {file} into Database: {db_path}")

#             # Ingest into SQL
#             ingest_db(df=df, table_name=file[:-4], db_path=db_path, engine=engine)

#     end = time.time()
#     total_time = (end - start) / 60
#     logger.info(f"--------------------------Ingestion Complete--------------------------")
#     logger.info(f"Total Time Taken {total_time} in minutes")



