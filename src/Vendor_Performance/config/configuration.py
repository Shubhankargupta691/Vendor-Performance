import os
from pathlib import Path
from src.Vendor_Performance import logger
from src.Vendor_Performance.constants import CONFIG_FILE_PATH
from src.Vendor_Performance.entity.config_entity import DataIngestionConfig, InventoryConfig
from src.Vendor_Performance.utils.common import read_yaml, create_directories


class ConfigurationManager:
    def __init__(self, config_filepath = CONFIG_FILE_PATH):
    
        self.config = read_yaml(config_filepath)
        create_directories([self.config.artifacts_root])
        
    def get_data_ingestion_config(self) -> DataIngestionConfig:
        config = self.config.data_ingestion

        create_directories([config.root_dir])

        data_ingestion_config = DataIngestionConfig(
            root_dir=config.root_dir,
            file_extension=config.file_extension 
        )

        return data_ingestion_config
    
    def get_inventory_data_config(self) -> InventoryConfig:
        config = self.config.inventory_data_db

        create_directories([config.root_dir])
        
        inventory_data_config = InventoryConfig(
            root_dir=config.root_dir,
            database_name=config.database_name
        )

        return inventory_data_config


class Inventory_DB:
    def __init__(self, config: InventoryConfig):
        self.config = config

    def create_inventory_db_file(self):
        file_path = self.config.database_name
        filedir, _ = os.path.split(file_path)

        if not os.path.exists(filedir):  
            create_directories([filedir])
            logger.info(f"Created directory at: {file_path}")
        else:
            logger.info(f"directory already exists: {file_path}")
        
        if not os.path.exists(file_path):
            with open(file_path, 'w') as f:
                pass
            logger.info(f"Created empty file: {file_path}")
        else:
            logger.info(f"File already exists: {file_path}")