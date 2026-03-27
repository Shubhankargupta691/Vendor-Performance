from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataIngestionConfig:
    root_dir: Path
    file_extension: str

@dataclass(frozen=True)
class InventoryConfig:
    root_dir: Path
    database_name: Path

@dataclass(frozen=True)
class SymmaryConfig:
    summary_name: str