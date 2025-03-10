import os
import json
import requests
import pandas as pd
from typing import Dict, List, Optional, Union
from datetime import datetime
from enum import Enum
from dataclasses import dataclass
import logging

# Fivetran connector SDK imports
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()


# This creates the connector object that will use the update function defined in this connector.py file.
# This example does not use the schema() function. If it did, it would need to be included in the connector object definition. 

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('BMWVinDecoderConnector')

class BMWSeriesEnum(Enum):
    """Enum for BMW Series classification"""
    SERIES_1 = "1"
    SERIES_2 = "2"
    SERIES_3 = "3"
    SERIES_4 = "4"
    SERIES_5 = "5"
    SERIES_6 = "6"
    SERIES_7 = "7"
    SERIES_8 = "8"
    SERIES_X = "X"
    SERIES_M = "M"
    SERIES_I = "i"
    UNKNOWN = "Unknown"

@dataclass
class BMWVehicleInfo:
    """Data class for storing BMW vehicle information"""
    vin: str
    series: BMWSeriesEnum
    model_year: int
    model_name: str
    body_type: str
    engine_type: str
    transmission: str
    drive_type: str
    manufacturing_plant: str
    production_date: Optional[str] = None
    decoded_date: str = datetime.now().isoformat()

    def to_record(self):
        """Convert to Fivetran Record"""
        return {
            "vin": self.vin,
            "series": self.series.value,
            "model_year": self.model_year,
            "model_name": self.model_name,
            "body_type": self.body_type,
            "engine_type": self.engine_type,
            "transmission": self.transmission,
            "drive_type": self.drive_type,
            "manufacturing_plant": self.manufacturing_plant,
            "production_date": self.production_date,
            "decoded_date": self.decoded_date
        }

class BMWVinDecoder:
    """VIN decoder specifically for BMW vehicles"""
    
    def __init__(self):
        self.base_url = "https://vpic.nhtsa.dot.gov/api/vehicles"
        self._plant_codes = {
            'A': 'Greer, SC, USA',
            'B': 'Dingolfing, Germany',
            'C': 'Munich, Germany',
            'L': 'Leipzig, Germany',
            'N': 'Regensburg, Germany',
            'P': 'Munich, Germany',
            'R': 'Spartanburg, SC, USA',
            'U': 'Rosslyn, South Africa',
            'W': 'Born, Netherlands'
        }

    def _validate_bmw_vin(self, vin: str) -> bool:
        """
        Validate if the VIN is for a BMW vehicle
        
        Args:
            vin (str): 17-character VIN
            
        Returns:
            bool: True if valid BMW VIN
        """
        if len(vin) != 17:
            raise ValueError("VIN must be 17 characters long")
        
        # BMW manufacturer codes (WBA, WBS, WBY, 4US)
        bmw_prefixes = ['WBA', 'WBS', 'WBY', '4US']
        if vin[:3] not in bmw_prefixes:
            raise ValueError("Not a valid BMW VIN prefix")
        
        return True

    def _determine_bmw_series(self, model_name: str) -> BMWSeriesEnum:
        """Determine BMW series from model name"""
        for series in BMWSeriesEnum:
            if series.value in model_name:
                return series
        return BMWSeriesEnum.UNKNOWN

    def decode_bmw_vin(self, vin: str) -> BMWVehicleInfo:
        """
        Decode a BMW VIN with enhanced information
        
        Args:
            vin (str): 17-character BMW VIN
            
        Returns:
            BMWVehicleInfo: Detailed BMW vehicle information
        """
        try:
            #self._validate_bmw_vin(vin)
            
            url = f"{self.base_url}/decodevin/{vin}?format=json"
            response = requests.get(url)
            response.raise_for_status()
            
            data = response.json()
            results = {item['Variable']: item['Value'] for item in data['Results'] if item['Value'] and item['Value'] != '0'}
            
            # Extract BMW-specific information
            model_name = results.get('Model', 'Unknown')
            series = self._determine_bmw_series(model_name)
            
            return BMWVehicleInfo(
                vin=vin,
                series=series,
                model_year=int(results.get('ModelYear', 0)),
                model_name=model_name,
                body_type=results.get('BodyClass', 'Unknown'),
                engine_type=results.get('EngineConfiguration', 'Unknown'),
                transmission=results.get('TransmissionStyle', 'Unknown'),
                drive_type=results.get('DriveType', 'Unknown'),
                manufacturing_plant=self._plant_codes.get(vin[11], 'Unknown'),
                production_date=results.get('DateProduced', None)
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise

    def get_bmw_recalls(self, vin: str) -> List[Dict]:
        """Get recall information for a BMW vehicle"""
        try:
            #self._validate_bmw_vin(vin)
            
            url = f"{self.base_url}/recalls/vin/{vin}?format=json"
            response = requests.get(url)
            response.raise_for_status()
            
            data = response.json()
            return data.get('Results', [])
            
        except Exception as e:
            logger.error(f"Error fetching recalls: {str(e)}")
            raise


# Define Fivetran connector classes
class BMWVehicleSource():
    """Fivetran Source for BMW vehicle information"""
    
    def __init__(self, config: Dict):
        """
        Initialize the source
        
        Args:
            config: The connector configuration
        """
        self.vins = config.get('vins', [])
        self.decoder = BMWVinDecoder()
        
    def get_primary_keys(self) -> Dict[str, List[str]]:
        """Define primary keys for tables"""
        return {
            "bmw_vehicles": ["vin"],
            "bmw_recalls": ["vin", "campaign_number"]
        }
    
    def get_state(self) -> Dict:
        """Get the current sync state"""
        # For simplicity, we're not maintaining state
        # In a real implementation, you might track last sync time or VINs
        return {}
    
    def update_state(self, new_state: Dict) -> None:
        """Update the sync state"""
        # Not implementing state management for simplicity
        pass
    
    def fetch_records(self, table_name: str, last_state: Dict) -> List[{}]:
        """
        Fetch records for the specified table
        
        Args:
            table_name: Name of the table to fetch
            last_state: Previous sync state
            
        Returns:
            List of Fivetran Records
        """
        records = []
        
        if table_name == "bmw_vehicles":
            for vin in self.vins:
                try:
                    vehicle_info = self.decoder.decode_bmw_vin(vin)
                    records.append(vehicle_info.to_record())
                except Exception as e:
                    logger.error(f"Error processing VIN {vin}: {str(e)}")
        
        elif table_name == "bmw_recalls":
            for vin in self.vins:
                try:
                    recalls = self.decoder.get_bmw_recalls(vin)
                    for recall in recalls:
                        record = {
                            "vin": vin,
                            "campaign_number": recall.get("CampaignNumber", "Unknown"),
                            "component": recall.get("Component", "Unknown"),
                            "summary": recall.get("Summary", ""),
                            "consequence": recall.get("Consequence", ""),
                            "remedy": recall.get("Remedy", ""),
                            "recall_date": recall.get("ReportReceivedDate", "")
                        }
                        records.append(record)
                except Exception as e:
                    logger.error(f"Error fetching recalls for VIN {vin}: {str(e)}")
        
        return records


# Define Fivetran connector schema
class BMWVinDecoderSchema():
    """Schema definition for BMW VIN Decoder connector"""
    
    def get_tables(self) -> List[str]:
        """Get list of tables in this schema"""
        return ["bmw_vehicles"] # "bmw_vehicles", "bmw_recalls"
    
    def get_columns(self, table_name: str) -> Dict[str, str]:
        """
        Get column definitions for a table
        
        Args:
            table_name: The table to get columns for
            
        Returns:
            Dictionary of column names to data types
        """
        if table_name == "bmw_vehicles":
            return {
                "vin": "STRING",
                "series": "STRING",
                "model_year": "INT",
                "model_name": "STRING",
                "body_type": "STRING",
                "engine_type": "STRING",
                "transmission": "STRING", 
                "drive_type": "STRING",
                "manufacturing_plant": "STRING",
                "production_date": "STRING",
                "decoded_date": "STRING"
            }
        elif table_name == "bmw_recalls":
            return {
                "vin": "STRING",
                "campaign_number": "STRING",
                "component": "STRING",
                "summary": "STRING",
                "consequence": "STRING",
                "remedy": "STRING",
                "recall_date": "STRING"
            }
        return {}


# Main Fivetran connector class
class BMWVinDecoderConnector():
    """Fivetran connector for BMW VIN decoding"""
    
    def setup(self, config: Dict) -> None:
        """
        Set up the connector
        
        Args:
            config: Connector configuration
        """
        self.schema = BMWVinDecoderSchema()
        self.source = BMWVehicleSource(config)
    
    def test_connection(self, config: Dict) -> bool:
        """
        Test the connection to the source
        
        Args:
            config: Connector configuration
            
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Test API connection with a sample VIN
            decoder = BMWVinDecoder()
            # Use the first VIN from config, or a default one
            test_vin = config.get('vins', ["WBA3A5C51CF256651"])[0]
            decoder.decode_bmw_vin(test_vin)
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False

def schema(configuration: dict):

    return [
        {
            "table": "crypto",  # Name of the table in the destination.
            "primary_key": ["msg"],  # Primary key column(s) for the table.
            # No columns are defined, meaning the types will be inferred.
        }
    ]

# Example usage with Fivetran SDK setup
def update(configuration: dict, state: dict):
    """Run the connector"""
    # Configuration would normally come from Fivetran
    config = {
        'vins': [
            "5UXCW2C09L9C15882",
            "WBAHF3C03NWX42344"  # Example second VIN
        ]
    }
    
    # Create and run connector
    bmwConnector = BMWVinDecoderConnector()
    bmwConnector.setup(config)
    
    # In a real implementation, Fivetran would call the appropriate methods
    # This is just a demonstration of how it would work
    if bmwConnector.test_connection(config):
        logger.info("Connection test successful")
        
        # Sync each table
        for table in bmwConnector.schema.get_tables():
            logger.info(f"Syncing table: {table}")
            records = bmwConnector.source.fetch_records(table, {})
            logger.info(f"Fetched {len(records)} records for {table}")
            
            # In a real connector, Fivetran would handle the record processing
            # This is just for demonstration
            for record in records[:2]:  # Show first 2 records
                logger.info(f"Record: {record}")
                yield op.upsert(table, record)

        yield op.checkpoint(state)
    else:
        logger.error("Connection test failed")

connector = Connector(update=update)

if __name__ == "__main__":
    connector.debug()


