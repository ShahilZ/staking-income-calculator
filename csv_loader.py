import csv
import logging
from typing import List, Dict, Any, Optional, Iterator
from pathlib import Path

logger = logging.getLogger(__name__)

def skip_empty_rows(reader: Iterator[List[str]]) -> Iterator[List[str]]:
    """
    Skip empty rows from a CSV reader.
    
    Args:
        reader (Iterator[List[str]]): The CSV reader iterator
        
    Returns:
        Iterator[List[str]]: Iterator that skips empty rows
    """
    for row in reader:
        if any(cell.strip() for cell in row):
            yield row

def load_csv(file_path: str, has_header: bool = True, delimiter: str = ',') -> List[Dict[str, Any]]:
    """
    Load data from a CSV file.
    
    Args:
        file_path (str): Path to the CSV file
        has_header (bool): Whether the CSV has a header row
        delimiter (str): The delimiter character used in the CSV
        
    Returns:
        List[Dict[str, Any]]: List of dictionaries, each representing a row
    """
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")
    
    logger.info(f"Loading CSV from {file_path}")
    data = []
    
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile, delimiter=delimiter)
            # Skip empty rows
            reader = skip_empty_rows(reader)
            
            if has_header:
                headers = [h.strip() for h in next(reader)]
                logger.info(f"Headers: {headers}")
                for row in reader:
                    if len(row) != len(headers):
                        logger.warning(f"Skipping row with incorrect number of columns: {row}")
                        continue
                    data.append(dict(zip(headers, row)))
            else:
                for row in reader:
                    data.append({f"column_{i}": value for i, value in enumerate(row)})
        
        logger.info(f"Successfully loaded {len(data)} rows from CSV")
        return data
    
    except Exception as e:
        logger.error(f"Error loading CSV: {str(e)}")
        raise

def save_csv(data: List[Dict[str, Any]], file_path: str, headers: Optional[List[str]] = None) -> None:
    """
    Save data to a CSV file.
    
    Args:
        data (List[Dict[str, Any]]): List of dictionaries to save
        file_path (str): Path where to save the CSV file
        headers (Optional[List[str]]): Headers to use. If None, uses keys from first row
    """
    if not data:
        logger.warning("No data to save to CSV")
        return
    
    file_path = Path(file_path)
    logger.info(f"Saving CSV to {file_path}")
    
    try:
        # Determine headers if not provided
        if headers is None:
            headers = list(data[0].keys())
        
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
        
        logger.info(f"Successfully saved {len(data)} rows to CSV")
    
    except Exception as e:
        logger.error(f"Error saving CSV: {str(e)}")
        raise 