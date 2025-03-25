"""
Core processor for handling voter registration data.
"""

from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
from ..adapters.base import BaseStateAdapter
from ..normalizers.base import BaseDataNormalizer
from .schema import VoterSchema
from .validator import DataValidator

class VoterDataProcessor:
    """Core class for processing voter registration data from different states."""
    
    def __init__(self):
        """Initialize the processor with schema and validator."""
        self.processed_data: Dict[str, pd.DataFrame] = {}
        self.quality_metrics: Dict[str, Dict] = {}
        self.schema = VoterSchema()
        self.validator = DataValidator()
    
    def process_state_data(self, adapter: BaseStateAdapter) -> pd.DataFrame:
        """
        Process voter registration data for a specific state.
        
        Args:
            adapter: State-specific adapter instance
            
        Returns:
            DataFrame containing normalized voter data
        """
        # Fetch raw data
        raw_data = adapter.fetch_data()
        
        # Get normalizer
        normalizer = adapter.get_normalizer()
        
        # Normalize data
        normalized_data = normalizer.normalize(raw_data)
        
        # Store processed data
        self.processed_data[adapter.state_code] = normalized_data
        
        # Calculate quality metrics
        self.quality_metrics[adapter.state_code] = self._calculate_quality_metrics(normalized_data)
        
        return normalized_data
    
    def _calculate_quality_metrics(self, data: pd.DataFrame) -> Dict:
        """Calculate data quality metrics for the processed dataset."""
        metrics = {
            'total_records': len(data),
            'missing_values': data.isnull().sum().to_dict(),
            'unique_values': {col: data[col].nunique() for col in data.columns},
            'data_types': data.dtypes.to_dict()
        }
        return metrics
    
    def get_quality_report(self, state_code: Optional[str] = None) -> Dict:
        """
        Get data quality report for one or all states.
        
        Args:
            state_code: Optional state code to get report for specific state
            
        Returns:
            Dictionary containing quality metrics
        """
        if state_code:
            if state_code not in self.quality_metrics:
                raise ValueError(f"No data processed for state: {state_code}")
            return self.quality_metrics[state_code]
        return self.quality_metrics
    
    def compare_states(self, state_codes: List[str]) -> Dict:
        """
        Compare voter data between different states.
        
        Args:
            state_codes: List of state codes to compare
            
        Returns:
            Dictionary containing comparison metrics
        """
        if not all(code in self.processed_data for code in state_codes):
            missing = [code for code in state_codes if code not in self.processed_data]
            raise ValueError(f"Missing data for states: {missing}")
            
        comparison = {
            'record_counts': {
                code: len(self.processed_data[code])
                for code in state_codes
            },
            'field_comparison': self._compare_fields(state_codes)
        }
        return comparison
    
    def _compare_fields(self, state_codes: List[str]) -> Dict:
        """Compare fields between different state datasets."""
        fields = {}
        for code in state_codes:
            df = self.processed_data[code]
            fields[code] = {
                'columns': list(df.columns),
                'dtypes': df.dtypes.to_dict(),
                'null_counts': df.isnull().sum().to_dict()
            }
        return fields
    
    def generate_report(self, data: pd.DataFrame, state_code: str) -> Dict:
        """
        Generate a data quality report for a state's voter data.
        
        Args:
            data: Normalized voter data
            state_code: Two-letter state code
            
        Returns:
            Dictionary containing report data
        """
        report = {
            'state_code': state_code,
            'generation_date': datetime.now().isoformat(),
            'total_voters': len(data),
            'column_statistics': {},
            'data_quality_metrics': {}
        }
        
        # Calculate column statistics
        for column in data.columns:
            stats = {
                'null_count': data[column].isnull().sum(),
                'null_percentage': (data[column].isnull().sum() / len(data)) * 100,
                'unique_values': data[column].nunique()
            }
            
            if data[column].dtype in ['int64', 'float64']:
                stats.update({
                    'min': data[column].min(),
                    'max': data[column].max(),
                    'mean': data[column].mean(),
                    'median': data[column].median()
                })
            
            report['column_statistics'][column] = stats
        
        # Calculate data quality metrics
        report['data_quality_metrics'] = {
            'completeness': self._calculate_completeness(data),
            'consistency': self._calculate_consistency(data),
            'validity': self._calculate_validity(data)
        }
        
        return report
    
    def _calculate_completeness(self, data: pd.DataFrame) -> float:
        """Calculate data completeness score."""
        return 1 - (data.isnull().sum().sum() / (len(data) * len(data.columns)))
    
    def _calculate_consistency(self, data: pd.DataFrame) -> float:
        """Calculate data consistency score."""
        # Implement consistency checks (e.g., valid dates, valid ZIP codes)
        return 0.0  # Placeholder
    
    def _calculate_validity(self, data: pd.DataFrame) -> float:
        """Calculate data validity score."""
        # Implement validity checks (e.g., valid names, valid addresses)
        return 0.0  # Placeholder 