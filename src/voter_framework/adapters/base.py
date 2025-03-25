"""
Base adapter for state-specific voter data access.
"""

from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Optional
from ..normalizers.base import BaseDataNormalizer

class BaseStateAdapter(ABC):
    """Abstract base class for state-specific voter data adapters."""
    
    def __init__(self, state_code: str):
        """
        Initialize the adapter.
        
        Args:
            state_code: Two-letter state code (e.g., 'WA', 'OR')
        """
        self.state_code = state_code
        self._normalizer: Optional[BaseDataNormalizer] = None
    
    @abstractmethod
    def fetch_data(self) -> pd.DataFrame:
        """
        Fetch voter registration data from the state.
        
        Returns:
            DataFrame containing raw voter data
        """
        pass
    
    def get_normalizer(self) -> BaseDataNormalizer:
        """
        Get the normalizer for this state's data format.
        
        Returns:
            DataNormalizer instance
        """
        if self._normalizer is None:
            self._normalizer = self._create_normalizer()
        return self._normalizer
    
    @abstractmethod
    def _create_normalizer(self) -> BaseDataNormalizer:
        """
        Create a normalizer specific to this state's data format.
        
        Returns:
            DataNormalizer instance
        """
        pass
    
    @abstractmethod
    def get_schema_mapping(self) -> Dict[str, str]:
        """
        Get the mapping between state fields and common schema fields.
        
        Returns:
            Dictionary mapping state field names to common schema field names
        """
        pass
    
    def validate_data_access(self) -> Dict:
        """
        Validate access to the state's voter data.
        
        Returns:
            Dictionary containing validation results
        """
        try:
            # Try to fetch a small sample of data
            sample = self.fetch_data()
            return {
                'success': True,
                'message': f'Successfully accessed {self.state_code} voter data',
                'record_count': len(sample),
                'columns': list(sample.columns)
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'Failed to access {self.state_code} voter data: {str(e)}'
            } 