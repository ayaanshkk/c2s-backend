# backend/properties/services/property_service.py

import os
import logging
from typing import Optional, Dict, Any, List
from backend.properties.repositories.property_repository import PropertyRepository
from backend.properties.repositories import PropertyStatusRepository
from backend.properties.repositories.user_repository import UserRepository

logger = logging.getLogger(__name__)


class PropertyService:
    """
    Service layer for property management
    Handles business logic, validation, and orchestration
    """
    
    def __init__(self):
        self.property_repo = PropertyRepository()
        self.status_repo = PropertyStatusRepository()
        self.user_repo = UserRepository()
    
    def get_all_properties(self, filters: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """Get all properties with optional filters"""
        try:
            return self.property_repo.get_all_properties(filters)
        except Exception as e:
            logger.error(f"Error fetching properties: {e}")
            return []
    
    def get_property_by_id(self, property_id: int) -> Optional[Dict]:
        """Get property by ID"""
        try:
            return self.property_repo.get_property_by_id(property_id)
        except Exception as e:
            logger.error(f"Error fetching property {property_id}: {e}")
            return None
    
    def create_property(self, data: Dict[str, Any], created_by: int) -> Optional[Dict]:
        """Create new property"""
        try:
            # Validate input data
            required_fields = ['property_name', 'address', 'city']
            missing_fields = [field for field in required_fields if not data.get(field)]
            
            if missing_fields:
                raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
            
            # Call repository to create
            result = self.property_repo.create_property(data, created_by)
            
            logger.info(f"PropertyRepository returned type: {type(result)}, value: {result}")
            
            # Check if result is an int (property_id) instead of dict
            if isinstance(result, int):
                logger.warning(f"Repository returned int {result}, fetching full property...")
                return self.property_repo.get_property_by_id(result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error creating property: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def update_property(self, property_id: int, data: Dict[str, Any]) -> Optional[Dict]:
        """Update property"""
        try:
            return self.property_repo.update_property(property_id, data)
        except Exception as e:
            logger.error(f"Error updating property {property_id}: {e}")
            raise
    
    def delete_property(self, property_id: int, deleted_by: int) -> bool:
        """Delete property"""
        try:
            return self.property_repo.delete_property(property_id, deleted_by)
        except Exception as e:
            logger.error(f"Error deleting property {property_id}: {e}")
            return False
    
    def assign_to_agent(self, property_id: int, agent_id: int) -> Optional[Dict]:
        """Assign property to agent"""
        try:
            return self.property_repo.assign_to_agent(property_id, agent_id)
        except Exception as e:
            logger.error(f"Error assigning property {property_id} to agent {agent_id}: {e}")
            raise
    
    def get_properties_by_agent(self, agent_id: int) -> List[Dict]:
        """Get properties assigned to agent"""
        try:
            return self.property_repo.get_properties_by_agent(agent_id)
        except Exception as e:
            logger.error(f"Error fetching properties for agent {agent_id}: {e}")
            return []
    
    def get_dashboard_stats(self) -> Dict[str, Any]:
        """Get dashboard statistics"""
        try:
            all_properties = self.property_repo.get_all_properties()
            
            total = len(all_properties)
            available = sum(1 for p in all_properties if p.get('status_name', '').lower() == 'available')
            occupied = sum(1 for p in all_properties if p.get('status_name', '').lower() == 'occupied')
            maintenance = sum(1 for p in all_properties if p.get('status_name', '').lower() == 'under maintenance')
            
            # Get unique cities
            cities = set(p.get('city') for p in all_properties if p.get('city'))
            
            return {
                'total_properties': total,
                'available': available,
                'occupied': occupied,
                'under_maintenance': maintenance,
                'total_cities': len(cities),
                'occupancy_rate': round((occupied / total * 100), 2) if total > 0 else 0
            }
        except Exception as e:
            logger.error(f"Error in get_dashboard_stats: {e}")
            return {}
    
    def get_all_statuses(self) -> List[Dict]:
        """Get all property statuses"""
        try:
            return self.status_repo.get_all_statuses()
        except Exception as e:
            logger.error(f"Error fetching statuses: {e}")
            return []