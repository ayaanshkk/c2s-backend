# -*- coding: utf-8 -*-
"""
Property Service
Business logic layer for property management operations
"""
import os
import logging
from typing import Optional, Dict, Any, List
from backend.properties.repositories.property_repository import PropertyRepository
from backend.properties.repositories.stage_repository import PropertyStatusRepository
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
    
    def get_properties(
        self, 
        tenant_id: int, 
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Get all properties for a tenant with optional filters
        
        Args:
            tenant_id: Tenant identifier
            filters: Optional filters (city, status_id, agent_id, property_type)
        
        Returns:
            Response with properties list and metadata
        """
        try:
            properties = self.property_repo.get_all_properties(tenant_id, filters)
            
            return {
                'success': True,
                'properties': properties,
                'total_count': len(properties),
                'filters_applied': filters or {}
            }
        except Exception as e:
            logger.error(f"Error fetching properties: {e}")
            return {
                'success': False,
                'error': str(e),
                'properties': []
            }
    
    def get_property_detail(
        self, 
        tenant_id: int, 
        property_id: int
    ) -> Dict[str, Any]:
        """
        Get detailed information for a specific property
        
        Args:
            tenant_id: Tenant identifier
            property_id: Property identifier
        
        Returns:
            Property details or error
        """
        try:
            property_data = self.property_repo.get_property_by_id(tenant_id, property_id)
            
            if not property_data:
                return {
                    'success': False,
                    'error': 'Property not found or access denied',
                    'property': None
                }
            
            return {
                'success': True,
                'property': property_data
            }
        except Exception as e:
            logger.error(f"Error fetching property {property_id}: {e}")
            return {
                'success': False,
                'error': str(e),
                'property': None
            }
    
    def create_property(
        self, 
        tenant_id: int, 
        property_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Create a new property with validation
        
        Args:
            tenant_id: Tenant identifier
            property_data: Property information
        
        Returns:
            Created property or error
        """
        # Validate required fields
        required_fields = ['property_name', 'address', 'city']
        missing_fields = [field for field in required_fields if not property_data.get(field)]
        
        if missing_fields:
            return {
                'success': False,
                'error': f'Missing required fields: {", ".join(missing_fields)}',
                'property': None
            }
        
        # Validate status_id if provided
        if property_data.get('status_id'):
            status = self.status_repo.get_status_by_id(property_data['status_id'])
            if not status:
                return {
                    'success': False,
                    'error': 'Invalid status_id provided',
                    'property': None
                }
        
        # Validate assigned_agent_id if provided
        if property_data.get('assigned_agent_id'):
            agent = self.user_repo.get_user_by_id(tenant_id, property_data['assigned_agent_id'])
            if not agent:
                return {
                    'success': False,
                    'error': 'Invalid agent_id - agent not found',
                    'property': None
                }
        
        try:
            new_property = self.property_repo.create_property(tenant_id, property_data)
            
            if not new_property:
                return {
                    'success': False,
                    'error': 'Failed to create property',
                    'property': None
                }
            
            logger.info(f'Created property {new_property.get("property_id")} for tenant {tenant_id}')
            
            return {
                'success': True,
                'property': new_property,
                'message': 'Property created successfully'
            }
        except Exception as e:
            logger.error(f"Error creating property: {e}")
            return {
                'success': False,
                'error': str(e),
                'property': None
            }
    
    def update_property(
        self, 
        tenant_id: int, 
        property_id: int, 
        property_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update an existing property with validation
        
        Args:
            tenant_id: Tenant identifier
            property_id: Property identifier
            property_data: Fields to update
        
        Returns:
            Updated property or error
        """
        # Check if property exists
        existing_property = self.property_repo.get_property_by_id(tenant_id, property_id)
        if not existing_property:
            return {
                'success': False,
                'error': 'Property not found or access denied',
                'property': None
            }
        
        # Validate status_id if being updated
        if property_data.get('status_id'):
            status = self.status_repo.get_status_by_id(property_data['status_id'])
            if not status:
                return {
                    'success': False,
                    'error': 'Invalid status_id provided',
                    'property': None
                }
        
        # Validate assigned_agent_id if being updated
        if property_data.get('assigned_agent_id'):
            agent = self.user_repo.get_user_by_id(tenant_id, property_data['assigned_agent_id'])
            if not agent:
                return {
                    'success': False,
                    'error': 'Invalid agent_id - agent not found',
                    'property': None
                }
        
        try:
            updated_property = self.property_repo.update_property(
                tenant_id, 
                property_id, 
                property_data
            )
            
            if not updated_property:
                return {
                    'success': False,
                    'error': 'Failed to update property',
                    'property': None
                }
            
            logger.info(f'Updated property {property_id} for tenant {tenant_id}')
            
            return {
                'success': True,
                'property': updated_property,
                'message': 'Property updated successfully'
            }
        except Exception as e:
            logger.error(f"Error updating property {property_id}: {e}")
            return {
                'success': False,
                'error': str(e),
                'property': None
            }
    
    def delete_property(
        self, 
        tenant_id: int, 
        property_id: int,
        soft_delete: bool = True
    ) -> Dict[str, Any]:
        """
        Delete a property (soft delete by default)
        
        Args:
            tenant_id: Tenant identifier
            property_id: Property identifier
            soft_delete: If True, mark as deleted; if False, permanently delete
        
        Returns:
            Success status and message
        """
        # Check if property exists
        existing_property = self.property_repo.get_property_by_id(tenant_id, property_id)
        if not existing_property:
            return {
                'success': False,
                'error': 'Property not found or access denied'
            }
        
        try:
            success = self.property_repo.delete_property(
                tenant_id, 
                property_id, 
                soft_delete
            )
            
            if not success:
                return {
                    'success': False,
                    'error': 'Failed to delete property'
                }
            
            delete_type = 'soft deleted' if soft_delete else 'permanently deleted'
            logger.info(f'Property {property_id} {delete_type} for tenant {tenant_id}')
            
            return {
                'success': True,
                'message': f'Property {delete_type} successfully'
            }
        except Exception as e:
            logger.error(f"Error deleting property {property_id}: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def assign_property_to_agent(
        self, 
        tenant_id: int, 
        property_id: int, 
        agent_id: Optional[int]
    ) -> Dict[str, Any]:
        """
        Assign property to a real estate agent
        
        Args:
            tenant_id: Tenant identifier
            property_id: Property identifier
            agent_id: Employee ID of agent (or None to unassign)
        
        Returns:
            Updated property or error
        """
        # Validate agent exists if assigning
        if agent_id:
            agent = self.user_repo.get_user_by_id(tenant_id, agent_id)
            if not agent:
                return {
                    'success': False,
                    'error': 'Agent not found or does not belong to this tenant',
                    'property': None
                }
        
        try:
            updated_property = self.property_repo.assign_property_to_agent(
                tenant_id, 
                property_id, 
                agent_id
            )
            
            if not updated_property:
                return {
                    'success': False,
                    'error': 'Failed to assign property - property may not exist',
                    'property': None
                }
            
            action = 'assigned' if agent_id else 'unassigned'
            logger.info(f'Property {property_id} {action} for tenant {tenant_id}')
            
            return {
                'success': True,
                'property': updated_property,
                'message': f'Property {action} successfully'
            }
        except Exception as e:
            logger.error(f"Error assigning property {property_id}: {e}")
            return {
                'success': False,
                'error': str(e),
                'property': None
            }
    
    def get_dashboard_stats(self, tenant_id: int) -> Dict[str, Any]:
        """
        Get property statistics for dashboard
        
        Args:
            tenant_id: Tenant identifier
        
        Returns:
            Dashboard statistics
        """
        try:
            stats = self.property_repo.get_property_stats(tenant_id)
            
            return {
                'success': True,
                'stats': stats
            }
        except Exception as e:
            logger.error(f"Error fetching dashboard stats: {e}")
            return {
                'success': False,
                'error': str(e),
                'stats': {}
            }
    
    def get_agent_properties(
        self, 
        tenant_id: int, 
        agent_id: int
    ) -> Dict[str, Any]:
        """
        Get all properties assigned to a specific agent
        
        Args:
            tenant_id: Tenant identifier
            agent_id: Employee ID of agent
        
        Returns:
            Agent's properties list
        """
        try:
            properties = self.property_repo.get_properties_by_agent(tenant_id, agent_id)
            
            return {
                'success': True,
                'properties': properties,
                'total_count': len(properties),
                'agent_id': agent_id
            }
        except Exception as e:
            logger.error(f"Error fetching agent properties: {e}")
            return {
                'success': False,
                'error': str(e),
                'properties': []
            }
    
    def get_all_statuses(self) -> Dict[str, Any]:
        """
        Get all available property statuses
        
        Returns:
            List of property statuses
        """
        try:
            statuses = self.status_repo.get_all_statuses()
            
            return {
                'success': True,
                'statuses': statuses
            }
        except Exception as e:
            logger.error(f"Error fetching property statuses: {e}")
            return {
                'success': False,
                'error': str(e),
                'statuses': []
            }