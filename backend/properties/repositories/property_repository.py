# -*- coding: utf-8 -*-
"""
Property Repository
Handles database operations for Property_Master table
"""
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from backend.properties.supabase_client import supabase

logger = logging.getLogger(__name__)


class PropertyRepository:
    """Repository for Property_Master table"""
    
    def __init__(self):
        """Initialize repository with schema and clients"""
        self.schema = "StreemLyne_MT"
        self.supabase = supabase
        self.logger = logging.getLogger(__name__)
    
    def get_all_properties(self, filters=None):
        """Get all properties with optional filters"""
        try:
            query = f'''
                SELECT 
                    p.*,
                    em.employee_name as assigned_agent_name
                FROM "{self.schema}"."Property_Master" p
                LEFT JOIN "{self.schema}"."Employee_Master" em ON p.assigned_agent_id = em.employee_id
                WHERE p.is_deleted = FALSE
            '''
            
            params = []
            
            if filters:
                if filters.get('city'):
                    query += f" AND p.city = %s"
                    params.append(filters['city'])
                if filters.get('status_id'):
                    query += f" AND p.property_status = %s"
                    params.append(filters['status_id'])
                if filters.get('agent_id'):
                    query += f" AND p.assigned_agent_id = %s"
                    params.append(filters['agent_id'])
                if filters.get('property_type'):
                    query += f" AND p.property_type = %s"
                    params.append(filters['property_type'])
            
            query += " ORDER BY p.created_at DESC"
            
            result = self.supabase.execute_query(query, tuple(params) if params else None)
            
            if result:
                return result
            return []
            
        except Exception as e:
            self.logger.error(f"Error fetching properties: {str(e)}")
            return []
    
    def get_property_by_id(self, property_id: int) -> Optional[Dict[str, Any]]:
        """Get property by ID"""
        try:
            query = f'''
                SELECT 
                    p.*,
                    em.employee_name as assigned_agent_name
                FROM "{self.schema}"."Property_Master" p
                LEFT JOIN "{self.schema}"."Employee_Master" em ON p.assigned_agent_id = em.employee_id
                WHERE p.property_id = %s
                AND p.is_deleted = FALSE
            '''
            
            result = self.supabase.execute_query(query, (property_id,), fetch_one=True)
            return result
            
        except Exception as e:
            self.logger.error(f"Error fetching property {property_id}: {str(e)}")
            return None
    
    def create_property(self, data, created_by):
        """Create new property"""
        try:
            now = datetime.utcnow().isoformat()
            
            query = f'''
                INSERT INTO "{self.schema}"."Property_Master" (
                    tenant_id, property_name, property_type, address, city,
                    postcode, country_id, assigned_agent_id, monthly_rent,
                    purchase_price, currency_id, bedrooms, bathrooms, square_feet,
                    property_status, main_photo_url, is_active, is_deleted,
                    created_at, updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING property_id
            '''
            
            params = (
                data.get('tenant_id', 1),
                data.get('property_name'),
                data.get('property_type', ''),
                data.get('address'),
                data.get('city', ''),
                data.get('postal_code', ''),  # Maps to postcode
                data.get('country_id', 1),     # Default country_id
                data.get('assigned_agent_id'),
                data.get('monthly_rent', 0),
                data.get('purchase_price', 0),
                data.get('currency_id', 1),    # Default currency_id
                data.get('bedrooms', 0),
                data.get('bathrooms', 0),
                data.get('square_feet', 0),
                data.get('property_status', 'Available'),  # Default status
                data.get('main_photo_url'),    # First photo URL
                True,                          # is_active
                False,                         # is_deleted
                now,
                now
            )
            
            result = self.supabase.execute_insert(query, params, returning=True)
            
            if result and result.get('property_id'):
                property_id = result['property_id']
                return self.get_property_by_id(property_id)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error creating property: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
    
    def update_property(self, property_id: int, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update property"""
        try:
            now = datetime.utcnow().isoformat()
            
            # Build dynamic UPDATE query
            update_fields = []
            params = []
            
            field_mapping = {
                'property_name': 'property_name',
                'property_type': 'property_type',
                'address': 'address',
                'city': 'city',
                'postal_code': 'postcode',
                'country_id': 'country_id',
                'assigned_agent_id': 'assigned_agent_id',
                'monthly_rent': 'monthly_rent',
                'purchase_price': 'purchase_price',
                'currency_id': 'currency_id',
                'bedrooms': 'bedrooms',
                'bathrooms': 'bathrooms',
                'square_feet': 'square_feet',
                'property_status': 'property_status',
                'main_photo_url': 'main_photo_url',
            }
            
            for key, db_field in field_mapping.items():
                if key in data:
                    update_fields.append(f'"{db_field}" = %s')
                    params.append(data[key])
            
            if not update_fields:
                return self.get_property_by_id(property_id)
            
            update_fields.append('"updated_at" = %s')
            params.append(now)
            params.append(property_id)
            
            query = f'''
                UPDATE "{self.schema}"."Property_Master"
                SET {', '.join(update_fields)}
                WHERE property_id = %s
                AND is_deleted = FALSE
            '''
            
            self.supabase.execute_update(query, tuple(params))
            return self.get_property_by_id(property_id)
            
        except Exception as e:
            self.logger.error(f"Error updating property {property_id}: {str(e)}")
            raise
    
    def delete_property(self, property_id: int, deleted_by: int) -> bool:
        """Soft delete property"""
        try:
            now = datetime.utcnow().isoformat()
            
            query = f'''
                UPDATE "{self.schema}"."Property_Master"
                SET is_deleted = TRUE,
                    deleted_at = %s,
                    updated_at = %s
                WHERE property_id = %s
            '''
            
            rows = self.supabase.execute_update(query, (now, now, property_id))
            return rows > 0
            
        except Exception as e:
            self.logger.error(f"Error deleting property {property_id}: {str(e)}")
            return False
    
    def assign_to_agent(self, property_id: int, agent_id: int) -> Optional[Dict[str, Any]]:
        """Assign property to agent"""
        try:
            now = datetime.utcnow().isoformat()
            
            query = f'''
                UPDATE "{self.schema}"."Property_Master"
                SET assigned_agent_id = %s,
                    updated_at = %s
                WHERE property_id = %s
                AND is_deleted = FALSE
            '''
            
            self.supabase.execute_update(query, (agent_id, now, property_id))
            return self.get_property_by_id(property_id)
            
        except Exception as e:
            self.logger.error(f"Error assigning property {property_id}: {str(e)}")
            raise
    
    def get_properties_by_agent(self, agent_id: int) -> List[Dict[str, Any]]:
        """Get all properties assigned to an agent"""
        try:
            query = f'''
                SELECT 
                    p.*,
                    em.employee_name as assigned_agent_name
                FROM "{self.schema}"."Property_Master" p
                LEFT JOIN "{self.schema}"."Employee_Master" em ON p.assigned_agent_id = em.employee_id
                WHERE p.assigned_agent_id = %s
                AND p.is_deleted = FALSE
                ORDER BY p.created_at DESC
            '''
            
            result = self.supabase.execute_query(query, (agent_id,))
            return result if result else []
            
        except Exception as e:
            self.logger.error(f"Error fetching properties for agent {agent_id}: {str(e)}")
            return []