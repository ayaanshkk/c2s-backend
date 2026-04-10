# -*- coding: utf-8 -*-
"""
Property Repository
Matches EXACT schema from Property_Master table
"""
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from backend.properties.supabase_client import supabase

logger = logging.getLogger(__name__)


class PropertyRepository:
    """Repository for Property_Master table"""

    def __init__(self):
        self.schema = "StreemLyne_MT"
        self.supabase = supabase
        self.logger = logging.getLogger(__name__)

    def get_all_properties(
        self, tenant_id: str, filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict]:
        """List properties for one tenant with optional filters."""
        try:
            query = f'''
                SELECT 
                    p.*,
                    em.employee_name as assigned_agent_name,
                    sm.stage_name as status_name
                FROM "{self.schema}"."Property_Master" p
                LEFT JOIN "{self.schema}"."Employee_Master" em 
                    ON p.assigned_agent_id = em.employee_id
                LEFT JOIN "{self.schema}"."Stage_Master" sm 
                    ON p.status_id = sm.stage_id
                WHERE p.is_deleted = FALSE
                  AND p.tenant_id = %s
            '''

            params: List[Any] = [tenant_id]

            if filters:
                if filters.get("city"):
                    query += " AND p.city = %s"
                    params.append(filters["city"])
                if filters.get("status_id") is not None:
                    query += " AND p.status_id = %s"
                    params.append(filters["status_id"])
                if filters.get("agent_id") is not None:
                    query += " AND p.assigned_agent_id = %s"
                    params.append(filters["agent_id"])
                if filters.get("property_type"):
                    query += " AND p.property_type = %s"
                    params.append(filters["property_type"])

            query += " ORDER BY p.created_at DESC"

            result = self.supabase.execute_query(query, tuple(params))
            return result if result else []

        except Exception as e:
            self.logger.error(f"Error fetching properties: {str(e)}")
            return []

    def get_property_by_id(
        self, property_id: int, tenant_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get a single property scoped to tenant."""
        try:
            query = f'''
                SELECT 
                    p.*,
                    em.employee_name as assigned_agent_name,
                    sm.stage_name as status_name
                FROM "{self.schema}"."Property_Master" p
                LEFT JOIN "{self.schema}"."Employee_Master" em 
                    ON p.assigned_agent_id = em.employee_id
                LEFT JOIN "{self.schema}"."Stage_Master" sm 
                    ON p.status_id = sm.stage_id
                WHERE p.property_id = %s
                  AND p.tenant_id = %s
                  AND p.is_deleted = FALSE
            '''

            return self.supabase.execute_query(
                query, (property_id, tenant_id), fetch_one=True
            )

        except Exception as e:
            self.logger.error(f"Error fetching property {property_id}: {str(e)}")
            return None

    def create_property(
        self,
        data: Dict[str, Any],
        created_by: int,
        tenant_id: str,
        status_id: Optional[int],
    ):
        """Insert property using EXACT column order from schema."""
        try:
            # ✅ Ensure occupancy_status has a default value
            occupancy_status = data.get('occupancy_status', 'Vacant')
            if not occupancy_status or str(occupancy_status).strip() == '':
                occupancy_status = 'Vacant'
            
            # ✅ INSERT with columns that exist in the EXACT schema
            query = f'''
                INSERT INTO "{self.schema}"."Property_Master" (
                    tenant_id,
                    property_name,
                    address,
                    city,
                    postcode,
                    country_id,
                    property_type,
                    occupancy_status,
                    bedrooms,
                    bathrooms,
                    square_feet,
                    assigned_agent_id,
                    monthly_rent,
                    purchase_price,
                    currency_id,
                    main_photo_url,
                    is_active,
                    is_deleted,
                    status_id,
                    created_by,
                    state,
                    country,
                    deposit_amount,
                    year_built,
                    lease_start_date,
                    lease_end_date,
                    tenant_name,
                    tenant_contact,
                    tenant_email,
                    description,
                    amenities,
                    parking_spaces,
                    pet_friendly,
                    furnished,
                    document_details,
                    rent_due_day
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                RETURNING property_id
            '''

            params = (
                tenant_id,                             
                data.get("property_name"),              
                data.get("address"),                    
                data.get("city", ""),                   
                data.get("postcode", ""),               
                data.get("country_id", 1),              
                data.get("property_type", ""),          
                occupancy_status,                      
                data.get("bedrooms", 0),                
                data.get("bathrooms", 0),               
                data.get("square_feet", 0),             
                data.get("assigned_agent_id"),         
                data.get("monthly_rent", 0),            
                data.get("purchase_price", 0),          
                data.get("currency_id", 1),           
                data.get("main_photo_url"),             
                True,                                   
                False,                                  
                status_id,                              
                created_by,                             
                data.get("state", ""),                  
                data.get("country", "UK"),              
                data.get("deposit_amount", 0),          
                data.get("year_built"),                 
                data.get("lease_start_date"),           
                data.get("lease_end_date"),             
                data.get("tenant_name"),                
                data.get("tenant_contact"),             
                data.get("tenant_email"),               
                data.get("description"),                
                data.get("amenities"),                  
                data.get("parking_spaces", 0),          
                data.get("pet_friendly", False),        
                data.get("furnished", False),           
                data.get("document_details"),           
                data.get("rent_due_day"),               
            )

            self.logger.info(f"🔧 Creating property: {data.get('property_name')}")
            self.logger.info(f"📊 Number of columns: 36, Number of params: {len(params)}")
            
            result = self.supabase.execute_insert(query, params, returning=True)
            
            self.logger.info(f"📊 Insert result type: {type(result)}")
            self.logger.info(f"📊 Insert result value: {result}")

            # Handle different return types from execute_insert
            property_id = None
            
            if isinstance(result, list):
                if len(result) > 0 and isinstance(result[0], dict):
                    property_id = result[0].get("property_id")
                    self.logger.info(f"✅ Extracted property_id from list: {property_id}")
            elif isinstance(result, dict):
                property_id = result.get("property_id")
                self.logger.info(f"✅ Extracted property_id from dict: {property_id}")

            if property_id:
                self.logger.info(f"✅ Property created with ID: {property_id}")
                return self.get_property_by_id(property_id, tenant_id)
            else:
                self.logger.error(f"❌ Could not extract property_id from result: {result}")
                return None

        except Exception as e:
            self.logger.error(f"Error creating property: {str(e)}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    def update_property(
        self, property_id: int, tenant_id: str, data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Update property within tenant."""
        try:
            now = datetime.utcnow().isoformat()

            update_fields = []
            params: List[Any] = []

            # ✅ Fields that exist in actual schema (INCLUDING occupancy_status and rent_due_day)
            field_mapping = {
                "property_name": "property_name",
                "property_type": "property_type",
                "occupancy_status": "occupancy_status",  
                "address": "address",
                "city": "city",
                "state": "state",
                "postcode": "postcode",
                "country": "country",
                "country_id": "country_id",
                "assigned_agent_id": "assigned_agent_id",
                "monthly_rent": "monthly_rent",
                "purchase_price": "purchase_price",
                "deposit_amount": "deposit_amount",
                "rent_due_day": "rent_due_day",  
                "currency_id": "currency_id",
                "bedrooms": "bedrooms",
                "bathrooms": "bathrooms",
                "square_feet": "square_feet",
                "year_built": "year_built",
                "property_status": "property_status",
                "status_id": "status_id",
                "main_photo_url": "main_photo_url",
                "lease_start_date": "lease_start_date",
                "lease_end_date": "lease_end_date",
                "tenant_name": "tenant_name",
                "tenant_contact": "tenant_contact",
                "tenant_email": "tenant_email",
                "description": "description",
                "amenities": "amenities",
                "parking_spaces": "parking_spaces",
                "pet_friendly": "pet_friendly",
                "furnished": "furnished",
                "document_details": "document_details",
                "is_active": "is_active",
                "photo_urls": "photo_urls",  
            }

            # ✅ Special handling for occupancy_status to ensure it's never empty
            if "occupancy_status" in data:
                occupancy_status = data["occupancy_status"]
                if not occupancy_status or str(occupancy_status).strip() == '':
                    data["occupancy_status"] = "Vacant"

            for key, db_field in field_mapping.items():
                if key in data:
                    update_fields.append(f'"{db_field}" = %s')
                    params.append(data[key])

            if not update_fields:
                return self.get_property_by_id(property_id, tenant_id)

            update_fields.append('"updated_at" = %s')
            params.append(now)
            params.extend([property_id, tenant_id])

            query = f'''
                UPDATE "{self.schema}"."Property_Master"
                SET {', '.join(update_fields)}
                WHERE property_id = %s
                AND tenant_id = %s
                AND is_deleted = FALSE
            '''

            self.supabase.execute_update(query, tuple(params))
            return self.get_property_by_id(property_id, tenant_id)

        except Exception as e:
            self.logger.error(f"Error updating property {property_id}: {str(e)}")
            raise

    def delete_property(
        self, property_id: int, deleted_by: int, tenant_id: str
    ) -> bool:
        """Soft-delete property within tenant."""
        try:
            now = datetime.utcnow().isoformat()

            query = f'''
                UPDATE "{self.schema}"."Property_Master"
                SET is_deleted = TRUE,
                    deleted_at = %s,
                    deleted_by = %s,
                    updated_at = %s
                WHERE property_id = %s
                  AND tenant_id = %s
            '''

            rows = self.supabase.execute_update(
                query, (now, deleted_by, now, property_id, tenant_id)
            )
            return rows > 0

        except Exception as e:
            self.logger.error(f"Error deleting property {property_id}: {str(e)}")
            return False

    def assign_to_agent(
        self, property_id: int, agent_id: int, tenant_id: str
    ) -> Optional[Dict[str, Any]]:
        """Assign property to agent (same tenant)."""
        try:
            now = datetime.utcnow().isoformat()

            query = f'''
                UPDATE "{self.schema}"."Property_Master"
                SET assigned_agent_id = %s,
                    updated_at = %s
                WHERE property_id = %s
                  AND tenant_id = %s
                  AND is_deleted = FALSE
            '''

            self.supabase.execute_update(query, (agent_id, now, property_id, tenant_id))
            return self.get_property_by_id(property_id, tenant_id)

        except Exception as e:
            self.logger.error(f"Error assigning property {property_id}: {str(e)}")
            raise

    def get_properties_by_agent(
        self, agent_id: int, tenant_id: str
    ) -> List[Dict[str, Any]]:
        """Properties for an agent within one tenant."""
        try:
            query = f'''
                SELECT 
                    p.*,
                    em.employee_name as assigned_agent_name,
                    sm.stage_name as status_name
                FROM "{self.schema}"."Property_Master" p
                LEFT JOIN "{self.schema}"."Employee_Master" em 
                    ON p.assigned_agent_id = em.employee_id
                LEFT JOIN "{self.schema}"."Stage_Master" sm 
                    ON p.status_id = sm.stage_id
                WHERE p.assigned_agent_id = %s
                  AND p.tenant_id = %s
                  AND p.is_deleted = FALSE
                ORDER BY p.created_at DESC
            '''

            result = self.supabase.execute_query(query, (agent_id, tenant_id))
            return result if result else []

        except Exception as e:
            self.logger.error(
                f"Error fetching properties for agent {agent_id}: {str(e)}"
            )
            return []