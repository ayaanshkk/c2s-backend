# -*- coding: utf-8 -*-
"""
Property Repository
Handles database operations for Property_Master table
"""
import os
import logging
from typing import Optional, Dict, Any, List
from backend.properties.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)


def _supabase_configured() -> bool:
    """True if Supabase env vars are set"""
    if not os.getenv("SUPABASE_URL") or not os.getenv("SUPABASE_SERVICE_ROLE_KEY"):
        return False
    if os.getenv("SUPABASE_DB_URL"):
        return True
    if os.getenv("DATABASE_URL") and "supabase" in (os.getenv("DATABASE_URL") or ""):
        return True
    if os.getenv("SUPABASE_DB_PASSWORD"):
        return True
    return False


class _LocalDBStub:
    """Stub DB adapter when Supabase is not configured"""
    def execute_query(self, query: str, params: tuple = None, fetch_one: bool = False):
        return None if fetch_one else []
    
    def execute_insert(self, query: str, params: tuple = None, returning: bool = True):
        return None
    
    def execute_update(self, query: str, params: tuple = None, returning: bool = False):
        return 0
    
    def execute_delete(self, query: str, params: tuple = None):
        return 0


class PropertyRepository:
    """
    Repository for Property_Master table
    All queries are tenant-filtered for multi-tenant isolation
    """
    
    # Default status_id for new properties (100 = 'Available')
    DEFAULT_STATUS_ID = 100
    
    def __init__(self):
        if _supabase_configured():
            self.db = get_supabase_client()
        else:
            self.db = _LocalDBStub()
    
    def get_all_properties(
        self, 
        tenant_id: int, 
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all properties for a tenant with optional filters
        
        Args:
            tenant_id: Tenant identifier (property management company)
            filters: Optional filters (city, status_id, assigned_agent_id, etc.)
        
        Returns:
            List of property records with agent details
        """
        query = """
            SELECT 
                p.*,
                s."stage_name" as "status_name",
                s."stage_description" as "status_description",
                em."employee_name" as "agent_name",
                em."email" as "agent_email",
                em."phone" as "agent_phone",
                c."country_name",
                curr."currency_code"
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Stage_Master" s 
                ON p."status_id" = s."stage_id"
            LEFT JOIN "StreemLyne_MT"."Employee_Master" em 
                ON p."assigned_agent_id" = em."employee_id"
            LEFT JOIN "StreemLyne_MT"."Country_Master" c 
                ON p."country_id" = c."country_id"
            LEFT JOIN "StreemLyne_MT"."Currency_Master" curr 
                ON p."currency_id" = curr."currency_id"
            WHERE p."tenant_id" = %s
            AND p."is_deleted" = FALSE
        """
        
        params = [tenant_id]
        
        # Apply filters
        if filters:
            if filters.get('city'):
                query += ' AND LOWER(p."city") = LOWER(%s)'
                params.append(filters['city'])
            
            if filters.get('postcode'):
                query += ' AND p."postcode" = %s'
                params.append(filters['postcode'])
            
            # ✅ UPDATED: Use status_id instead of property_status
            if filters.get('status_id'):
                query += ' AND p."status_id" = %s'
                params.append(filters['status_id'])
            
            if filters.get('assigned_agent_id'):
                query += ' AND p."assigned_agent_id" = %s'
                params.append(filters['assigned_agent_id'])
            
            if filters.get('property_type'):
                query += ' AND p."property_type" = %s'
                params.append(filters['property_type'])
        
        query += ' ORDER BY p."created_at" DESC'
        
        try:
            return self.db.execute_query(query, tuple(params))
        except Exception as e:
            logger.error(f"Error fetching properties for tenant {tenant_id}: {e}")
            return []
    
    def get_property_by_id(
        self, 
        tenant_id: int, 
        property_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get a specific property by ID with tenant isolation
        
        Args:
            tenant_id: Tenant identifier
            property_id: Property identifier
        
        Returns:
            Property record with full details or None
        """
        query = """
            SELECT 
                p.*,
                s."stage_name" as "status_name",
                s."stage_description" as "status_description",
                em."employee_name" as "agent_name",
                em."email" as "agent_email",
                em."phone" as "agent_phone",
                em."employee_designation_id",
                dm."designation_description" as "agent_designation",
                c."country_name",
                curr."currency_code",
                curr."currency_name"
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Stage_Master" s 
                ON p."status_id" = s."stage_id"
            LEFT JOIN "StreemLyne_MT"."Employee_Master" em 
                ON p."assigned_agent_id" = em."employee_id"
            LEFT JOIN "StreemLyne_MT"."Designation_Master" dm 
                ON em."employee_designation_id" = dm."designation_id"
            LEFT JOIN "StreemLyne_MT"."Country_Master" c 
                ON p."country_id" = c."country_id"
            LEFT JOIN "StreemLyne_MT"."Currency_Master" curr 
                ON p."currency_id" = curr."currency_id"
            WHERE p."tenant_id" = %s 
            AND p."property_id" = %s
            AND p."is_deleted" = FALSE
            LIMIT 1
        """
        
        try:
            return self.db.execute_query(
                query, 
                (tenant_id, property_id), 
                fetch_one=True
            )
        except Exception as e:
            logger.error(f"Error fetching property {property_id}: {e}")
            return None
    
    def create_property(
        self, 
        tenant_id: int, 
        property_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Create a new property
        
        Args:
            tenant_id: Tenant identifier
            property_data: Property information (name, address, city, etc.)
        
        Returns:
            Created property record or None
        """
        query = """
            INSERT INTO "StreemLyne_MT"."Property_Master" (
                "tenant_id",
                "property_name",
                "address",
                "city",
                "postcode",
                "country_id",
                "property_type",
                "bedrooms",
                "bathrooms",
                "square_feet",
                "status_id",
                "assigned_agent_id",
                "monthly_rent",
                "purchase_price",
                "currency_id",
                "main_photo_url",
                "main_photo_blob_id",
                "is_active",
                "created_at"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            RETURNING *
        """
        
        try:
            return self.db.execute_insert(
                query,
                (
                    tenant_id,
                    property_data.get('property_name'),
                    property_data.get('address'),
                    property_data.get('city'),
                    property_data.get('postcode'),
                    property_data.get('country_id'),
                    property_data.get('property_type'),
                    property_data.get('bedrooms'),
                    property_data.get('bathrooms'),
                    property_data.get('square_feet'),
                    property_data.get('status_id', self.DEFAULT_STATUS_ID),  # ✅ UPDATED
                    property_data.get('assigned_agent_id'),
                    property_data.get('monthly_rent'),
                    property_data.get('purchase_price'),
                    property_data.get('currency_id'),
                    property_data.get('main_photo_url'),
                    property_data.get('main_photo_blob_id'),
                    property_data.get('is_active', True),
                ),
                returning=True
            )
        except Exception as e:
            logger.error(f"Error creating property: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def update_property(
        self, 
        tenant_id: int, 
        property_id: int, 
        property_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Update an existing property
        
        Args:
            tenant_id: Tenant identifier
            property_id: Property identifier
            property_data: Fields to update
        
        Returns:
            Updated property record or None
        """
        # Allowed update fields
        allowed_fields = [
            'property_name', 'address', 'city', 'postcode', 'country_id',
            'property_type', 'bedrooms', 'bathrooms', 'square_feet',
            'status_id',  # ✅ UPDATED: Use status_id instead of property_status
            'assigned_agent_id', 'monthly_rent',
            'purchase_price', 'currency_id', 'main_photo_url', 'main_photo_blob_id',
            'is_active'
        ]
        
        # Build dynamic UPDATE query
        set_clauses = []
        params = []
        
        for field, value in property_data.items():
            if field in allowed_fields:
                set_clauses.append(f'"{field}" = %s')
                params.append(value)
        
        if not set_clauses:
            # No valid fields to update
            return self.get_property_by_id(tenant_id, property_id)
        
        # Add updated_at
        set_clauses.append('"updated_at" = NOW()')
        
        # Add WHERE clause params
        params.extend([tenant_id, property_id])
        
        query = f"""
            UPDATE "StreemLyne_MT"."Property_Master"
            SET {', '.join(set_clauses)}
            WHERE "tenant_id" = %s 
            AND "property_id" = %s
            AND "is_deleted" = FALSE
            RETURNING *
        """
        
        try:
            result = self.db.execute_update(query, tuple(params), returning=True)
            if result:
                logger.info(f'Updated property {property_id} for tenant {tenant_id}')
                return result
            return None
        except Exception as e:
            logger.error(f"Error updating property {property_id}: {e}")
            return None
    
    def delete_property(
        self, 
        tenant_id: int, 
        property_id: int,
        soft_delete: bool = True
    ) -> bool:
        """
        Delete a property (soft delete by default)
        
        Args:
            tenant_id: Tenant identifier
            property_id: Property identifier
            soft_delete: If True, mark as deleted; if False, permanently delete
        
        Returns:
            True if deleted successfully
        """
        try:
            if soft_delete:
                # Soft delete - mark as deleted
                query = """
                    UPDATE "StreemLyne_MT"."Property_Master"
                    SET "is_deleted" = TRUE,
                        "deleted_at" = NOW()
                    WHERE "tenant_id" = %s 
                    AND "property_id" = %s
                """
                rows_affected = self.db.execute_update(query, (tenant_id, property_id))
            else:
                # Hard delete - permanently remove
                query = """
                    DELETE FROM "StreemLyne_MT"."Property_Master"
                    WHERE "tenant_id" = %s 
                    AND "property_id" = %s
                """
                rows_affected = self.db.execute_delete(query, (tenant_id, property_id))
            
            return rows_affected > 0
        except Exception as e:
            logger.error(f"Error deleting property {property_id}: {e}")
            return False
    
    def assign_property_to_agent(
        self, 
        tenant_id: int, 
        property_id: int, 
        agent_id: Optional[int]
    ) -> Optional[Dict[str, Any]]:
        """
        Assign property to a real estate agent (or unassign if agent_id is None)
        
        Args:
            tenant_id: Tenant identifier
            property_id: Property identifier
            agent_id: Employee ID of agent (or None to unassign)
        
        Returns:
            Updated property record or None
        """
        # Verify agent belongs to tenant if assigning
        if agent_id:
            agent_check = self.db.execute_query(
                'SELECT 1 FROM "StreemLyne_MT"."Employee_Master" WHERE "employee_id" = %s AND "tenant_id" = %s',
                (agent_id, tenant_id),
                fetch_one=True
            )
            if not agent_check:
                logger.warning(f'Agent {agent_id} not found for tenant {tenant_id}')
                return None
        
        query = """
            UPDATE "StreemLyne_MT"."Property_Master"
            SET "assigned_agent_id" = %s,
                "updated_at" = NOW()
            WHERE "tenant_id" = %s 
            AND "property_id" = %s
            RETURNING *
        """
        
        try:
            return self.db.execute_update(
                query, 
                (agent_id, tenant_id, property_id), 
                returning=True
            )
        except Exception as e:
            logger.error(f"Error assigning property {property_id} to agent {agent_id}: {e}")
            return None
    
    def get_property_stats(self, tenant_id: int) -> Dict[str, Any]:
        """
        Get property statistics for dashboard
        
        Args:
            tenant_id: Tenant identifier
        
        Returns:
            Dictionary with property statistics
        """
        # ✅ UPDATED: Query Stage_Master for status names
        query = """
            SELECT 
                COUNT(*) as total_properties,
                COUNT(CASE WHEN s."stage_name" = 'Available' THEN 1 END) as available,
                COUNT(CASE WHEN s."stage_name" = 'Occupied' THEN 1 END) as occupied,
                COUNT(CASE WHEN s."stage_name" = 'Under Maintenance' THEN 1 END) as maintenance,
                COUNT(CASE WHEN p."assigned_agent_id" IS NOT NULL THEN 1 END) as assigned_count,
                SUM(CASE WHEN s."stage_name" = 'Occupied' AND p."monthly_rent" IS NOT NULL 
                    THEN p."monthly_rent" ELSE 0 END) as total_monthly_income
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Stage_Master" s 
                ON p."status_id" = s."stage_id"
            WHERE p."tenant_id" = %s
            AND p."is_deleted" = FALSE
        """
        
        try:
            result = self.db.execute_query(query, (tenant_id,), fetch_one=True)
            return result or {
                'total_properties': 0,
                'available': 0,
                'occupied': 0,
                'maintenance': 0,
                'assigned_count': 0,
                'total_monthly_income': 0
            }
        except Exception as e:
            logger.error(f"Error fetching property stats: {e}")
            return {
                'total_properties': 0,
                'available': 0,
                'occupied': 0,
                'maintenance': 0,
                'assigned_count': 0,
                'total_monthly_income': 0
            }
    
    def get_properties_by_agent(
        self, 
        tenant_id: int, 
        agent_id: int
    ) -> List[Dict[str, Any]]:
        """
        Get all properties assigned to a specific agent
        
        Args:
            tenant_id: Tenant identifier
            agent_id: Employee ID of agent
        
        Returns:
            List of properties assigned to this agent
        """
        query = """
            SELECT 
                p.*,
                s."stage_name" as "status_name",
                c."country_name",
                curr."currency_code"
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Stage_Master" s 
                ON p."status_id" = s."stage_id"
            LEFT JOIN "StreemLyne_MT"."Country_Master" c 
                ON p."country_id" = c."country_id"
            LEFT JOIN "StreemLyne_MT"."Currency_Master" curr 
                ON p."currency_id" = curr."currency_id"
            WHERE p."tenant_id" = %s 
            AND p."assigned_agent_id" = %s
            AND p."is_deleted" = FALSE
            ORDER BY p."created_at" DESC
        """
        
        try:
            return self.db.execute_query(query, (tenant_id, agent_id))
        except Exception as e:
            logger.error(f"Error fetching properties for agent {agent_id}: {e}")
            return []