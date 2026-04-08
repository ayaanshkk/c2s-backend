# -*- coding: utf-8 -*-
"""
Tenancy Repository
Handles database operations for property tenancy/rental contracts
"""
from typing import Optional, Dict, Any, List
from backend.properties.supabase_client import get_supabase_client
import logging

logger = logging.getLogger(__name__)


class TenancyRepository:
    """
    Repository for Property_Tenancies table (rental contracts)
    All queries are tenant-filtered for multi-tenant isolation
    """
    
    def __init__(self):
        self.db = get_supabase_client()
    
    def get_all_tenancies(
        self, 
        tenant_id: int, 
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all tenancy contracts for a property management tenant
        
        Args:
            tenant_id: Tenant identifier (property manager)
            filters: Optional filters (status, property_id, etc.)
        
        Returns:
            List of tenancy records
        """
        query = """
            SELECT 
                pt.*,
                p."property_name",
                p."address" as "property_address",
                c."client_company_name" as "tenant_name",
                c."client_email" as "tenant_email",
                c."client_phone" as "tenant_phone"
            FROM "StreemLyne_MT"."Property_Tenancies" pt
            INNER JOIN "StreemLyne_MT"."Property_Master" p 
                ON pt."property_id" = p."property_id"
            LEFT JOIN "StreemLyne_MT"."Client_Master" c 
                ON pt."tenant_client_id" = c."client_id"
            WHERE pt."tenant_id" = %s
        """
        params = [tenant_id]
        
        # Apply filters
        if filters:
            if filters.get('status'):
                query += ' AND pt."tenancy_status" = %s'
                params.append(filters['status'])
            
            if filters.get('property_id'):
                query += ' AND pt."property_id" = %s'
                params.append(filters['property_id'])
            
            if filters.get('is_active'):
                query += ' AND pt."is_active" = %s'
                params.append(filters['is_active'])
        
        query += ' ORDER BY pt."start_date" DESC'
        
        try:
            return self.db.execute_query(query, tuple(params))
        except Exception as e:
            logger.error(f"Error fetching tenancies for tenant {tenant_id}: {e}")
            return []
    
    def get_tenancy_by_id(
        self, 
        tenant_id: int, 
        tenancy_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get a specific tenancy contract by ID
        
        Args:
            tenant_id: Tenant identifier
            tenancy_id: Tenancy contract identifier
        
        Returns:
            Tenancy record or None
        """
        query = """
            SELECT 
                pt.*,
                p."property_name",
                p."address" as "property_address",
                p."city",
                p."postcode",
                c."client_company_name" as "tenant_name",
                c."client_contact_name" as "tenant_contact",
                c."client_email" as "tenant_email",
                c."client_phone" as "tenant_phone"
            FROM "StreemLyne_MT"."Property_Tenancies" pt
            INNER JOIN "StreemLyne_MT"."Property_Master" p 
                ON pt."property_id" = p."property_id"
            LEFT JOIN "StreemLyne_MT"."Client_Master" c 
                ON pt."tenant_client_id" = c."client_id"
            WHERE pt."tenant_id" = %s AND pt."tenancy_id" = %s
            LIMIT 1
        """
        
        try:
            return self.db.execute_query(
                query, 
                (tenant_id, tenancy_id), 
                fetch_one=True
            )
        except Exception as e:
            logger.error(f"Error fetching tenancy {tenancy_id}: {e}")
            return None
    
    def get_tenancies_by_property(
        self, 
        tenant_id: int, 
        property_id: int
    ) -> List[Dict[str, Any]]:
        """
        Get all tenancies for a specific property
        
        Args:
            tenant_id: Tenant identifier
            property_id: Property identifier
        
        Returns:
            List of tenancy records
        """
        query = """
            SELECT 
                pt.*,
                c."client_company_name" as "tenant_name",
                c."client_email" as "tenant_email"
            FROM "StreemLyne_MT"."Property_Tenancies" pt
            LEFT JOIN "StreemLyne_MT"."Client_Master" c 
                ON pt."tenant_client_id" = c."client_id"
            WHERE pt."tenant_id" = %s AND pt."property_id" = %s
            ORDER BY pt."start_date" DESC
        """
        
        try:
            return self.db.execute_query(query, (tenant_id, property_id))
        except Exception as e:
            logger.error(f"Error fetching tenancies for property {property_id}: {e}")
            return []
    
    def get_active_tenancy_for_property(
        self, 
        tenant_id: int, 
        property_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get current active tenancy for a property
        
        Args:
            tenant_id: Tenant identifier
            property_id: Property identifier
        
        Returns:
            Active tenancy record or None
        """
        query = """
            SELECT 
                pt.*,
                c."client_company_name" as "tenant_name",
                c."client_email" as "tenant_email",
                c."client_phone" as "tenant_phone"
            FROM "StreemLyne_MT"."Property_Tenancies" pt
            LEFT JOIN "StreemLyne_MT"."Client_Master" c 
                ON pt."tenant_client_id" = c."client_id"
            WHERE pt."tenant_id" = %s 
            AND pt."property_id" = %s
            AND pt."is_active" = TRUE
            AND pt."end_date" >= CURRENT_DATE
            ORDER BY pt."start_date" DESC
            LIMIT 1
        """
        
        try:
            return self.db.execute_query(
                query, 
                (tenant_id, property_id), 
                fetch_one=True
            )
        except Exception as e:
            logger.error(f"Error fetching active tenancy: {e}")
            return None
    
    def create_tenancy(
        self, 
        tenant_id: int, 
        tenancy_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Create a new tenancy contract
        
        Args:
            tenant_id: Tenant identifier
            tenancy_data: Tenancy contract details
        
        Returns:
            Created tenancy record or None
        """
        query = """
            INSERT INTO "StreemLyne_MT"."Property_Tenancies"
            (
                "tenant_id",
                "property_id",
                "tenant_client_id",
                "start_date",
                "end_date",
                "monthly_rent",
                "deposit_amount",
                "tenancy_status",
                "is_active",
                "created_at"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING *
        """
        
        try:
            return self.db.execute_insert(
                query,
                (
                    tenant_id,
                    tenancy_data.get('property_id'),
                    tenancy_data.get('tenant_client_id'),
                    tenancy_data.get('start_date'),
                    tenancy_data.get('end_date'),
                    tenancy_data.get('monthly_rent'),
                    tenancy_data.get('deposit_amount'),
                    tenancy_data.get('tenancy_status', 'ACTIVE'),
                    tenancy_data.get('is_active', True)
                ),
                returning=True
            )
        except Exception as e:
            logger.error(f"Error creating tenancy: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def update_tenancy(
        self, 
        tenant_id: int, 
        tenancy_id: int, 
        updates: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Update a tenancy contract
        
        Args:
            tenant_id: Tenant identifier
            tenancy_id: Tenancy contract identifier
            updates: Fields to update
        
        Returns:
            Updated tenancy record or None
        """
        allowed_fields = [
            'end_date', 'monthly_rent', 'deposit_amount', 
            'tenancy_status', 'is_active', 'notes'
        ]
        
        # Build dynamic update query
        update_fields = []
        params = []
        
        for field, value in updates.items():
            if field in allowed_fields:
                update_fields.append(f'"{field}" = %s')
                params.append(value)
        
        if not update_fields:
            return None
        
        params.extend([tenant_id, tenancy_id])
        
        query = f"""
            UPDATE "StreemLyne_MT"."Property_Tenancies"
            SET {', '.join(update_fields)}, "updated_at" = CURRENT_TIMESTAMP
            WHERE "tenant_id" = %s AND "tenancy_id" = %s
            RETURNING *
        """
        
        try:
            return self.db.execute_update(query, tuple(params), returning=True)
        except Exception as e:
            logger.error(f"Error updating tenancy {tenancy_id}: {e}")
            return None
    
    def get_tenancy_stats(self, tenant_id: int) -> Dict[str, Any]:
        """
        Get tenancy statistics for dashboard
        
        Args:
            tenant_id: Tenant identifier
        
        Returns:
            Dictionary with tenancy statistics
        """
        query = """
            SELECT 
                COUNT(*) as total_tenancies,
                COUNT(CASE WHEN "is_active" = TRUE THEN 1 END) as active_tenancies,
                COUNT(CASE WHEN "tenancy_status" = 'EXPIRING_SOON' THEN 1 END) as expiring_soon,
                SUM(CASE WHEN "is_active" = TRUE THEN "monthly_rent" ELSE 0 END) as total_monthly_income
            FROM "StreemLyne_MT"."Property_Tenancies"
            WHERE "tenant_id" = %s
        """
        
        try:
            result = self.db.execute_query(query, (tenant_id,), fetch_one=True)
            return result or {
                'total_tenancies': 0,
                'active_tenancies': 0,
                'expiring_soon': 0,
                'total_monthly_income': 0
            }
        except Exception as e:
            logger.error(f"Error fetching tenancy stats: {e}")
            return {
                'total_tenancies': 0,
                'active_tenancies': 0,
                'expiring_soon': 0,
                'total_monthly_income': 0
            }