# -*- coding: utf-8 -*-
"""
Property Repository
Tenant-scoped database operations for Property_Master.

tenant_id must always be supplied by the service layer from JWT (never from client body).
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
                    em.employee_name AS assigned_agent_name
                FROM "{self.schema}"."Property_Master" p
                LEFT JOIN "{self.schema}"."Employee_Master" em ON p.assigned_agent_id = em.employee_id
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
                    query += (
                        " AND (p.assigned_crm_agent_id = %s OR p.assigned_agent_id = %s)"
                    )
                    params.append(filters["agent_id"])
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
                    em.employee_name AS assigned_agent_name
                FROM "{self.schema}"."Property_Master" p
                LEFT JOIN "{self.schema}"."Employee_Master" em ON p.assigned_agent_id = em.employee_id
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
        """Insert property; tenant_id from JWT only (passed in)."""
        try:
            now = datetime.utcnow().isoformat()
            
            display_id_query = f'''
                SELECT COALESCE(MAX(display_id), 0) + 1 as next_display_id
                FROM "{self.schema}"."Property_Master"
                WHERE tenant_id = %s AND is_deleted = FALSE
            '''
            display_result = self.supabase.execute_query(display_id_query, (tenant_id,), fetch_one=True)
            next_display_id = display_result.get('next_display_id', 1) if display_result else 1

            query = f'''
                INSERT INTO "{self.schema}"."Property_Master" (
                    tenant_id, display_id, property_name, property_type, address, city,
                    postcode, country_id, assigned_agent_id, assigned_crm_agent_id, 
                    monthly_rent, rent_due_day, deposit_amount,
                    purchase_price, currency_id, bedrooms, bathrooms, square_feet,
                    status_id, main_photo_url, document_details, is_active, is_deleted,
                    created_at, updated_at, created_by
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
                RETURNING property_id
            '''

            params = (
                tenant_id,
                next_display_id,
                data.get("property_name"),
                data.get("property_type", ""),
                data.get("address"),
                data.get("city", ""),
                data.get("postal_code", ""),
                data.get("country_id", 1),
                data.get("assigned_agent_id"),
                data.get("assigned_crm_agent_id"),
                data.get("monthly_rent", 0),
                data.get("rent_due_day"),
                data.get("deposit_amount", 0),
                data.get("purchase_price", 0),
                data.get("currency_id", 1),
                data.get("bedrooms", 0),
                data.get("bathrooms", 0),
                data.get("square_feet", 0),
                status_id,
                data.get("main_photo_url"),
                data.get("document_details"),
                True,
                False,
                now,
                now,
                created_by,
            )

            result = self.supabase.execute_insert(query, params, returning=True)

            if result and result.get("property_id"):
                return self.get_property_by_id(result["property_id"], tenant_id)

            return None

        except Exception as e:
            self.logger.error(f"Error creating property: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    def update_property(
        self, property_id: int, tenant_id: str, data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Update property within tenant."""
        try:
            now = datetime.utcnow().isoformat()

            update_fields = []
            params: List[Any] = []

            field_mapping = {
                "property_name": "property_name",
                "property_type": "property_type",
                "occupancy_status": "occupancy_status",
                "rent_due_day": "rent_due_day",  
                "address": "address",
                "city": "city",
                "state": "state",
                "postal_code": "postcode",
                "postcode": "postcode",  
                "country_id": "country_id",
                "assigned_agent_id": "assigned_agent_id",
                "assigned_crm_agent_id": "assigned_crm_agent_id",
                "monthly_rent": "monthly_rent",
                "deposit_amount": "deposit_amount",
                "purchase_price": "purchase_price",
                "currency_id": "currency_id",
                "bedrooms": "bedrooms",
                "bathrooms": "bathrooms",
                "square_feet": "square_feet",
                "year_built": "year_built",  
                "property_status": "property_status",
                "status_id": "status_id",
                "main_photo_url": "main_photo_url",
                "document_details": "document_details",
                "photo_urls": "photo_urls",  
                "description": "description",
                "amenities": "amenities",
                "parking_spaces": "parking_spaces",
                "pet_friendly": "pet_friendly",
                "furnished": "furnished",
                "tenant_name": "tenant_name",
                "tenant_contact": "tenant_contact",
                "tenant_email": "tenant_email",
                "lease_start_date": "lease_start_date",
                "lease_end_date": "lease_end_date",
            }

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
        """Soft-delete property within tenant and recalculate display IDs."""
        try:
            now = datetime.utcnow().isoformat()

            query = f'''
                UPDATE "{self.schema}"."Property_Master"
                SET is_deleted = TRUE,
                    deleted_at = %s,
                    updated_at = %s
                WHERE property_id = %s
                AND tenant_id = %s
            '''

            rows = self.supabase.execute_update(
                query, (now, now, property_id, tenant_id)
            )
            
            # ✅ AUTOMATIC: Recalculate display_ids after deletion
            if rows > 0:
                from backend.properties.services.property_display_id_service import recalculate_display_ids
                try:
                    recalculate_display_ids(tenant_id)
                    self.logger.info(f"✅ Auto-recalculated display IDs after deleting property {property_id}")
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to recalculate display IDs after delete: {e}")
                    # Don't fail the delete operation if recalculation fails
            
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
        """Properties assigned to legacy employee agent (assigned_agent_id)."""
        try:
            query = f'''
                SELECT 
                    p.*,
                    em.employee_name as assigned_agent_name
                FROM "{self.schema}"."Property_Master" p
                LEFT JOIN "{self.schema}"."Employee_Master" em ON p.assigned_agent_id = em.employee_id
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

    def get_properties_by_crm_agent(
        self, crm_agent_id: int, tenant_id: str
    ) -> List[Dict[str, Any]]:
        """Properties assigned via assigned_crm_agent_id (CRM agents table)."""
        try:
            query = f'''
                SELECT 
                    p.*,
                    em.employee_name AS assigned_agent_name
                FROM "{self.schema}"."Property_Master" p
                LEFT JOIN "{self.schema}"."Employee_Master" em ON p.assigned_agent_id = em.employee_id
                WHERE p.assigned_crm_agent_id = %s
                  AND p.tenant_id = %s
                  AND p.is_deleted = FALSE
                ORDER BY p.created_at DESC
            '''

            result = self.supabase.execute_query(query, (crm_agent_id, tenant_id))
            return result if result else []

        except Exception as e:
            self.logger.error(
                f"Error fetching properties for CRM agent {crm_agent_id}: {str(e)}"
            )
            return []