# -*- coding: utf-8 -*-
"""
Property Management Supporting Repositories
Handles database operations for supporting tables
"""
import logging
from typing import Optional, Dict, Any, List
from backend.properties.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)


class RoleRepository:
    """Repository for Role_Master table - UNIVERSAL, keep as-is"""
    
    def __init__(self):
        self.db = get_supabase_client()
    
    def get_all_roles(self, tenant_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get all roles (system + tenant-specific)
        
        Args:
            tenant_id: Optional tenant filter
        
        Returns:
            List of role records
        """
        if tenant_id:
            query = """
                SELECT * FROM "StreemLyne_MT"."Role_Master"
                WHERE "tenant_id" IS NULL OR "tenant_id" = %s
                ORDER BY "role_name"
            """
            params = (tenant_id,)
        else:
            query = 'SELECT * FROM "StreemLyne_MT"."Role_Master" ORDER BY "role_name"'
            params = None
        
        try:
            return self.db.execute_query(query, params)
        except Exception as e:
            logger.error(f"Error fetching roles: {e}")
            return []


class PropertyStatusRepository:
    """
    Repository for property status management
    Replaces StageRepository - properties have statuses, not sales pipeline stages
    """
    
    # Property statuses
    STATUSES = {
        'AVAILABLE': 'Available for Rent',
        'OCCUPIED': 'Currently Occupied',
        'MAINTENANCE': 'Under Maintenance',
        'LISTED': 'Listed for Sale/Rent',
        'RESERVED': 'Reserved',
        'SOLD': 'Sold',
        'UNAVAILABLE': 'Unavailable'
    }
    
    def __init__(self):
        self.db = get_supabase_client()
    
    def get_all_statuses(self) -> List[Dict[str, Any]]:
        """
        Get all property statuses
        Returns hardcoded list (or fetch from Property_Status_Master if you create one)
        """
        return [
            {'status_code': code, 'status_name': name}
            for code, name in self.STATUSES.items()
        ]
    
    def get_status_by_code(self, status_code: str) -> Optional[Dict[str, Any]]:
        """Get status details by code"""
        if status_code in self.STATUSES:
            return {
                'status_code': status_code,
                'status_name': self.STATUSES[status_code]
            }
        return None


class AgentRepository:
    """
    Repository for real estate agents (Employee_Master filtered by agent role)
    Replaces SupplierRepository
    """
    
    def __init__(self):
        self.db = get_supabase_client()
    
    def get_all_agents(self, tenant_id: int, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Get all real estate agents for a tenant
        
        Args:
            tenant_id: Tenant identifier
            active_only: Only return active agents
        
        Returns:
            List of agent records
        """
        query = """
            SELECT 
                em."employee_id",
                em."employee_name",
                em."email",
                em."phone",
                em."employee_designation_id",
                dm."designation_description" as "designation"
            FROM "StreemLyne_MT"."Employee_Master" em
            LEFT JOIN "StreemLyne_MT"."Designation_Master" dm 
                ON em."employee_designation_id" = dm."designation_id"
            WHERE em."tenant_id" = %s
        """
        
        params = [tenant_id]
        
        # You can add a specific role filter if you have an "Agent" role
        # query += ' AND em."role_ids" LIKE %s'
        # params.append('%AGENT%')
        
        query += ' ORDER BY em."employee_name"'
        
        try:
            return self.db.execute_query(query, tuple(params))
        except Exception as e:
            logger.error(f"Error fetching agents: {e}")
            return []
    
    def get_agent_by_id(self, tenant_id: int, agent_id: int) -> Optional[Dict[str, Any]]:
        """Get specific agent details"""
        query = """
            SELECT 
                em."employee_id",
                em."employee_name",
                em."email",
                em."phone",
                em."employee_designation_id",
                dm."designation_description" as "designation"
            FROM "StreemLyne_MT"."Employee_Master" em
            LEFT JOIN "StreemLyne_MT"."Designation_Master" dm 
                ON em."employee_designation_id" = dm."designation_id"
            WHERE em."tenant_id" = %s AND em."employee_id" = %s
        """
        
        try:
            return self.db.execute_query(query, (tenant_id, agent_id), fetch_one=True)
        except Exception as e:
            logger.error(f"Error fetching agent: {e}")
            return None


class PropertyTypeRepository:
    """
    Repository for property types
    New - not in energy broker system
    """
    
    # Property types (you can create a Property_Type_Master table later)
    TYPES = {
        'APARTMENT': 'Apartment',
        'HOUSE': 'House',
        'CONDO': 'Condominium',
        'TOWNHOUSE': 'Townhouse',
        'STUDIO': 'Studio',
        'COMMERCIAL': 'Commercial Property',
        'LAND': 'Land/Plot'
    }
    
    def __init__(self):
        self.db = get_supabase_client()
    
    def get_all_types(self) -> List[Dict[str, Any]]:
        """Get all property types"""
        return [
            {'type_code': code, 'type_name': name}
            for code, name in self.TYPES.items()
        ]


class PropertyInteractionRepository:
    """
    Repository for property-related interactions (viewings, inspections, maintenance logs)
    Replaces InteractionRepository
    """
    
    def __init__(self):
        self.db = get_supabase_client()
    
    def get_interactions_by_property(
        self, 
        tenant_id: int, 
        property_id: int,
        interaction_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all interactions for a specific property
        
        Args:
            tenant_id: Tenant identifier
            property_id: Property identifier
            interaction_type: Optional filter (VIEWING, INSPECTION, MAINTENANCE, etc.)
        
        Returns:
            List of interaction records
        """
        # Assuming you have a Property_Interactions table
        query = """
            SELECT 
                pi.*,
                em."employee_name" as "agent_name"
            FROM "StreemLyne_MT"."Property_Interactions" pi
            LEFT JOIN "StreemLyne_MT"."Employee_Master" em 
                ON pi."employee_id" = em."employee_id"
            WHERE pi."tenant_id" = %s AND pi."property_id" = %s
        """
        
        params = [tenant_id, property_id]
        
        if interaction_type:
            query += ' AND pi."interaction_type" = %s'
            params.append(interaction_type)
        
        query += ' ORDER BY pi."interaction_date" DESC'
        
        try:
            return self.db.execute_query(query, tuple(params))
        except Exception as e:
            logger.error(f"Error fetching property interactions: {e}")
            return []
    
    def create_interaction(
        self, 
        tenant_id: int, 
        property_id: int, 
        interaction_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Create a property interaction record
        
        Args:
            tenant_id: Tenant identifier
            property_id: Property identifier
            interaction_data: Interaction details (type, date, notes, employee_id, etc.)
        
        Returns:
            Created interaction record
        """
        query = """
            INSERT INTO "StreemLyne_MT"."Property_Interactions"
            (
                "tenant_id", 
                "property_id", 
                "interaction_type", 
                "interaction_date",
                "notes", 
                "employee_id",
                "created_at"
            )
            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING *
        """
        
        try:
            return self.db.execute_insert(
                query,
                (
                    tenant_id,
                    property_id,
                    interaction_data.get('interaction_type', 'NOTE'),
                    interaction_data.get('interaction_date', 'CURRENT_DATE'),
                    interaction_data.get('notes', ''),
                    interaction_data.get('employee_id')
                ),
                returning=True
            )
        except Exception as e:
            logger.error(f"Error creating property interaction: {e}")
            import traceback
            traceback.print_exc()
            return None


class MaintenanceRepository:
    """
    Repository for property maintenance requests/records
    New - specific to property management
    """
    
    def __init__(self):
        self.db = get_supabase_client()
    
    def get_maintenance_by_property(
        self, 
        tenant_id: int, 
        property_id: int
    ) -> List[Dict[str, Any]]:
        """Get all maintenance records for a property"""
        query = """
            SELECT * FROM "StreemLyne_MT"."Property_Maintenance"
            WHERE "tenant_id" = %s AND "property_id" = %s
            ORDER BY "created_at" DESC
        """
        
        try:
            return self.db.execute_query(query, (tenant_id, property_id))
        except Exception as e:
            logger.error(f"Error fetching maintenance records: {e}")
            return []
    
    def create_maintenance_request(
        self, 
        tenant_id: int, 
        property_id: int, 
        maintenance_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Create a new maintenance request"""
        query = """
            INSERT INTO "StreemLyne_MT"."Property_Maintenance"
            (
                "tenant_id",
                "property_id",
                "issue_type",
                "description",
                "priority",
                "status",
                "reported_by",
                "created_at"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING *
        """
        
        try:
            return self.db.execute_insert(
                query,
                (
                    tenant_id,
                    property_id,
                    maintenance_data.get('issue_type'),
                    maintenance_data.get('description'),
                    maintenance_data.get('priority', 'MEDIUM'),
                    maintenance_data.get('status', 'PENDING'),
                    maintenance_data.get('reported_by')
                ),
                returning=True
            )
        except Exception as e:
            logger.error(f"Error creating maintenance request: {e}")
            return None