# -*- coding: utf-8 -*-
"""
User Repository
Handles database operations for User_Master table (Property Management Users)
"""
import os
import logging
from typing import Optional, Dict, Any, List
from backend.properties.supabase_client import supabase  # ✅ Import supabase directly

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


class UserRepository:
    """
    Repository for User_Master table (Property Management Users)
    All queries are tenant-filtered for multi-tenant isolation
    """

    def __init__(self):
        # ✅ Add schema and supabase attributes
        self.schema = "StreemLyne_MT"
        self.supabase = supabase
        self.logger = logging.getLogger(__name__)
        
        if _supabase_configured():
            self.db = supabase  # Use supabase client
        else:
            self.db = _LocalDBStub()

    def get_all_users(self, tenant_id: str, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Get all users for a tenant (property management company)

        Args:
            tenant_id: Tenant identifier
            active_only: If True, only return active users

        Returns:
            List of user records with role information
        """
        query = """
            SELECT
                um.*,
                em."employee_name",
                em."email" as "employee_email",
                em."phone" as "employee_phone",
                rm."role_name",
                rm."role_description"
            FROM "StreemLyne_MT"."User_Master" um
            LEFT JOIN "StreemLyne_MT"."Employee_Master" em 
                ON um."employee_id" = em."employee_id"
            LEFT JOIN "StreemLyne_MT"."User_Role_Mapping" urm 
                ON urm."user_id" = um."user_id"
            LEFT JOIN "StreemLyne_MT"."Role_Master" rm 
                ON rm."role_id" = urm."role_id"
            WHERE em."tenant_id" = %s
        """

        params = [tenant_id]

        if active_only:
            query += ' AND (um."is_active" IS NULL OR um."is_active" = TRUE)'

        query += ' ORDER BY um."user_name"'

        try:
            return self.db.execute_query(query, tuple(params))
        except Exception as e:
            logger.error(f"Error fetching users for tenant {tenant_id}: {e}")
            return []

    def get_user_by_id(self, tenant_id: str, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a specific user by ID (with tenant isolation)

        Args:
            tenant_id: Tenant identifier
            user_id: User identifier

        Returns:
            User record with employee and role details or None
        """
        query = """
            SELECT
                um.*,
                em."employee_name",
                em."email" as "employee_email",
                em."phone" as "employee_phone",
                em."employee_designation_id",
                dm."designation_description",
                rm."role_name",
                rm."role_description"
            FROM "StreemLyne_MT"."User_Master" um
            LEFT JOIN "StreemLyne_MT"."Employee_Master" em 
                ON um."employee_id" = em."employee_id"
            LEFT JOIN "StreemLyne_MT"."Designation_Master" dm 
                ON em."employee_designation_id" = dm."designation_id"
            LEFT JOIN "StreemLyne_MT"."User_Role_Mapping" urm 
                ON urm."user_id" = um."user_id"
            LEFT JOIN "StreemLyne_MT"."Role_Master" rm 
                ON rm."role_id" = urm."role_id"
            WHERE em."tenant_id" = %s
            AND um."user_id" = %s
            LIMIT 1
        """

        try:
            return self.db.execute_query(query, (tenant_id, user_id), fetch_one=True)
        except Exception as e:
            logger.error(f"Error fetching user {user_id}: {e}")
            return None

    def get_users_by_role(self, tenant_id: str, role_id: int) -> List[Dict[str, Any]]:
        """
        Get all users with a specific role (e.g., all agents, all property managers)

        Args:
            tenant_id: Tenant identifier
            role_id: Role identifier

        Returns:
            List of users with the specified role
        """
        query = """
            SELECT
                um.*,
                em."employee_name",
                em."email" as "employee_email",
                em."phone" as "employee_phone",
                rm."role_name"
            FROM "StreemLyne_MT"."User_Master" um
            INNER JOIN "StreemLyne_MT"."Employee_Master" em 
                ON um."employee_id" = em."employee_id"
            INNER JOIN "StreemLyne_MT"."User_Role_Mapping" urm 
                ON urm."user_id" = um."user_id"
            LEFT JOIN "StreemLyne_MT"."Role_Master" rm 
                ON rm."role_id" = urm."role_id"
            WHERE em."tenant_id" = %s
            AND urm."role_id" = %s
            ORDER BY um."user_name"
        """

        try:
            return self.db.execute_query(query, (tenant_id, role_id))
        except Exception as e:
            logger.error(f"Error fetching users by role: {e}")
            return []
    
    def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Get a user by username (for authentication)
        
        Args:
            username: User's username/email
        
        Returns:
            User record or None
        """
        query = """
            SELECT
                um.*,
                em."employee_name",
                em."tenant_id",
                em."email" as "employee_email"
            FROM "StreemLyne_MT"."User_Master" um
            LEFT JOIN "StreemLyne_MT"."Employee_Master" em 
                ON um."employee_id" = em."employee_id"
            WHERE LOWER(um."user_name") = LOWER(%s)
            LIMIT 1
        """
        
        try:
            return self.db.execute_query(query, (username,), fetch_one=True)
        except Exception as e:
            logger.error(f"Error fetching user by username: {e}")
            return None
    
    def get_all_agents(self, tenant_id: str):
        """Get property agents for one tenant (Employee_Master)."""
        try:
            query = f'''
                SELECT 
                    em.employee_id,
                    em.employee_name,
                    em.email,
                    em.phone
                FROM "{self.schema}"."Employee_Master" em
                WHERE em.tenant_id = %s
                ORDER BY em.employee_name
            '''

            result = self.supabase.execute_query(query, (tenant_id,))

            if result:
                return result
            return []

        except Exception as e:
            self.logger.error(f"Error fetching all agents: {str(e)}")
            return []

    def get_agent_by_id(self, agent_id: int, tenant_id: str):
        """Get agent by ID scoped to tenant."""
        try:
            query = f'''
                SELECT 
                    em.employee_id,
                    em.employee_name,
                    em.email,
                    em.phone
                FROM "{self.schema}"."Employee_Master" em
                WHERE em.employee_id = %s
                  AND em.tenant_id = %s
            '''
            
            # ✅ REMOVED: AND em.is_active = TRUE (column doesn't exist)
            
            result = self.supabase.execute_query(
                query, (agent_id, tenant_id), fetch_one=True
            )
            
            if result:
                return result
            return None
            
        except Exception as e:
            self.logger.error(f"Error fetching agent {agent_id}: {str(e)}")
            return None

    def employee_belongs_to_tenant(self, employee_id: int, tenant_id: str) -> bool:
        """True if employee_id exists in Employee_Master for this tenant (string slug)."""
        if not tenant_id or employee_id is None:
            return False
        try:
            query = f'''
                SELECT 1
                FROM "{self.schema}"."Employee_Master" em
                WHERE em.employee_id = %s AND em.tenant_id = %s
                LIMIT 1
            '''
            row = self.supabase.execute_query(
                query, (employee_id, tenant_id), fetch_one=True
            )
            return bool(row)
        except Exception as e:
            self.logger.error(
                "employee_belongs_to_tenant failed: %s", e, exc_info=True
            )
            return False