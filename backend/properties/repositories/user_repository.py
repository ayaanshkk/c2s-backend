# -*- coding: utf-8 -*-
"""
User Repository
Handles database operations for User_Master table (Property Management Users)
"""
import os
import logging
from typing import Optional, Dict, Any, List
from backend.properties.supabase_client import supabase  
import secrets

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
        try:
            rows = self.supabase.execute_query(
                f'''
                SELECT
                    um.user_id,
                    um.user_name,
                    um.password,
                    um.employee_id,
                    em.employee_name,
                    em.tenant_id,
                    em.email as employee_email
                FROM "{self.schema}"."User_Master" um
                LEFT JOIN "{self.schema}"."Employee_Master" em 
                    ON um.employee_id = em.employee_id
                WHERE LOWER(um.user_name) = LOWER(%s)
                LIMIT 1
                ''',
                (username,),
            )
            return rows[0] if rows else None
        except Exception as e:
            self.logger.error(f"Error fetching user by username: {e}")
            return None
    
    def get_all_agents(self, tenant_id: str):
        """Get only employees with role_id = 3 (Agent) for this tenant."""
        try:
            rows = self.supabase.execute_query(
                f'''
                SELECT
                    em.employee_id,
                    em.employee_name,
                    em.email,
                    em.phone,
                    em.commission_percentage,
                    dm.designation_description          AS designation,
                    um.user_id,
                    um.is_invite_pending,
                    COALESCE(
                        json_agg(rm.role_name) FILTER (WHERE rm.role_name IS NOT NULL),
                        '[]'::json
                    )                                   AS roles
                FROM "{self.schema}"."Employee_Master" em
                LEFT JOIN "{self.schema}"."Designation_Master" dm
                    ON dm.designation_id = em.employee_designation_id
                LEFT JOIN "{self.schema}"."User_Master" um
                    ON um.employee_id = em.employee_id
                LEFT JOIN "{self.schema}"."User_Role_Mapping" urm
                    ON urm.user_id = um.user_id
                LEFT JOIN "{self.schema}"."Role_Master" rm
                    ON rm.role_id = urm.role_id
                WHERE em.tenant_id = %s
                GROUP BY
                    em.employee_id, em.employee_name, em.email, em.phone,
                    em.commission_percentage, dm.designation_description,
                    um.user_id, um.is_invite_pending
                HAVING bool_or(urm.role_id = 3)
                ORDER BY em.employee_name
                ''',
                (tenant_id,),
            )
            return rows or []
        except Exception as e:
            self.logger.error("Error fetching all agents: %s", e)
            return []


    def get_agent_by_id(self, agent_id: int, tenant_id: str):
        """Get single agent by ID scoped to tenant, with user and role info."""
        try:
            rows = self.supabase.execute_query(
                f'''
                SELECT
                    em.employee_id,
                    em.employee_name,
                    em.email,
                    em.phone,
                    em.commission_percentage,
                    em.date_of_joining,
                    em.date_of_birth,
                    em.id_type,
                    em.id_number,
                    dm.designation_description          AS designation,
                    um.user_id,
                    um.is_invite_pending,
                    COALESCE(
                        json_agg(rm.role_name) FILTER (WHERE rm.role_name IS NOT NULL),
                        '[]'::json
                    )                                   AS roles
                FROM "{self.schema}"."Employee_Master" em
                LEFT JOIN "{self.schema}"."Designation_Master" dm
                    ON dm.designation_id = em.employee_designation_id
                LEFT JOIN "{self.schema}"."User_Master" um
                    ON um.employee_id = em.employee_id
                LEFT JOIN "{self.schema}"."User_Role_Mapping" urm
                    ON urm.user_id = um.user_id
                LEFT JOIN "{self.schema}"."Role_Master" rm
                    ON rm.role_id = urm.role_id
                WHERE em.tenant_id = %s
                AND em.employee_id = %s
                GROUP BY
                    em.employee_id, em.employee_name, em.email, em.phone,
                    em.commission_percentage, em.date_of_joining, em.date_of_birth,
                    em.id_type, em.id_number, dm.designation_description,
                    um.user_id, um.is_invite_pending
                ''',
                (tenant_id, agent_id),
            )
            return rows[0] if rows else None
        except Exception as e:
            self.logger.error("Error fetching agent %s: %s", agent_id, e)
            return None

    def create_employee_agent(self, tenant_id: str, name: str, email: str | None, phone: str) -> dict:
        """Create employee and user with invite token and assign agent role"""
        from backend.db import SessionLocal
        from sqlalchemy import text
        import secrets
        
        session = SessionLocal()
        try:
            # Insert employee
            insert_emp = text('''
                INSERT INTO "StreemLyne_MT"."Employee_Master" (tenant_id, employee_name, email, phone)
                VALUES (:tenant_id, :employee_name, :email, :phone)
                RETURNING employee_id, employee_name, email, phone
            ''')
            
            emp_row = session.execute(insert_emp, {
                'tenant_id': tenant_id,
                'employee_name': name,
                'email': email,
                'phone': phone
            }).mappings().first()
            
            if not emp_row:
                raise Exception('Failed to create employee')
            
            employee_id = emp_row['employee_id']
            
            # Generate invite token
            invite_token = secrets.token_urlsafe(32)
            
            # Create User_Master record with invite pending
            insert_user = text('''
                INSERT INTO "StreemLyne_MT"."User_Master" 
                (employee_id, invite_token, is_invite_pending)
                VALUES (:employee_id, :invite_token, TRUE)
                RETURNING user_id
            ''')
            
            user_row = session.execute(insert_user, {
                'employee_id': employee_id,
                'invite_token': invite_token
            }).mappings().first()
            
            if not user_row:
                raise Exception('Failed to create user record')
            
            user_id = user_row['user_id']
            
            # ✅ ADD THIS: Assign agent role (role_id = 3)
            insert_role = text('''
                INSERT INTO "StreemLyne_MT"."User_Role_Mapping" (user_id, role_id)
                VALUES (:user_id, 3)
            ''')
            
            session.execute(insert_role, {'user_id': user_id})
            
            session.commit()
            
            return {
                'employee_id': employee_id,
                'employee_name': emp_row['employee_name'],
                'email': emp_row['email'],
                'phone': emp_row['phone'],
                'invite_token': invite_token,
                'is_invite_pending': True
            }
            
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()


    def complete_invite_acceptance(self, user_id: int, username: str, password: str) -> bool:
        """Complete invite acceptance by setting username and password"""
        try:
            result = self.supabase.execute_update(
                f'''
                UPDATE "{self.schema}"."User_Master"
                SET 
                    user_name = %s,
                    password = %s,
                    is_invite_pending = FALSE,
                    invite_token = NULL
                WHERE user_id = %s
                AND is_invite_pending = TRUE
                ''',
                (username, password, user_id),
            )
            return result is not None
        except Exception as e:
            self.logger.error("complete_invite_acceptance failed: %s", e)
            return False


    def generate_invite_for_user(self, user_id: int) -> Optional[str]:
        """Generate and store a unique invite token for a user."""
        token = secrets.token_urlsafe(32)
        result = self.supabase.execute_update(
            f'''
            UPDATE "{self.schema}"."User_Master"
            SET invite_token = %s, is_invite_pending = TRUE
            WHERE user_id = %s
            ''',
            (token, user_id),
        )
        return token if result else None


    def get_user_by_invite_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Get user details by invite token (for accept-invite page)."""
        rows = self.supabase.execute_query(
            f'''
            SELECT
                um.user_id,
                um.user_name,
                um.is_invite_pending,
                em.employee_name,
                em.email       AS employee_email,
                em.phone       AS employee_phone,
                rm.role_name
            FROM "{self.schema}"."User_Master" um
            LEFT JOIN "{self.schema}"."Employee_Master" em
                ON em.employee_id = um.employee_id
            LEFT JOIN "{self.schema}"."User_Role_Mapping" urm
                ON urm.user_id = um.user_id
            LEFT JOIN "{self.schema}"."Role_Master" rm
                ON rm.role_id = urm.role_id
            WHERE um.invite_token = %s
            AND um.is_invite_pending = TRUE
            LIMIT 1
            ''',
            (token,),
        )
        return rows[0] if rows else None
    
    def regenerate_agent_invite(self, agent_id: int, tenant_id: str) -> dict | None:
        """
        Regenerate invite token for an agent with pending invite.
        Returns None if agent not found or invite already accepted.
        """
        from backend.db import SessionLocal
        from sqlalchemy import text
        import secrets
        
        session = SessionLocal()
        try:
            # Check if there's a User_Master record with pending invite for this employee
            check_query = text("""
                SELECT 
                    um.user_id,
                    um.employee_id,
                    um.invite_token,
                    um.is_invite_pending,
                    em.employee_name,
                    em.email,
                    em.phone
                FROM "StreemLyne_MT"."User_Master" um
                JOIN "StreemLyne_MT"."Employee_Master" em
                    ON um.employee_id = em.employee_id
                WHERE um.employee_id = :agent_id
                AND em.tenant_id = :tenant_id
            """)
            
            result = session.execute(check_query, {
                'agent_id': agent_id,
                'tenant_id': tenant_id
            }).first()
            
            if not result:
                # No user record found for this employee
                return None
            
            if not result.is_invite_pending:
                # Invite already accepted
                return None
            
            # Generate new token
            new_token = secrets.token_urlsafe(32)
            
            # Update the token
            update_query = text("""
                UPDATE "StreemLyne_MT"."User_Master"
                SET invite_token = :token,
                    updated_at = CURRENT_TIMESTAMP
                WHERE employee_id = :agent_id
                RETURNING user_id, employee_id, invite_token
            """)
            
            updated = session.execute(update_query, {
                'agent_id': agent_id,
                'token': new_token
            }).first()
            
            session.commit()
            
            if updated:
                return {
                    'employee_id': result.employee_id,
                    'employee_name': result.employee_name,
                    'email': result.email,
                    'phone': result.phone,
                    'invite_token': updated.invite_token
                }
            
            return None
            
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()


    def delete_agent(self, agent_id: int, tenant_id: str) -> None:
        """Delete agent and all associated user/role records."""
        # Delete role mappings first
        self.supabase.execute_delete(
            f'''
            DELETE FROM "{self.schema}"."User_Role_Mapping"
            WHERE user_id IN (
                SELECT user_id FROM "{self.schema}"."User_Master"
                WHERE employee_id = %s
            )
            ''',
            (agent_id,),
        )
        # Delete user account
        self.supabase.execute_delete(
            f'DELETE FROM "{self.schema}"."User_Master" WHERE employee_id = %s',
            (agent_id,),
        )
        # Delete employee record (tenant-scoped)
        self.supabase.execute_delete(
            f'''
            DELETE FROM "{self.schema}"."Employee_Master"
            WHERE employee_id = %s AND tenant_id = %s
            ''',
            (agent_id, tenant_id),
        )

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