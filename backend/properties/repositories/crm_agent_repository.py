# -*- coding: utf-8 -*-
"""CRM agents table — tenant-scoped; separate from Employee_Master."""
import logging
from typing import Any, Dict, List, Optional

from backend.properties.supabase_client import supabase

logger = logging.getLogger(__name__)


class CrmAgentRepository:
    def __init__(self):
        self.schema = "StreemLyne_MT"
        self.supabase = supabase

    def list_agents(self, tenant_id: str, active_only: bool = True) -> List[Dict[str, Any]]:
        q = f'''
            SELECT crm_agent_id, tenant_id, display_name, email, phone, notes,
                   is_active, linked_employee_id, created_at, updated_at
            FROM "{self.schema}"."crm_agents"
            WHERE tenant_id = %s
        '''
        params: List[Any] = [tenant_id]
        if active_only:
            q += " AND is_active = TRUE"
        q += " ORDER BY display_name ASC"
        try:
            rows = self.supabase.execute_query(q, tuple(params))
            return rows if rows else []
        except Exception as e:
            logger.error("list_agents: %s", e)
            raise

    def get_by_id(self, crm_agent_id: int, tenant_id: str) -> Optional[Dict[str, Any]]:
        q = f'''
            SELECT crm_agent_id, tenant_id, display_name, email, phone, notes,
                   is_active, linked_employee_id, created_at, updated_at
            FROM "{self.schema}"."crm_agents"
            WHERE crm_agent_id = %s AND tenant_id = %s
            LIMIT 1
        '''
        try:
            return self.supabase.execute_query(
                q, (crm_agent_id, tenant_id), fetch_one=True
            )
        except Exception as e:
            logger.error("get_by_id crm_agent: %s", e)
            raise

    def belongs_to_tenant(self, crm_agent_id: int, tenant_id: str) -> bool:
        """True if row exists and is active (assignable)."""
        row = self.get_by_id(crm_agent_id, tenant_id)
        return row is not None and bool(row.get("is_active", True))

    def exists_in_tenant(self, crm_agent_id: int, tenant_id: str) -> bool:
        """True if row exists (any active flag); for read-only listings."""
        return self.get_by_id(crm_agent_id, tenant_id) is not None

    def create(
        self,
        tenant_id: str,
        display_name: str,
        email: Optional[str] = None,
        phone: Optional[str] = None,
        notes: Optional[str] = None,
        linked_employee_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        q = f'''
            INSERT INTO "{self.schema}"."crm_agents"
                (tenant_id, display_name, email, phone, notes, linked_employee_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING crm_agent_id
        '''
        try:
            result = self.supabase.execute_insert(
                q,
                (tenant_id, display_name.strip(), email, phone, notes, linked_employee_id),
                returning=True,
            )
            if result and result.get("crm_agent_id"):
                return self.get_by_id(int(result["crm_agent_id"]), tenant_id)
            return None
        except Exception as e:
            logger.error("create crm_agent: %s", e)
            raise

    def update(
        self,
        crm_agent_id: int,
        tenant_id: str,
        data: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        allowed = {
            "display_name": "display_name",
            "email": "email",
            "phone": "phone",
            "notes": "notes",
            "is_active": "is_active",
            "linked_employee_id": "linked_employee_id",
        }
        sets = []
        params: List[Any] = []
        for key, col in allowed.items():
            if key in data:
                sets.append(f'"{col}" = %s')
                params.append(data[key])
        if not sets:
            return self.get_by_id(crm_agent_id, tenant_id)
        sets.append('"updated_at" = NOW()')
        params.extend([crm_agent_id, tenant_id])
        q = f'''
            UPDATE "{self.schema}"."crm_agents"
            SET {", ".join(sets)}
            WHERE crm_agent_id = %s AND tenant_id = %s
        '''
        try:
            self.supabase.execute_update(q, tuple(params))
            return self.get_by_id(crm_agent_id, tenant_id)
        except Exception as e:
            logger.error("update crm_agent: %s", e)
            raise

    def soft_delete(self, crm_agent_id: int, tenant_id: str) -> bool:
        q = f'''
            UPDATE "{self.schema}"."crm_agents"
            SET is_active = FALSE, updated_at = NOW()
            WHERE crm_agent_id = %s AND tenant_id = %s
        '''
        try:
            n = self.supabase.execute_update(q, (crm_agent_id, tenant_id))
            return n > 0
        except Exception as e:
            logger.error("soft_delete crm_agent: %s", e)
            raise
