# -*- coding: utf-8 -*-
"""
Property Status Repository
Handles database operations for property statuses using Stage_Master table
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


class PropertyStatusRepository:
    """
    Repository for property statuses using Stage_Master table
    Filters by stage_type = 3 for property-specific statuses
    """
    
    # Property status stage_type identifier
    PROPERTY_STATUS_TYPE = 3
    
    def __init__(self):
        if _supabase_configured():
            self.db = get_supabase_client()
        else:
            self.db = _LocalDBStub()
    
    def get_all_statuses(self) -> List[Dict[str, Any]]:
        """
        Get all property statuses from Stage_Master
        Filters by stage_type = 3
        
        Returns:
            List of property status records
        """
        query = """
            SELECT 
                "stage_id" as "status_id",
                "stage_name" as "status_name",
                "stage_description" as "status_description"
            FROM "StreemLyne_MT"."Stage_Master"
            WHERE "stage_type" = %s
            ORDER BY "stage_id"
        """
        
        try:
            return self.db.execute_query(query, (self.PROPERTY_STATUS_TYPE,))
        except Exception as e:
            logger.error(f"Error fetching property statuses: {e}")
            return []
    
    def get_status_by_id(self, status_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a specific property status by ID
        
        Args:
            status_id: Stage ID (must be property status type)
        
        Returns:
            Status record or None
        """
        query = """
            SELECT 
                "stage_id" as "status_id",
                "stage_name" as "status_name",
                "stage_description" as "status_description"
            FROM "StreemLyne_MT"."Stage_Master"
            WHERE "stage_id" = %s
            AND "stage_type" = %s
            LIMIT 1
        """
        
        try:
            return self.db.execute_query(
                query, 
                (status_id, self.PROPERTY_STATUS_TYPE), 
                fetch_one=True
            )
        except Exception as e:
            logger.error(f"Error fetching status {status_id}: {e}")
            return None
    
    def get_status_by_name(self, status_name: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific property status by name
        
        Args:
            status_name: Status name (e.g., 'Available', 'Occupied')
        
        Returns:
            Status record or None
        """
        query = """
            SELECT 
                "stage_id" as "status_id",
                "stage_name" as "status_name",
                "stage_description" as "status_description"
            FROM "StreemLyne_MT"."Stage_Master"
            WHERE LOWER("stage_name") = LOWER(%s)
            AND "stage_type" = %s
            LIMIT 1
        """
        
        try:
            return self.db.execute_query(
                query, 
                (status_name, self.PROPERTY_STATUS_TYPE), 
                fetch_one=True
            )
        except Exception as e:
            logger.error(f"Error fetching status by name '{status_name}': {e}")
            return None
    
    def get_default_status(self) -> Optional[Dict[str, Any]]:
        """
        Get the default property status ('Available')
        
        Returns:
            Default status record or None
        """
        return self.get_status_by_name('Available')