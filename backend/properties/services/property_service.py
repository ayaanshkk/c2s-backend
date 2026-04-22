# backend/properties/services/property_service.py

import logging
from typing import Optional, Dict, Any, List

from backend.properties.repositories.property_repository import PropertyRepository
from backend.properties.repositories import PropertyStatusRepository
from backend.properties.repositories.user_repository import UserRepository

logger = logging.getLogger(__name__)


def _strip_tenant_from_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    """Client must never control tenant_id — ignore if sent."""
    if not data:
        return {}
    return {k: v for k, v in data.items() if k != "tenant_id"}


class PropertyService:
    """Service layer for property management (tenant-scoped)."""

    def __init__(self):
        self.property_repo = PropertyRepository()
        self.status_repo = PropertyStatusRepository()
        self.user_repo = UserRepository()

    def _ensure_agent_in_tenant(self, agent_id: Optional[int], tenant_id: str) -> None:
        """Cross-tenant guard: assigned employee must belong to JWT tenant.
        
        Agent assignment is OPTIONAL - only validate if explicitly provided.
        """
        # ✅ FIX: Agent assignment is OPTIONAL - skip validation for None, "", 0, or False
        if not agent_id:  # Catches None, 0, "", False
            return
        
        try:
            agent_id_int = int(agent_id)
        except (ValueError, TypeError):
            # If we can't convert to int, treat as "no agent"
            return
        
        if not self.user_repo.employee_belongs_to_tenant(agent_id_int, tenant_id):
            raise PermissionError(
                "Agent does not belong to this tenant or was not found."
            )

    def get_all_properties(
        self, tenant_id: str, filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict]:
        try:
            return self.property_repo.get_all_properties(tenant_id, filters)
        except Exception as e:
            logger.error(f"Error fetching properties: {e}")
            return []

    def get_property_by_id(
        self, property_id: int, tenant_id: str
    ) -> Optional[Dict]:
        try:
            return self.property_repo.get_property_by_id(property_id, tenant_id)
        except Exception as e:
            logger.error(f"Error fetching property {property_id}: {e}")
            return None

    def create_property(
        self, data: Dict[str, Any], created_by: int, tenant_id: str
    ) -> Optional[Dict]:
        try:
            data = _strip_tenant_from_payload(data)
            required_fields = ["property_name", "address", "city"]
            missing = [f for f in required_fields if not data.get(f)]
            if missing:
                raise ValueError(
                    f"Missing required fields: {', '.join(missing)}"
                )

            # ✅ FIX: Only validate agent if one is actually being assigned
            # Agent assignment is completely OPTIONAL
            assigned_agent_id = data.get("assigned_agent_id")
            if not assigned_agent_id or assigned_agent_id == 0:
                data["assigned_agent_id"] = None

            status_id = data.get("status_id")
            if status_id is None:
                default = self.status_repo.get_default_status()
                if default:
                    status_id = default.get("status_id")
            if status_id is None:
                raise ValueError(
                    "Could not resolve status_id — ensure Stage_Master has stage_type=3 rows "
                    "(e.g. 'Available') or pass status_id in the payload."
                )

            result = self.property_repo.create_property(
                data, created_by, tenant_id, int(status_id)
            )

            if isinstance(result, int):
                return self.property_repo.get_property_by_id(result, tenant_id)
            return result

        except Exception as e:
            logger.error(f"Error creating property: {e}")
            raise

    def update_property(
        self, property_id: int, tenant_id: str, data: Dict[str, Any]
    ) -> Optional[Dict]:
        try:
            data = _strip_tenant_from_payload(data)
            
            # ✅ ADD THIS: Normalize assigned_agent_id before validation
            if "assigned_agent_id" in data:
                assigned_agent_id = data.get("assigned_agent_id")
                if not assigned_agent_id or assigned_agent_id == 0:
                    data["assigned_agent_id"] = None
                else:
                    # Only validate if we're assigning a real agent
                    self._ensure_agent_in_tenant(assigned_agent_id, tenant_id)
            
            return self.property_repo.update_property(property_id, tenant_id, data)
        except Exception as e:
            logger.error(f"Error updating property {property_id}: {e}")
            raise

    def delete_property(
        self, property_id: int, deleted_by: int, tenant_id: str
    ) -> bool:
        try:
            return self.property_repo.delete_property(
                property_id, deleted_by, tenant_id
            )
        except Exception as e:
            logger.error(f"Error deleting property {property_id}: {e}")
            return False

    def assign_to_agent(
        self, property_id: int, agent_id: Optional[int], tenant_id: str
    ) -> Optional[Dict]:
        try:
            # ✅ FIX: Allow None to unassign agent, only validate if assigning
            if agent_id:  # Only validate if actually assigning an agent
                self._ensure_agent_in_tenant(agent_id, tenant_id)
            return self.property_repo.assign_to_agent(
                property_id, agent_id, tenant_id
            )
        except Exception as e:
            logger.error(
                f"Error assigning property {property_id} to agent {agent_id}: {e}"
            )
            raise

    def get_properties_by_agent(
        self, agent_id: int, tenant_id: str
    ) -> List[Dict]:
        # ✅ NOTE: Here we DO require a valid agent_id since we're querying BY agent
        # If agent_id is invalid, we should raise an error
        if not agent_id:
            raise ValueError("agent_id is required to fetch properties by agent")
        
        self._ensure_agent_in_tenant(agent_id, tenant_id)
        try:
            return self.property_repo.get_properties_by_agent(agent_id, tenant_id)
        except Exception as e:
            logger.error(f"Error fetching properties for agent {agent_id}: {e}")
            return []

    def get_dashboard_stats(self, tenant_id: str) -> Dict[str, Any]:
        """Lightweight stats from repository list (tenant-scoped)."""
        try:
            all_properties = self.property_repo.get_all_properties(tenant_id, None)
            total = len(all_properties)
            available = sum(
                1
                for p in all_properties
                if (p.get("status_name") or p.get("property_status") or "")
                .lower()
                == "available"
            )
            occupied = sum(
                1
                for p in all_properties
                if (p.get("status_name") or p.get("property_status") or "")
                .lower()
                == "occupied"
            )
            maintenance = sum(
                1
                for p in all_properties
                if "maintenance"
                in (p.get("status_name") or p.get("property_status") or "").lower()
            )
            cities = set(
                p.get("city") for p in all_properties if p.get("city")
            )
            return {
                "total_properties": total,
                "available": available,
                "occupied": occupied,
                "under_maintenance": maintenance,
                "total_cities": len(cities),
                "occupancy_rate": round((occupied / total * 100), 2) if total > 0 else 0,
            }
        except Exception as e:
            logger.error(f"Error in get_dashboard_stats: {e}")
            return {}

    def get_all_statuses(self) -> List[Dict]:
        try:
            return self.status_repo.get_all_statuses()
        except Exception as e:
            logger.error(f"Error fetching statuses: {e}")
            return []
