# backend/routes/agent_routes.py

from flask import Blueprint, jsonify, request
from backend.routes.auth_helpers import token_required, get_current_tenant_id
from backend.properties.repositories.crm_agent_repository import CrmAgentRepository
from backend.properties.services.property_service import PropertyService
import logging

logger = logging.getLogger(__name__)

agent_bp = Blueprint("agents", __name__)
_repo = CrmAgentRepository()


def _serialize_agent(row: dict) -> dict:
    """Stable shape for frontend + backward-compatible aliases."""
    if not row:
        return row
    cid = row.get("crm_agent_id")
    name = row.get("display_name") or ""
    out = {
        "crm_agent_id": cid,
        "display_name": name,
        "email": row.get("email"),
        "phone": row.get("phone"),
        "notes": row.get("notes"),
        "is_active": row.get("is_active", True),
        "linked_employee_id": row.get("linked_employee_id"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        # Aliases (legacy UI components)
        "employee_id": cid,
        "employee_name": name,
    }
    return out


def _crm_table_missing_response(e: Exception):
    msg = str(e).lower()
    if "does not exist" in msg or "undefinedtable" in msg or "crm_agents" in msg:
        return (
            jsonify(
                {
                    "success": False,
                    "error": "crm_agents table missing",
                    "message": "Run migrations/004_crm_agents.sql in Supabase.",
                }
            ),
            503,
        )
    return None


@agent_bp.route("/", methods=["GET", "POST", "OPTIONS"])
@token_required
def agents_collection():
    if request.method == "OPTIONS":
        return "", 204

    tenant_id = get_current_tenant_id()
    if not tenant_id:
        return (
            jsonify(
                {
                    "success": False,
                    "error": "Invalid tenant context",
                    "message": "tenant_id missing in token or X-Tenant-ID mismatch",
                }
            ),
            403,
        )

    if request.method == "GET":
        include_inactive = request.args.get("include_inactive", "false").lower() == "true"
        try:
            rows = _repo.list_agents(tenant_id, active_only=not include_inactive)
            return (
                jsonify(
                    {
                        "success": True,
                        "data": [_serialize_agent(r) for r in rows],
                    }
                ),
                200,
            )
        except Exception as e:
            logger.error("Error fetching CRM agents: %s", e)
            resp = _crm_table_missing_response(e)
            if resp:
                return resp
            return jsonify({"success": False, "error": str(e)}), 500

    # POST — create agent
    try:
        body = request.get_json(silent=True) or {}
        name = (body.get("display_name") or body.get("name") or "").strip()
        if not name:
            return jsonify({"success": False, "error": "display_name is required"}), 400
        row = _repo.create(
            tenant_id,
            display_name=name,
            email=(body.get("email") or "").strip() or None,
            phone=(body.get("phone") or "").strip() or None,
            notes=(body.get("notes") or "").strip() or None,
            linked_employee_id=body.get("linked_employee_id"),
        )
        if not row:
            return jsonify({"success": False, "error": "Create failed"}), 500
        return (
            jsonify(
                {
                    "success": True,
                    "agent": _serialize_agent(row),
                    "message": "Agent created",
                }
            ),
            201,
        )
    except Exception as e:
        logger.error("Error creating CRM agent: %s", e)
        resp = _crm_table_missing_response(e)
        if resp:
            return resp
        return jsonify({"success": False, "error": str(e)}), 500


@agent_bp.route("/<int:agent_id>", methods=["GET", "PATCH", "DELETE", "OPTIONS"])
@token_required
def agent_one(agent_id):
    if request.method == "OPTIONS":
        return "", 204

    tenant_id = get_current_tenant_id()
    if not tenant_id:
        return (
            jsonify(
                {
                    "success": False,
                    "error": "Invalid tenant context",
                    "message": "tenant_id missing in token or X-Tenant-ID mismatch",
                }
            ),
            403,
        )

    if request.method == "GET":
        try:
            row = _repo.get_by_id(agent_id, tenant_id)
            if not row:
                return jsonify({"success": False, "error": "Agent not found"}), 404
            return jsonify({"success": True, "agent": _serialize_agent(row)}), 200
        except Exception as e:
            resp = _crm_table_missing_response(e)
            if resp:
                return resp
            return jsonify({"success": False, "error": str(e)}), 500

    if request.method == "PATCH":
        try:
            body = request.get_json(silent=True) or {}
            patch = {}
            for k in (
                "display_name",
                "email",
                "phone",
                "notes",
                "is_active",
                "linked_employee_id",
            ):
                if k in body:
                    patch[k] = body[k]
            if "name" in body and "display_name" not in patch:
                patch["display_name"] = body["name"]
            row = _repo.update(agent_id, tenant_id, patch)
            if not row:
                return jsonify({"success": False, "error": "Agent not found"}), 404
            return (
                jsonify(
                    {
                        "success": True,
                        "agent": _serialize_agent(row),
                        "message": "Agent updated",
                    }
                ),
                200,
            )
        except Exception as e:
            resp = _crm_table_missing_response(e)
            if resp:
                return resp
            return jsonify({"success": False, "error": str(e)}), 500

    # DELETE — soft-delete
    try:
        ok = _repo.soft_delete(agent_id, tenant_id)
        if not ok:
            return jsonify({"success": False, "error": "Agent not found"}), 404
        return jsonify({"success": True, "message": "Agent deactivated"}), 200
    except Exception as e:
        resp = _crm_table_missing_response(e)
        if resp:
            return resp
        return jsonify({"success": False, "error": str(e)}), 500


@agent_bp.route("/<int:agent_id>/properties", methods=["GET", "OPTIONS"])
@token_required
def get_agent_properties(agent_id):
    """Properties assigned to this CRM agent (assigned_crm_agent_id)."""
    if request.method == "OPTIONS":
        return "", 204

    tenant_id = get_current_tenant_id()
    if not tenant_id:
        return (
            jsonify(
                {
                    "success": False,
                    "error": "Invalid tenant context",
                    "message": "tenant_id missing in token or X-Tenant-ID mismatch",
                }
            ),
            403,
        )

    try:
        service = PropertyService()
        properties = service.get_properties_by_crm_agent(agent_id, tenant_id)
        return (
            jsonify(
                {
                    "success": True,
                    "properties": properties,
                    "count": len(properties) if properties else 0,
                }
            ),
            200,
        )
    except PermissionError as e:
        return jsonify({"success": False, "error": str(e)}), 403
    except Exception as e:
        logger.error("Error fetching agent properties: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500


@agent_bp.route("/<int:agent_id>/stats", methods=["GET", "OPTIONS"])
@token_required
def get_agent_stats(agent_id):
    if request.method == "OPTIONS":
        return "", 204

    tenant_id = get_current_tenant_id()
    if not tenant_id:
        return (
            jsonify(
                {
                    "success": False,
                    "error": "Invalid tenant context",
                    "message": "tenant_id missing in token or X-Tenant-ID mismatch",
                }
            ),
            403,
        )

    try:
        service = PropertyService()
        properties = service.get_properties_by_crm_agent(agent_id, tenant_id)
        total_properties = len(properties) if properties else 0
        available = (
            sum(
                1
                for p in properties
                if str(p.get("status_name", "")).lower() == "available"
            )
            if properties
            else 0
        )
        occupied = (
            sum(
                1
                for p in properties
                if str(p.get("status_name", "")).lower() == "occupied"
            )
            if properties
            else 0
        )
        maintenance = (
            sum(
                1
                for p in properties
                if str(p.get("status_name", "")).lower() == "under maintenance"
            )
            if properties
            else 0
        )
        total_income = (
            sum(
                p.get("monthly_rent", 0) or 0
                for p in properties
                if str(p.get("status_name", "")).lower() == "occupied"
            )
            if properties
            else 0
        )
        stats = {
            "agent_id": agent_id,
            "total_properties": total_properties,
            "available": available,
            "occupied": occupied,
            "under_maintenance": maintenance,
            "monthly_income": total_income,
            "occupancy_rate": round((occupied / total_properties * 100), 2)
            if total_properties > 0
            else 0,
        }
        return jsonify({"success": True, "stats": stats}), 200
    except PermissionError as e:
        return jsonify({"success": False, "error": str(e)}), 403
    except Exception as e:
        logger.error("Error fetching agent stats: %s", e)
        return jsonify({"success": False, "error": str(e)}), 500
