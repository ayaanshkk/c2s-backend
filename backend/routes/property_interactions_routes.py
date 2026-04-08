# -*- coding: utf-8 -*-
"""
Property interaction routes — uses dedicated table property_interactions (not Client_Interactions).
"""
from flask import Blueprint, request, jsonify, g
from datetime import datetime
from sqlalchemy import text
from backend.db import SessionLocal
from backend.routes.auth_helpers import token_required, get_current_tenant_id
import logging

logger = logging.getLogger(__name__)

interaction_bp = Blueprint("interactions", __name__, url_prefix="/api/interactions")

SCHEMA = "StreemLyne_MT"
PI = f'"{SCHEMA}"."property_interactions"'


@interaction_bp.after_request
def after_request(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add(
        "Access-Control-Allow-Headers",
        "Content-Type,Authorization,X-Tenant-ID",
    )
    response.headers.add(
        "Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS"
    )
    return response


@interaction_bp.route("/properties/<int:property_id>", methods=["POST", "OPTIONS"])
@token_required
def create_interaction(property_id):
    if request.method == "OPTIONS":
        return jsonify({}), 200

    session = SessionLocal()

    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify(
                {
                    "error": "Invalid tenant context",
                    "message": "tenant_id missing in token or X-Tenant-ID mismatch",
                }
            ), 403

        data = request.get_json() or {}
        interaction_type = data.get("interaction_type")
        if not interaction_type:
            return jsonify({"error": "interaction_type is required"}), 400

        valid_types = ["viewing", "inspection", "note", "maintenance", "callback"]
        if interaction_type not in valid_types:
            return jsonify(
                {
                    "error": f'interaction_type must be one of: {", ".join(valid_types)}',
                }
            ), 400

        property_check = text(
            f"""
            SELECT property_id
            FROM "{SCHEMA}"."Property_Master"
            WHERE property_id = :property_id
              AND tenant_id = :tenant_id
              AND is_deleted = FALSE
            LIMIT 1
        """
        )

        if not session.execute(
            property_check, {"property_id": property_id, "tenant_id": tenant_id}
        ).first():
            return jsonify({"error": "Property not found or access denied"}), 404

        interaction_date = None
        if data.get("interaction_date"):
            try:
                interaction_date = datetime.strptime(
                    data["interaction_date"], "%Y-%m-%d"
                ).date()
            except ValueError:
                return jsonify(
                    {"error": "Invalid interaction_date format. Use YYYY-MM-DD"}
                ), 400

        reminder_date = None
        if data.get("reminder_date"):
            try:
                reminder_date = datetime.strptime(
                    data["reminder_date"], "%Y-%m-%d"
                ).date()
            except ValueError:
                return jsonify(
                    {"error": "Invalid reminder_date format. Use YYYY-MM-DD"}
                ), 400

        employee_id = getattr(g.user, "employee_id", None)

        insert_query = text(
            f"""
            INSERT INTO {PI} (
                tenant_id, property_id, employee_id,
                interaction_type, interaction_date, reminder_date,
                notes, next_steps, contact_method, created_at
            )
            VALUES (
                :tenant_id, :property_id, :employee_id,
                :interaction_type, :interaction_date, :reminder_date,
                :notes, :next_steps, :contact_method, NOW()
            )
            RETURNING interaction_id
        """
        )

        result = session.execute(
            insert_query,
            {
                "tenant_id": tenant_id,
                "property_id": property_id,
                "employee_id": employee_id,
                "interaction_type": interaction_type,
                "interaction_date": interaction_date or datetime.utcnow().date(),
                "reminder_date": reminder_date,
                "notes": data.get("notes", ""),
                "next_steps": data.get("next_steps"),
                "contact_method": 1,
            },
        )
        interaction_id = result.scalar()
        session.commit()

        logger.info("Created %s for property %s", interaction_type, property_id)

        return jsonify(
            {
                "success": True,
                "message": "Interaction created successfully",
                "interaction_id": interaction_id,
            }
        ), 201

    except Exception as e:
        session.rollback()
        logger.exception("Error creating interaction: %s", e)
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()


@interaction_bp.route("/properties/<int:property_id>", methods=["GET", "OPTIONS"])
@token_required
def get_property_interactions(property_id):
    if request.method == "OPTIONS":
        return jsonify({}), 200

    session = SessionLocal()

    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify(
                {
                    "error": "Invalid tenant context",
                    "message": "tenant_id missing in token or X-Tenant-ID mismatch",
                }
            ), 403

        query = text(
            f"""
            SELECT
                pi.interaction_id,
                pi.interaction_type,
                pi.interaction_date,
                pi.reminder_date,
                pi.notes,
                pi.next_steps,
                pi.created_at,
                em.employee_name AS created_by
            FROM {PI} pi
            LEFT JOIN "{SCHEMA}"."Employee_Master" em
                ON pi.employee_id = em.employee_id
            WHERE pi.property_id = :property_id
              AND pi.tenant_id = :tenant_id
            ORDER BY pi.interaction_date DESC NULLS LAST, pi.created_at DESC
        """
        )

        result = session.execute(query, {"property_id": property_id, "tenant_id": tenant_id})

        interactions = []
        for row in result:
            interactions.append(
                {
                    "interaction_id": row.interaction_id,
                    "interaction_type": row.interaction_type,
                    "interaction_date": str(row.interaction_date)
                    if row.interaction_date
                    else None,
                    "reminder_date": str(row.reminder_date)
                    if row.reminder_date
                    else None,
                    "notes": row.notes,
                    "next_steps": row.next_steps,
                    "created_at": row.created_at.isoformat()
                    if row.created_at
                    else None,
                    "created_by": row.created_by,
                }
            )

        return jsonify(
            {
                "success": True,
                "interactions": interactions,
                "count": len(interactions),
            }
        ), 200

    except Exception as e:
        logger.exception("Error fetching interactions: %s", e)
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()


@interaction_bp.route("/<int:interaction_id>", methods=["DELETE", "OPTIONS"])
@token_required
def delete_interaction(interaction_id):
    if request.method == "OPTIONS":
        return jsonify({}), 200

    session = SessionLocal()

    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify(
                {
                    "error": "Invalid tenant context",
                    "message": "tenant_id missing in token or X-Tenant-ID mismatch",
                }
            ), 403

        delete_query = text(
            f"""
            DELETE FROM {PI}
            WHERE interaction_id = :interaction_id
              AND tenant_id = :tenant_id
            RETURNING interaction_id
        """
        )

        result = session.execute(
            delete_query,
            {"interaction_id": interaction_id, "tenant_id": tenant_id},
        )
        deleted_id = result.scalar()

        if not deleted_id:
            return jsonify({"error": "Interaction not found or access denied"}), 404

        session.commit()
        return jsonify(
            {"success": True, "message": "Interaction deleted successfully"}
        ), 200

    except Exception as e:
        session.rollback()
        logger.exception("Error deleting interaction: %s", e)
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()


@interaction_bp.route("/<int:interaction_id>", methods=["PUT", "OPTIONS"])
@token_required
def update_interaction(interaction_id):
    if request.method == "OPTIONS":
        return jsonify({}), 200

    session = SessionLocal()

    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify(
                {
                    "error": "Invalid tenant context",
                    "message": "tenant_id missing in token or X-Tenant-ID mismatch",
                }
            ), 403

        data = request.get_json() or {}
        update_fields = []
        params = {"interaction_id": interaction_id, "tenant_id": tenant_id}

        if "notes" in data:
            update_fields.append("notes = :notes")
            params["notes"] = data["notes"]

        if "next_steps" in data:
            update_fields.append("next_steps = :next_steps")
            params["next_steps"] = data["next_steps"]

        if "reminder_date" in data:
            if data["reminder_date"]:
                try:
                    params["reminder_date"] = datetime.strptime(
                        data["reminder_date"], "%Y-%m-%d"
                    ).date()
                    update_fields.append("reminder_date = :reminder_date")
                except ValueError:
                    return jsonify({"error": "Invalid reminder_date format"}), 400
            else:
                update_fields.append("reminder_date = NULL")

        if not update_fields:
            return jsonify({"error": "No fields to update"}), 400

        update_query = text(
            f"""
            UPDATE {PI}
            SET {", ".join(update_fields)}
            WHERE interaction_id = :interaction_id
              AND tenant_id = :tenant_id
            RETURNING interaction_id
        """
        )

        result = session.execute(update_query, params)
        updated_id = result.scalar()

        if not updated_id:
            return jsonify({"error": "Interaction not found or access denied"}), 404

        session.commit()
        return jsonify(
            {"success": True, "message": "Interaction updated successfully"}
        ), 200

    except Exception as e:
        session.rollback()
        logger.exception("Error updating interaction: %s", e)
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()
