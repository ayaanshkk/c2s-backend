# -*- coding: utf-8 -*-
"""
Property CRM: expenses, invoices, mortgages, lease agreements (per property, tenant-scoped).
Requires migration 003_property_crm_financials.sql
"""
import logging
from datetime import date, datetime
from decimal import Decimal
from flask import Blueprint, request, jsonify
from sqlalchemy import text
from backend.db import SessionLocal
from backend.routes.auth_helpers import token_required, get_current_tenant_id

logger = logging.getLogger(__name__)

property_crm_bp = Blueprint("property_crm", __name__, url_prefix="/api/pcrm")
SCHEMA = "StreemLyne_MT"


def _clean_row(r) -> dict:
    d = dict(r)
    for k, v in list(d.items()):
        if isinstance(v, Decimal):
            d[k] = float(v)
        elif isinstance(v, (date, datetime)):
            d[k] = v.isoformat()
    return d


def _tenant_or_403():
    tid = get_current_tenant_id()
    if not tid:
        return None, (
            jsonify(
                {
                    "success": False,
                    "error": "Invalid tenant context",
                    "message": "tenant_id missing in token or X-Tenant-ID mismatch",
                }
            ),
            403,
        )
    return tid, None


def _property_belongs(session, property_id: int, tenant_id: str) -> bool:
    row = session.execute(
        text(
            f"""
        SELECT 1 FROM "{SCHEMA}"."Property_Master"
        WHERE property_id = :pid AND tenant_id = :tid AND is_deleted = FALSE
        LIMIT 1
    """
        ),
        {"pid": property_id, "tid": tenant_id},
    ).first()
    return row is not None


def _db_error_response(e: Exception):
    msg = str(e)
    if "does not exist" in msg.lower() or "undefinedtable" in msg.lower():
        return jsonify(
            {
                "success": False,
                "error": "CRM tables not installed",
                "message": "Run migrations/003_property_crm_financials.sql in Supabase.",
            }
        ), 503
    logger.exception("property_crm DB error: %s", e)
    return jsonify({"success": False, "error": msg}), 500


# --- Expenses ---


@property_crm_bp.route("/expenses", methods=["GET", "OPTIONS"])
@token_required
def list_expenses():
    if request.method == "OPTIONS":
        return "", 204
    tenant_id, err = _tenant_or_403()
    if err:
        return err
    prop_filter = request.args.get("property_id", type=int)
    session = SessionLocal()
    try:
        if prop_filter is not None:
            if not _property_belongs(session, prop_filter, tenant_id):
                return jsonify({"success": False, "error": "Property not found"}), 404
            rows = session.execute(
                text(
                    f"""
                SELECT * FROM "{SCHEMA}"."property_expenses"
                WHERE tenant_id = :tid AND property_id = :pid
                ORDER BY incurred_date DESC NULLS LAST, created_at DESC
            """
                ),
                {"tid": tenant_id, "pid": prop_filter},
            ).mappings().all()
        else:
            rows = session.execute(
                text(
                    f"""
                SELECT * FROM "{SCHEMA}"."property_expenses"
                WHERE tenant_id = :tid
                ORDER BY created_at DESC
                LIMIT 500
            """
                ),
                {"tid": tenant_id},
            ).mappings().all()
        return jsonify({"success": True, "expenses": [_clean_row(r) for r in rows]}), 200
    except Exception as e:
        return _db_error_response(e)
    finally:
        session.close()


@property_crm_bp.route("/expenses", methods=["POST", "OPTIONS"])
@token_required
def create_expense():
    if request.method == "OPTIONS":
        return "", 204
    tenant_id, err = _tenant_or_403()
    if err:
        return err
    data = request.get_json() or {}
    pid = data.get("property_id")
    if pid is None:
        return jsonify({"success": False, "error": "property_id required"}), 400
    session = SessionLocal()
    try:
        if not _property_belongs(session, int(pid), tenant_id):
            return jsonify({"success": False, "error": "Property not found"}), 404
        session.execute(
            text(
                f"""
            INSERT INTO "{SCHEMA}"."property_expenses"
            (tenant_id, property_id, category, amount, incurred_date, description)
            VALUES (:tid, :pid, :cat, :amt, :idate, :desc)
        """
            ),
            {
                "tid": tenant_id,
                "pid": int(pid),
                "cat": data.get("category") or "General",
                "amt": float(data.get("amount") or 0),
                "idate": data.get("incurred_date") or None,
                "desc": data.get("description"),
            },
        )
        session.commit()
        return jsonify({"success": True, "message": "Expense added"}), 201
    except Exception as e:
        session.rollback()
        return _db_error_response(e)
    finally:
        session.close()


@property_crm_bp.route("/expenses/<int:expense_id>", methods=["DELETE", "OPTIONS"])
@token_required
def delete_expense(expense_id):
    if request.method == "OPTIONS":
        return "", 204
    tenant_id, err = _tenant_or_403()
    if err:
        return err
    session = SessionLocal()
    try:
        r = session.execute(
            text(
                f"""
            DELETE FROM "{SCHEMA}"."property_expenses"
            WHERE expense_id = :eid AND tenant_id = :tid
            RETURNING expense_id
        """
            ),
            {"eid": expense_id, "tid": tenant_id},
        ).first()
        session.commit()
        if not r:
            return jsonify({"success": False, "error": "Not found"}), 404
        return jsonify({"success": True}), 200
    except Exception as e:
        session.rollback()
        return _db_error_response(e)
    finally:
        session.close()


# --- Invoices ---


@property_crm_bp.route("/invoices", methods=["GET", "OPTIONS"])
@token_required
def list_invoices():
    if request.method == "OPTIONS":
        return "", 204
    tenant_id, err = _tenant_or_403()
    if err:
        return err
    prop_filter = request.args.get("property_id", type=int)
    session = SessionLocal()
    try:
        if prop_filter is not None:
            if not _property_belongs(session, prop_filter, tenant_id):
                return jsonify({"success": False, "error": "Property not found"}), 404
            rows = session.execute(
                text(
                    f"""
                SELECT * FROM "{SCHEMA}"."property_invoices"
                WHERE tenant_id = :tid AND property_id = :pid
                ORDER BY due_date DESC NULLS LAST, created_at DESC
            """
                ),
                {"tid": tenant_id, "pid": prop_filter},
            ).mappings().all()
        else:
            rows = session.execute(
                text(
                    f"""
                SELECT * FROM "{SCHEMA}"."property_invoices"
                WHERE tenant_id = :tid
                ORDER BY created_at DESC
                LIMIT 500
            """
                ),
                {"tid": tenant_id},
            ).mappings().all()
        return jsonify({"success": True, "invoices": [_clean_row(r) for r in rows]}), 200
    except Exception as e:
        return _db_error_response(e)
    finally:
        session.close()


@property_crm_bp.route("/invoices", methods=["POST", "OPTIONS"])
@token_required
def create_invoice():
    if request.method == "OPTIONS":
        return "", 204
    tenant_id, err = _tenant_or_403()
    if err:
        return err
    data = request.get_json() or {}
    pid = data.get("property_id")
    if pid is None:
        return jsonify({"success": False, "error": "property_id required"}), 400
    session = SessionLocal()
    try:
        if not _property_belongs(session, int(pid), tenant_id):
            return jsonify({"success": False, "error": "Property not found"}), 404
        session.execute(
            text(
                f"""
            INSERT INTO "{SCHEMA}"."property_invoices"
            (tenant_id, property_id, invoice_number, amount, due_date, status, document_url, notes)
            VALUES (:tid, :pid, :inum, :amt, :dd, :st, :doc, :notes)
        """
            ),
            {
                "tid": tenant_id,
                "pid": int(pid),
                "inum": data.get("invoice_number"),
                "amt": float(data.get("amount") or 0),
                "dd": data.get("due_date") or None,
                "st": data.get("status") or "draft",
                "doc": data.get("document_url"),
                "notes": data.get("notes"),
            },
        )
        session.commit()
        return jsonify({"success": True, "message": "Invoice created"}), 201
    except Exception as e:
        session.rollback()
        return _db_error_response(e)
    finally:
        session.close()


@property_crm_bp.route("/invoices/<int:invoice_id>", methods=["PATCH", "OPTIONS"])
@token_required
def patch_invoice(invoice_id):
    if request.method == "OPTIONS":
        return "", 204
    tenant_id, err = _tenant_or_403()
    if err:
        return err
    data = request.get_json() or {}
    session = SessionLocal()
    try:
        sets, params = [], {"iid": invoice_id, "tid": tenant_id}
        for col in ("invoice_number", "amount", "due_date", "status", "document_url", "notes"):
            if col in data:
                sets.append(f'"{col}" = :{col}')
                val = data[col]
                if col == "amount" and val is not None:
                    val = float(val)
                params[col] = val
        if not sets:
            return jsonify({"success": False, "error": "No fields to update"}), 400
        q = f'UPDATE "{SCHEMA}"."property_invoices" SET {", ".join(sets)} WHERE invoice_id = :iid AND tenant_id = :tid'
        r = session.execute(text(q), params)
        session.commit()
        if (r.rowcount or 0) == 0:
            return jsonify({"success": False, "error": "Not found"}), 404
        return jsonify({"success": True}), 200
    except Exception as e:
        session.rollback()
        return _db_error_response(e)
    finally:
        session.close()


@property_crm_bp.route("/invoices/<int:invoice_id>", methods=["DELETE", "OPTIONS"])
@token_required
def delete_invoice(invoice_id):
    if request.method == "OPTIONS":
        return "", 204
    tenant_id, err = _tenant_or_403()
    if err:
        return err
    session = SessionLocal()
    try:
        r = session.execute(
            text(
                f"""
            DELETE FROM "{SCHEMA}"."property_invoices"
            WHERE invoice_id = :iid AND tenant_id = :tid
            RETURNING invoice_id
        """
            ),
            {"iid": invoice_id, "tid": tenant_id},
        ).first()
        session.commit()
        if not r:
            return jsonify({"success": False, "error": "Not found"}), 404
        return jsonify({"success": True}), 200
    except Exception as e:
        session.rollback()
        return _db_error_response(e)
    finally:
        session.close()


# --- Mortgages ---


@property_crm_bp.route("/mortgages", methods=["GET", "OPTIONS"])
@token_required
def list_mortgages():
    if request.method == "OPTIONS":
        return "", 204
    tenant_id, err = _tenant_or_403()
    if err:
        return err
    prop_filter = request.args.get("property_id", type=int)
    session = SessionLocal()
    try:
        if prop_filter is not None:
            if not _property_belongs(session, prop_filter, tenant_id):
                return jsonify({"success": False, "error": "Property not found"}), 404
            rows = session.execute(
                text(
                    f"""
                SELECT * FROM "{SCHEMA}"."property_mortgages"
                WHERE tenant_id = :tid AND property_id = :pid
                ORDER BY created_at DESC
            """
                ),
                {"tid": tenant_id, "pid": prop_filter},
            ).mappings().all()
        else:
            rows = session.execute(
                text(
                    f"""
                SELECT * FROM "{SCHEMA}"."property_mortgages"
                WHERE tenant_id = :tid
                ORDER BY created_at DESC
                LIMIT 200
            """
                ),
                {"tid": tenant_id},
            ).mappings().all()
        return jsonify({"success": True, "mortgages": [_clean_row(r) for r in rows]}), 200
    except Exception as e:
        return _db_error_response(e)
    finally:
        session.close()


@property_crm_bp.route("/mortgages", methods=["POST", "OPTIONS"])
@token_required
def create_mortgage():
    if request.method == "OPTIONS":
        return "", 204
    tenant_id, err = _tenant_or_403()
    if err:
        return err
    data = request.get_json() or {}
    pid = data.get("property_id")
    if pid is None:
        return jsonify({"success": False, "error": "property_id required"}), 400
    session = SessionLocal()
    try:
        if not _property_belongs(session, int(pid), tenant_id):
            return jsonify({"success": False, "error": "Property not found"}), 404
        session.execute(
            text(
                f"""
            INSERT INTO "{SCHEMA}"."property_mortgages"
            (tenant_id, property_id, lender, principal, monthly_payment, rate_percent, start_date, end_date, document_url, notes)
            VALUES (:tid, :pid, :lend, :prin, :mpay, :rate, :sd, :ed, :doc, :notes)
        """
            ),
            {
                "tid": tenant_id,
                "pid": int(pid),
                "lend": data.get("lender"),
                "prin": data.get("principal"),
                "mpay": data.get("monthly_payment"),
                "rate": data.get("rate_percent"),
                "sd": data.get("start_date"),
                "ed": data.get("end_date"),
                "doc": data.get("document_url"),
                "notes": data.get("notes"),
            },
        )
        session.commit()
        return jsonify({"success": True, "message": "Mortgage record added"}), 201
    except Exception as e:
        session.rollback()
        return _db_error_response(e)
    finally:
        session.close()


@property_crm_bp.route("/mortgages/<int:mortgage_id>", methods=["PATCH", "OPTIONS"])
@token_required
def patch_mortgage(mortgage_id):
    if request.method == "OPTIONS":
        return "", 204
    tenant_id, err = _tenant_or_403()
    if err:
        return err
    data = request.get_json() or {}
    session = SessionLocal()
    try:
        sets, params = [], {"mid": mortgage_id, "tid": tenant_id}
        for col in (
            "lender",
            "principal",
            "monthly_payment",
            "rate_percent",
            "start_date",
            "end_date",
            "document_url",
            "notes",
        ):
            if col in data:
                sets.append(f'"{col}" = :{col}')
                params[col] = data[col]
        if not sets:
            return jsonify({"success": False, "error": "No fields to update"}), 400
        q = f'UPDATE "{SCHEMA}"."property_mortgages" SET {", ".join(sets)} WHERE mortgage_id = :mid AND tenant_id = :tid'
        r = session.execute(text(q), params)
        session.commit()
        if (r.rowcount or 0) == 0:
            return jsonify({"success": False, "error": "Not found"}), 404
        return jsonify({"success": True}), 200
    except Exception as e:
        session.rollback()
        return _db_error_response(e)
    finally:
        session.close()


@property_crm_bp.route("/mortgages/<int:mortgage_id>", methods=["DELETE", "OPTIONS"])
@token_required
def delete_mortgage(mortgage_id):
    if request.method == "OPTIONS":
        return "", 204
    tenant_id, err = _tenant_or_403()
    if err:
        return err
    session = SessionLocal()
    try:
        r = session.execute(
            text(
                f"""
            DELETE FROM "{SCHEMA}"."property_mortgages"
            WHERE mortgage_id = :mid AND tenant_id = :tid
            RETURNING mortgage_id
        """
            ),
            {"mid": mortgage_id, "tid": tenant_id},
        ).first()
        session.commit()
        if not r:
            return jsonify({"success": False, "error": "Not found"}), 404
        return jsonify({"success": True}), 200
    except Exception as e:
        session.rollback()
        return _db_error_response(e)
    finally:
        session.close()


# --- Lease agreements ---


@property_crm_bp.route("/lease-agreements", methods=["GET", "OPTIONS"])
@token_required
def list_lease_agreements():
    if request.method == "OPTIONS":
        return "", 204
    tenant_id, err = _tenant_or_403()
    if err:
        return err
    prop_filter = request.args.get("property_id", type=int)
    session = SessionLocal()
    try:
        if prop_filter is not None:
            if not _property_belongs(session, prop_filter, tenant_id):
                return jsonify({"success": False, "error": "Property not found"}), 404
            rows = session.execute(
                text(
                    f"""
                SELECT * FROM "{SCHEMA}"."property_lease_agreements"
                WHERE tenant_id = :tid AND property_id = :pid
                ORDER BY start_date DESC NULLS LAST, created_at DESC
            """
                ),
                {"tid": tenant_id, "pid": prop_filter},
            ).mappings().all()
        else:
            rows = session.execute(
                text(
                    f"""
                SELECT * FROM "{SCHEMA}"."property_lease_agreements"
                WHERE tenant_id = :tid
                ORDER BY created_at DESC
                LIMIT 200
            """
                ),
                {"tid": tenant_id},
            ).mappings().all()
        return jsonify({"success": True, "agreements": [_clean_row(r) for r in rows]}), 200
    except Exception as e:
        return _db_error_response(e)
    finally:
        session.close()


@property_crm_bp.route("/lease-agreements", methods=["POST", "OPTIONS"])
@token_required
def create_lease_agreement():
    if request.method == "OPTIONS":
        return "", 204
    tenant_id, err = _tenant_or_403()
    if err:
        return err
    data = request.get_json() or {}
    pid = data.get("property_id")
    if pid is None:
        return jsonify({"success": False, "error": "property_id required"}), 400
    session = SessionLocal()
    try:
        if not _property_belongs(session, int(pid), tenant_id):
            return jsonify({"success": False, "error": "Property not found"}), 404
        session.execute(
            text(
                f"""
            INSERT INTO "{SCHEMA}"."property_lease_agreements"
            (tenant_id, property_id, tenant_display_name, title, start_date, end_date, rent_amount, document_url, notes)
            VALUES (:tid, :pid, :tdn, :title, :sd, :ed, :rent, :doc, :notes)
        """
            ),
            {
                "tid": tenant_id,
                "pid": int(pid),
                "tdn": data.get("tenant_display_name"),
                "title": data.get("title") or "Lease agreement",
                "sd": data.get("start_date"),
                "ed": data.get("end_date"),
                "rent": data.get("rent_amount"),
                "doc": data.get("document_url"),
                "notes": data.get("notes"),
            },
        )
        session.commit()
        return jsonify({"success": True, "message": "Agreement created"}), 201
    except Exception as e:
        session.rollback()
        return _db_error_response(e)
    finally:
        session.close()


@property_crm_bp.route("/lease-agreements/<int:agreement_id>", methods=["PATCH", "OPTIONS"])
@token_required
def patch_lease_agreement(agreement_id):
    if request.method == "OPTIONS":
        return "", 204
    tenant_id, err = _tenant_or_403()
    if err:
        return err
    data = request.get_json() or {}
    session = SessionLocal()
    try:
        sets, params = [], {"aid": agreement_id, "tid": tenant_id}
        for col in (
            "tenant_display_name",
            "title",
            "start_date",
            "end_date",
            "rent_amount",
            "document_url",
            "notes",
        ):
            if col in data:
                sets.append(f'"{col}" = :{col}')
                val = data[col]
                if col == "rent_amount" and val is not None:
                    val = float(val)
                params[col] = val
        if not sets:
            return jsonify({"success": False, "error": "No fields to update"}), 400
        sets.append('"updated_at" = NOW()')
        q = f'UPDATE "{SCHEMA}"."property_lease_agreements" SET {", ".join(sets)} WHERE agreement_id = :aid AND tenant_id = :tid'
        r = session.execute(text(q), params)
        session.commit()
        if (r.rowcount or 0) == 0:
            return jsonify({"success": False, "error": "Not found"}), 404
        return jsonify({"success": True}), 200
    except Exception as e:
        session.rollback()
        return _db_error_response(e)
    finally:
        session.close()


@property_crm_bp.route("/lease-agreements/<int:agreement_id>", methods=["DELETE", "OPTIONS"])
@token_required
def delete_lease_agreement(agreement_id):
    if request.method == "OPTIONS":
        return "", 204
    tenant_id, err = _tenant_or_403()
    if err:
        return err
    session = SessionLocal()
    try:
        r = session.execute(
            text(
                f"""
            DELETE FROM "{SCHEMA}"."property_lease_agreements"
            WHERE agreement_id = :aid AND tenant_id = :tid
            RETURNING agreement_id
        """
            ),
            {"aid": agreement_id, "tid": tenant_id},
        ).first()
        session.commit()
        if not r:
            return jsonify({"success": False, "error": "Not found"}), 404
        return jsonify({"success": True}), 200
    except Exception as e:
        session.rollback()
        return _db_error_response(e)
    finally:
        session.close()
