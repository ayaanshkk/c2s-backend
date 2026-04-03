# -*- coding: utf-8 -*-
"""
Authentication Helpers
JWT token validation and tenant resolution for property management
"""
from functools import wraps
from flask import request, jsonify, current_app, g
import jwt
import logging

logger = logging.getLogger(__name__)

# ✅ UPDATED: Import from properties module
from backend.properties.models import UserMaster
from backend.properties.db import SessionLocal


def token_required(f):
    """
    Decorator to require valid JWT token (property management aware).
    
    Strategy:
      1. Decode JWT to get employee_id (primary identity claim)
      2. Look up UserMaster by employee_id column
      3. Overlay tenant_id and role from JWT onto the user object
    
    This ensures proper tenant isolation and role-based access control.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.method == 'OPTIONS':
            return jsonify({}), 200

        local_session = SessionLocal()
        try:
            token = None
            if 'Authorization' in request.headers:
                auth_header = request.headers['Authorization']
                try:
                    token = auth_header.split(" ")[1]
                except IndexError:
                    return jsonify({'error': 'Invalid token format'}), 401

            if not token:
                return jsonify({'error': 'Token is missing'}), 401

            try:
                secret_key = current_app.config['SECRET_KEY']
                payload = jwt.decode(token, secret_key, algorithms=['HS256'])

                logger.info("🔐 JWT decoded. Keys: %s", list(payload.keys()))

                # ── Identity resolution ──────────────────────────────────────
                # JWT carries both user_id (User_Master PK) and employee_id.
                # employee_id is the reliable scoping key for all queries.
                employee_id_from_jwt = payload.get('employee_id')
                user_id_from_jwt     = payload.get('user_id')

                user = None

                # ✅ PRIMARY: look up by employee_id
                if employee_id_from_jwt is not None:
                    user = (
                        local_session.query(UserMaster)
                        .filter_by(employee_id=employee_id_from_jwt)
                        .first()
                    )

                # ✅ FALLBACK: look up by User_Master PK for old-format tokens
                if user is None and user_id_from_jwt is not None:
                    user = local_session.get(UserMaster, user_id_from_jwt)

                if user is None:
                    logger.warning(
                        "Auth token valid but UserMaster not found "
                        "(employee_id=%s, user_id=%s)",
                        employee_id_from_jwt, user_id_from_jwt
                    )
                    return jsonify({'error': 'User not found'}), 401

                if not getattr(user, 'is_active', True):
                    return jsonify({'error': 'User not active'}), 401

                # ── Overlay JWT claims onto user object ──────────────────────
                # tenant_id and role always come from JWT (authoritative source)
                user.tenant_id = payload.get('tenant_id')
                if employee_id_from_jwt is not None:
                    user.employee_id = employee_id_from_jwt

                raw_role = payload.get('role')
                # ✅ Preserve original case — don't convert to lowercase
                user.role = str(raw_role).strip() if raw_role else None

                logger.info(
                    "👤 Authenticated: user_id=%s employee_id=%s tenant_id=%s role=%s",
                    getattr(user, 'user_id', None),
                    user.employee_id,
                    user.tenant_id,
                    user.role,
                )

                g.user = user
                request.current_user = user

            except jwt.ExpiredSignatureError:
                return jsonify({'error': 'Token expired'}), 401
            except jwt.InvalidTokenError as e:
                logger.error("Invalid token: %s", e)
                return jsonify({'error': 'Token is invalid or expired'}), 401
            except Exception as e:
                logger.error("Token verification failed: %s", e)
                return jsonify({'error': 'Token verification failed'}), 401

            return f(*args, **kwargs)
        finally:
            # ✅ Guard against connection errors during session close
            try:
                local_session.close()
            except Exception as close_err:
                logger.warning(
                    "Session close failed (stale connection — harmless): %s", 
                    close_err
                )

    return decorated


def get_tenant_id_from_user(user):
    """
    Extract tenant_id from authenticated user object.
    JWT is always the authoritative source (set in token_required above).
    Falls back to Employee_Master lookup for legacy flows.
    
    Args:
        user: Authenticated user object
    
    Returns:
        Tenant ID or None
    """
    if hasattr(user, 'tenant_id') and user.tenant_id is not None:
        return user.tenant_id

    if hasattr(user, 'employee_id') and user.employee_id is not None:
        session = SessionLocal()
        try:
            # ✅ UPDATED: Import from properties models
            from backend.properties.models import Employee_Master
            employee = session.query(Employee_Master).filter_by(
                employee_id=user.employee_id
            ).first()
            return employee.tenant_id if employee else None
        finally:
            try:
                session.close()
            except Exception:
                pass

    return None


def is_admin_user(user) -> bool:
    """
    Check if user has admin privileges
    
    Args:
        user: Authenticated user object
    
    Returns:
        True if user is admin, False otherwise
    """
    if not user or not hasattr(user, 'role'):
        return False
    
    admin_roles = ['Platform Admin', 'Tenant Super Admin', 'Admin']
    return user.role in admin_roles


def require_admin(f):
    """
    Decorator to require admin role
    Must be used after @token_required
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        user = getattr(g, 'user', None)
        
        if not user or not is_admin_user(user):
            return jsonify({
                'error': 'Admin access required',
                'message': 'You do not have permission to perform this action'
            }), 403
        
        return f(*args, **kwargs)
    
    return decorated