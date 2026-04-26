# -*- coding: utf-8 -*-
"""
Authentication Routes
Login, logout, user management for property management system
"""
from flask import Blueprint, request, jsonify, current_app, g
from backend.models import UserMaster, Employee_Master
from backend.routes.auth_helpers import (
    token_required,
    require_admin,
    normalize_tenant_id,
    get_current_tenant_id,
)
from datetime import datetime, timedelta
from functools import wraps
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
import secrets
import re
import jwt
import logging
import os

from backend.db import SessionLocal

logger = logging.getLogger(__name__)

auth_bp = Blueprint('auth', __name__)

# --- Configuration and Helpers ---

def get_client_ip():
    """Get client IP address"""
    if request.environ.get('HTTP_X_FORWARDED_FOR') is None:
        return request.environ['REMOTE_ADDR']
    else:
        return request.environ['HTTP_X_FORWARDED_FOR']

def validate_email(email):
    """Validate email format"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def validate_password(password):
    """Validate password strength"""
    if len(password) < 6:
        return False, "Password must be at least 6 characters long"
    return True, "Password is valid"

def get_tenant_id_from_token():
    """Read tenant_id directly from the JWT"""
    try:
        token = request.headers.get('Authorization', '').split(' ')[1]
        payload = jwt.decode(token, current_app.config['SECRET_KEY'], algorithms=['HS256'])
        return payload.get('tenant_id')
    except Exception:
        return None


def get_allowed_login_tenant_id():
    """Tenant id allowed to sign in to this app instance."""
    return normalize_tenant_id(
        current_app.config.get('LOGIN_ALLOWED_TENANT_ID')
        or os.getenv('LOGIN_ALLOWED_TENANT_ID')
        or '5'
    )

# --- Routes ---

@auth_bp.route('/health', methods=['GET'])
def health_check():
    return {
        'status': 'ok', 
        'message': 'Property Management Backend is running!'
    }, 200

@auth_bp.route('/login', methods=['POST'])
def login():
    """
    Property management login against StreemLyne_MT tables.
    
    Expected JSON: { "username", "password" }
    Returns JWT: { user_id, employee_id, tenant_id, user_name, role }
    """
    import time
    start_time = time.time()
    
    session = SessionLocal()
    try:
        # ===== 1. PARSE REQUEST =====
        data = request.get_json() or {}
        data.pop('tenant_id', None)

        if not data.get('username') or not data.get('password'):
            return jsonify({'error': 'username and password required'}), 400

        username = data['username'].strip()
        input_password = data['password']
        
        logger.info(f"🔐 Login attempt: {username}")

        # ===== 2. FETCH USER + EMPLOYEE =====
        user_sql = text('''
            SELECT
                u.user_id,
                u.user_name,
                u.password,
                u.employee_id,
                e.tenant_id,
                e.employee_name,
                e.email,
                e.phone
            FROM "StreemLyne_MT"."User_Master" u
            JOIN "StreemLyne_MT"."Employee_Master" e ON u.employee_id = e.employee_id
            WHERE u.user_name = :username
            LIMIT 1;
        ''')

        row = session.execute(user_sql, {'username': username}).mappings().first()

        if not row:
            logger.warning(f"❌ User not found: {username}")
            return jsonify({'error': 'Invalid username or password'}), 401

        # ===== 3. VERIFY PASSWORD =====
        db_password = row.get('password')
        if db_password != input_password:
            logger.warning(f"❌ Invalid password for {username}")
            return jsonify({'error': 'Invalid username or password'}), 401

        # ===== 4. FETCH ROLE =====
        role_sql = text('''
            SELECT rm.role_name, rm.role_id
            FROM "StreemLyne_MT"."User_Role_Mapping" urm
            JOIN "StreemLyne_MT"."Role_Master" rm ON urm.role_id = rm.role_id
            WHERE urm.user_id = :user_id
            LIMIT 1;
        ''')
        
        role_row = session.execute(role_sql, {'user_id': row['user_id']}).mappings().first()

        # ===== 5. GENERATE JWT ===== (tenant_id always string — never from client for authz)
        tenant_slug = normalize_tenant_id(row.get('tenant_id'))
        allowed_tenant_id = get_allowed_login_tenant_id()
        if not tenant_slug or (allowed_tenant_id and tenant_slug != allowed_tenant_id):
            logger.warning(
                "Login blocked due to tenant restriction | user=%s user_tenant=%s allowed_tenant=%s",
                username,
                tenant_slug,
                allowed_tenant_id,
            )
            return jsonify({'error': 'Invalid username or password'}), 401

        payload = {
            'user_id': row.get('user_id'),
            'employee_id': row.get('employee_id'),
            'tenant_id': tenant_slug,
            'user_name': row.get('user_name'),
            'role': role_row['role_name'] if role_row else None,
            'exp': datetime.utcnow() + timedelta(days=7),
            'iat': datetime.utcnow()
        }
        
        token = jwt.encode(payload, current_app.config['SECRET_KEY'], algorithm='HS256')

        # ===== 6. BUILD RESPONSE =====
        user = {
            'employee_id': row.get('employee_id'),
            'id': row.get('employee_id'),
            'name': (row.get('employee_name') or row.get('user_name')),
            'email': row.get('email'),
            'phone': row.get('phone'),
            'username': row.get('user_name'),
            'role': role_row['role_name'] if role_row else None,
            'role_id': role_row['role_id'] if role_row else None,
            'tenant_id': tenant_slug
        }

        total_time = time.time() - start_time
        logger.info(
            f"✅ LOGIN SUCCESS | user={username} tenant={row.get('tenant_id')} "
            f"role={user['role']} | Total: {total_time*1000:.0f}ms"
        )
        
        return jsonify({'success': True, 'token': token, 'user': user}), 200

    except Exception as e:
        logger.error(f"❌ LOGIN ERROR: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({'error': 'Internal server error'}), 500
    finally:
        session.close()

@auth_bp.route('/signup', methods=['POST'])
@token_required
@require_admin
def signup():
    """
    Create user under the admin's tenant (tenant_id from JWT only — never from body).
    Expected JSON: { "employee_name", "email", "username", "password", "phone"? }
    """
    session = SessionLocal()
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'error': 'Invalid tenant context',
                'message': 'tenant_id missing in token or X-Tenant-ID mismatch',
            }), 403

        data = request.get_json() or {}
        data.pop('tenant_id', None)
        required = ['employee_name', 'email', 'username', 'password']
        for f in required:
            if not data.get(f):
                return jsonify({'error': f'{f} is required'}), 400
        employee_name = data.get('employee_name').strip()
        email = data.get('email').strip()
        phone = data.get('phone')
        username = data.get('username').strip()
        password = data.get('password')

        # Validate email
        if not validate_email(email):
            return jsonify({'error': 'Invalid email format'}), 400

        # Validate password
        is_valid, message = validate_password(password)
        if not is_valid:
            return jsonify({'error': message}), 400

        # Uniqueness checks
        q_user_exists = text('SELECT 1 FROM "StreemLyne_MT"."User_Master" WHERE user_name = :username LIMIT 1')
        if session.execute(q_user_exists, {'username': username}).first():
            return jsonify({'error': 'username already exists'}), 400

        q_email_exists = text('SELECT 1 FROM "StreemLyne_MT"."Employee_Master" WHERE email = :email LIMIT 1')
        if session.execute(q_email_exists, {'email': email}).first():
            return jsonify({'error': 'email already exists'}), 400

        # Insert employee
        insert_emp = text('''
            INSERT INTO "StreemLyne_MT"."Employee_Master" (tenant_id, employee_name, email, phone)
            VALUES (:tenant_id, :employee_name, :email, :phone)
            RETURNING employee_id
        ''')
        emp_row = session.execute(insert_emp, {
            'tenant_id': tenant_id,
            'employee_name': employee_name,
            'email': email,
            'phone': phone
        }).mappings().first()

        if not emp_row or not emp_row.get('employee_id'):
            session.rollback()
            logger.error('Failed to create Employee_Master row')
            return jsonify({'error': 'Could not create employee'}), 500

        employee_id = emp_row.get('employee_id')

        # Insert user
        insert_user = text('''
            INSERT INTO "StreemLyne_MT"."User_Master" (employee_id, user_name, password)
            VALUES (:employee_id, :user_name, :password)
            RETURNING user_id
        ''')
        user_row = session.execute(insert_user, {
            'employee_id': employee_id,
            'user_name': username,
            'password': password
        }).mappings().first()

        if not user_row or not user_row.get('user_id'):
            session.rollback()
            logger.error('Failed to create User_Master row')
            return jsonify({'error': 'Could not create user'}), 500

        user_id = user_row.get('user_id')

        session.commit()

        # Build JWT
        payload = {
            'user_id': user_id,
            'employee_id': employee_id,
            'tenant_id': tenant_id,
            'user_name': username,
            'exp': datetime.utcnow() + timedelta(days=7),
            'iat': datetime.utcnow()
        }
        token = jwt.encode(payload, current_app.config['SECRET_KEY'], algorithm='HS256')

        user_out = {
            'user_id': user_id,
            'employee_id': employee_id,
            'user_name': username,
            'tenant_id': str(tenant_id),
            'email': email,
            'name': employee_name
        }

        logger.info(f"✅ Signup successful: user_id={user_id} user_name={username} tenant_id={tenant_id}")
        return jsonify({'success': True, 'message': 'Signup successful', 'token': token, 'user': user_out}), 201

    except IntegrityError as ie:
        session.rollback()
        msg = str(ie.orig) if hasattr(ie, 'orig') else 'Integrity error'
        logger.warning(f"Signup integrity error: {msg}")
        return jsonify({'error': 'username or email already exists'}), 400
    except Exception as e:
        session.rollback()
        logger.exception(f"❌ Signup error: {e}")
        return jsonify({'error': 'Internal server error'}), 500
    finally:
        session.close()

@auth_bp.route('/logout', methods=['POST'])
@token_required
def logout():
    """Stateless logout: JWT-only so simply acknowledge the request"""
    try:
        return jsonify({'message': 'Logged out successfully'}), 200
    except Exception as e:
        logger.exception(f"Error during logout: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@auth_bp.route('/me', methods=['GET'])
@token_required
def get_current_user():
    """Get current user information"""
    try:
        user = g.user
        
        user_data = {
            'user_id': getattr(user, 'user_id', None),
            'employee_id': getattr(user, 'employee_id', None),
            'user_name': getattr(user, 'user_name', None),
            'email': getattr(user, 'email', None),
            'role': getattr(user, 'role', None),
            'tenant_id': getattr(user, 'tenant_id', None),
        }
        
        return jsonify({'user': user_data}), 200
    except Exception as e:
        logger.exception(f"Error in /auth/me: {e}")
        return jsonify({'error': 'Internal server error'}), 500

@auth_bp.route('/change-password', methods=['POST'])
@token_required
def change_password():
    """Change user password"""
    session = SessionLocal()
    
    try:
        data = request.get_json()
        current_password = data.get('current_password')
        new_password = data.get('new_password')
        
        if not current_password or not new_password:
            return jsonify({'error': 'Current and new password are required'}), 400
        
        is_valid, message = validate_password(new_password)
        if not is_valid:
            return jsonify({'error': message}), 400
        
        user = g.user
        user_id = user.user_id
        
        # Verify current password
        sql = text('''
            SELECT password 
            FROM "StreemLyne_MT"."User_Master" 
            WHERE user_id = :user_id 
            LIMIT 1
        ''')
        
        row = session.execute(sql, {'user_id': user_id}).mappings().first()
        
        if not row or row['password'] != current_password:
            return jsonify({'error': 'Current password is incorrect'}), 401
        
        # Update password
        update_sql = text('''
            UPDATE "StreemLyne_MT"."User_Master"
            SET password = :password
            WHERE user_id = :user_id
        ''')
        
        session.execute(update_sql, {
            'password': new_password,
            'user_id': user_id
        })
        
        session.commit()
        
        logger.info(f"✅ Password changed successfully for user_id: {user_id}")
        
        return jsonify({
            'message': 'Password changed successfully'
        }), 200
        
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Error changing password: {str(e)}")
        return jsonify({'error': 'Failed to change password'}), 500
        
    finally:
        session.close()

@auth_bp.route('/refresh', methods=['POST'])
@token_required
def refresh_token():
    """Refresh JWT token"""
    try:
        user = g.user

        # Get old token payload
        old_token = request.headers.get('Authorization').split(" ")[1]
        old_payload = {}
        try:
            old_payload = jwt.decode(
                old_token, 
                current_app.config['SECRET_KEY'], 
                algorithms=['HS256'], 
                options={'verify_exp': False}
            )
        except Exception:
            old_payload = {}

        employee_id = getattr(user, 'employee_id', None) or old_payload.get('employee_id')
        tenant_id = normalize_tenant_id(
            old_payload.get('tenant_id') or getattr(user, 'tenant_id', None)
        )
        user_name = old_payload.get('user_name') or getattr(user, 'user_name', None)
        role = old_payload.get('role') or getattr(user, 'role', None)

        payload = {
            'user_id': getattr(user, 'user_id', None),
            'employee_id': employee_id,
            'user_name': user_name,
            'tenant_id': tenant_id,
            'role': role,
            'exp': datetime.utcnow() + timedelta(days=7),
            'iat': datetime.utcnow()
        }
        
        # Remove None values
        payload = {k: v for k, v in payload.items() if v is not None}
        new_token = jwt.encode(payload, current_app.config['SECRET_KEY'], algorithm='HS256')

        return jsonify({'token': new_token}), 200
        
    except Exception as e:
        logger.exception(f"Error refreshing token: {e}")
        return jsonify({'error': str(e)}), 500
    
@auth_bp.route('/verify-invite/<token>', methods=['GET'])
def verify_invite_token(token):
    """Verify if an invite token is valid"""
    session = SessionLocal()
    try:
        # Check if token exists and is still pending
        verify_sql = text('''
            SELECT 
                um.user_id,
                um.employee_id,
                um.is_invite_pending,
                em.employee_name,
                em.email,
                em.phone,
                em.tenant_id
            FROM "StreemLyne_MT"."User_Master" um
            JOIN "StreemLyne_MT"."Employee_Master" em 
                ON um.employee_id = em.employee_id
            WHERE um.invite_token = :token
            AND um.is_invite_pending = TRUE
            LIMIT 1
        ''')
        
        result = session.execute(verify_sql, {'token': token}).mappings().first()
        
        if not result:
            return jsonify({
                'success': False,
                'error': 'Invalid or expired invite token'
            }), 404
        
        return jsonify({
            'success': True,
            'agent': {
                'employee_id': result['employee_id'],
                'employee_name': result['employee_name'],
                'email': result['email'],
                'phone': result['phone']
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error verifying invite token: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        session.close()


@auth_bp.route('/accept-invite', methods=['POST'])
def accept_invite():
    """Accept invite and set password"""
    session = SessionLocal()
    try:
        data = request.get_json() or {}
        token = data.get('token')
        username = data.get('username', '').strip()
        password = data.get('password')
        
        if not token or not username or not password:
            return jsonify({
                'success': False,
                'error': 'Token, username, and password are required'
            }), 400
        
        # Validate password
        is_valid, message = validate_password(password)
        if not is_valid:
            return jsonify({'success': False, 'error': message}), 400
        
        # Verify token and get user
        verify_sql = text('''
            SELECT 
                um.user_id,
                um.employee_id,
                um.is_invite_pending,
                em.employee_name,
                em.email,
                em.tenant_id
            FROM "StreemLyne_MT"."User_Master" um
            JOIN "StreemLyne_MT"."Employee_Master" em 
                ON um.employee_id = em.employee_id
            WHERE um.invite_token = :token
            AND um.is_invite_pending = TRUE
            LIMIT 1
        ''')
        
        result = session.execute(verify_sql, {'token': token}).mappings().first()
        
        if not result:
            return jsonify({
                'success': False,
                'error': 'Invalid or expired invite token'
            }), 404
        
        # Check if username already exists
        username_check = text('''
            SELECT 1 FROM "StreemLyne_MT"."User_Master" 
            WHERE user_name = :username 
            AND user_id != :user_id
            LIMIT 1
        ''')
        
        if session.execute(username_check, {
            'username': username,
            'user_id': result['user_id']
        }).first():
            return jsonify({
                'success': False,
                'error': 'Username already exists'
            }), 400
        
        # Update user with username, password, and mark invite as accepted
        update_sql = text('''
            UPDATE "StreemLyne_MT"."User_Master"
            SET user_name = :username,
                password = :password,
                is_invite_pending = FALSE,
                invite_token = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = :user_id
            RETURNING user_id, employee_id, user_name
        ''')
        
        updated = session.execute(update_sql, {
            'username': username,
            'password': password,
            'user_id': result['user_id']
        }).mappings().first()
        
        session.commit()
        
        if not updated:
            return jsonify({
                'success': False,
                'error': 'Failed to accept invite'
            }), 500
        
        # Generate JWT token
        tenant_slug = normalize_tenant_id(result['tenant_id'])
        payload = {
            'user_id': updated['user_id'],
            'employee_id': updated['employee_id'],
            'tenant_id': tenant_slug,
            'user_name': updated['user_name'],
            'exp': datetime.utcnow() + timedelta(days=7),
            'iat': datetime.utcnow()
        }
        
        auth_token = jwt.encode(payload, current_app.config['SECRET_KEY'], algorithm='HS256')
        
        user_data = {
            'user_id': updated['user_id'],
            'employee_id': updated['employee_id'],
            'user_name': updated['user_name'],
            'tenant_id': tenant_slug,
            'email': result['email'],
            'name': result['employee_name']
        }
        
        logger.info(f"✅ Invite accepted: user_id={updated['user_id']} username={username}")
        
        return jsonify({
            'success': True,
            'message': 'Invite accepted successfully',
            'token': auth_token,
            'user': user_data
        }), 200
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error accepting invite: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        session.close()
