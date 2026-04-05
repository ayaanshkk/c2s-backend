# -*- coding: utf-8 -*-
"""
Tenant Routes
Manage property management companies (tenants)
No permissions required - all authenticated users can create/modify
"""
from flask import Blueprint, request, jsonify, g
from backend.routes.auth_helpers import token_required
from backend.db import SessionLocal
from sqlalchemy import text
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

tenant_bp = Blueprint('tenants', __name__, url_prefix='/api/tenants')

# ✅ CORS support
@tenant_bp.after_request
def after_request(response):
    """Add CORS headers to all responses"""
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Tenant-ID')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response


@tenant_bp.route('', methods=['GET', 'OPTIONS'])
@token_required
def get_all_tenants():
    """
    Get all property management companies (tenants)
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        query = text('''
            SELECT 
                tenant_id,
                tenant_name,
                tenant_address,
                tenant_phone,
                tenant_email,
                tenant_website,
                created_at,
                is_active
            FROM "StreemLyne_MT"."Tenant_Master"
            ORDER BY tenant_name ASC
        ''')
        
        result = session.execute(query)
        tenants = []
        
        for row in result:
            tenants.append({
                'tenant_id': row.tenant_id,
                'tenant_name': row.tenant_name,
                'tenant_address': row.tenant_address,
                'tenant_phone': row.tenant_phone,
                'tenant_email': row.tenant_email,
                'tenant_website': row.tenant_website,
                'created_at': row.created_at.isoformat() if row.created_at else None,
                'is_active': row.is_active
            })
        
        return jsonify({
            'success': True,
            'tenants': tenants,
            'count': len(tenants)
        }), 200
        
    except Exception as e:
        logger.exception("❌ Error fetching tenants")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()


@tenant_bp.route('/<int:tenant_id>', methods=['GET', 'OPTIONS'])
@token_required
def get_tenant(tenant_id):
    """
    Get a specific tenant by ID
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        query = text('''
            SELECT 
                tenant_id,
                tenant_name,
                tenant_address,
                tenant_phone,
                tenant_email,
                tenant_website,
                created_at,
                is_active
            FROM "StreemLyne_MT"."Tenant_Master"
            WHERE tenant_id = :tenant_id
            LIMIT 1
        ''')
        
        result = session.execute(query, {'tenant_id': tenant_id}).first()
        
        if not result:
            return jsonify({
                'success': False,
                'error': 'Tenant not found'
            }), 404
        
        tenant = {
            'tenant_id': result.tenant_id,
            'tenant_name': result.tenant_name,
            'tenant_address': result.tenant_address,
            'tenant_phone': result.tenant_phone,
            'tenant_email': result.tenant_email,
            'tenant_website': result.tenant_website,
            'created_at': result.created_at.isoformat() if result.created_at else None,
            'is_active': result.is_active
        }
        
        return jsonify({
            'success': True,
            'tenant': tenant
        }), 200
        
    except Exception as e:
        logger.exception(f"❌ Error fetching tenant {tenant_id}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()


@tenant_bp.route('', methods=['POST', 'OPTIONS'])
@token_required
def create_tenant():
    """
    Create a new property management company (tenant)
    
    Expected JSON: {
        "tenant_name": "ABC Property Management",
        "tenant_address": "123 Main St, London",
        "tenant_phone": "020 1234 5678",
        "tenant_email": "info@abcpm.com",
        "tenant_website": "https://abcpm.com"
    }
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        data = request.get_json() or {}
        
        # Validate required fields
        if not data.get('tenant_name'):
            return jsonify({
                'success': False,
                'error': 'tenant_name is required'
            }), 400
        
        # Check if tenant name already exists
        check_query = text('''
            SELECT 1 FROM "StreemLyne_MT"."Tenant_Master"
            WHERE LOWER(tenant_name) = LOWER(:tenant_name)
            LIMIT 1
        ''')
        
        exists = session.execute(check_query, {
            'tenant_name': data['tenant_name']
        }).first()
        
        if exists:
            return jsonify({
                'success': False,
                'error': 'A tenant with this name already exists'
            }), 400
        
        # Insert new tenant
        insert_query = text('''
            INSERT INTO "StreemLyne_MT"."Tenant_Master" (
                tenant_name,
                tenant_address,
                tenant_phone,
                tenant_email,
                tenant_website,
                created_at,
                is_active
            )
            VALUES (
                :tenant_name,
                :tenant_address,
                :tenant_phone,
                :tenant_email,
                :tenant_website,
                NOW(),
                TRUE
            )
            RETURNING tenant_id, tenant_name, tenant_address, tenant_phone, 
                      tenant_email, tenant_website, created_at, is_active
        ''')
        
        result = session.execute(insert_query, {
            'tenant_name': data['tenant_name'],
            'tenant_address': data.get('tenant_address'),
            'tenant_phone': data.get('tenant_phone'),
            'tenant_email': data.get('tenant_email'),
            'tenant_website': data.get('tenant_website')
        }).first()
        
        session.commit()
        
        tenant = {
            'tenant_id': result.tenant_id,
            'tenant_name': result.tenant_name,
            'tenant_address': result.tenant_address,
            'tenant_phone': result.tenant_phone,
            'tenant_email': result.tenant_email,
            'tenant_website': result.tenant_website,
            'created_at': result.created_at.isoformat() if result.created_at else None,
            'is_active': result.is_active
        }
        
        logger.info(f"✅ Created tenant: {tenant['tenant_name']} (ID: {tenant['tenant_id']})")
        
        return jsonify({
            'success': True,
            'message': 'Tenant created successfully',
            'tenant': tenant
        }), 201
        
    except Exception as e:
        session.rollback()
        logger.exception("❌ Error creating tenant")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()


@tenant_bp.route('/<int:tenant_id>', methods=['PUT', 'PATCH', 'OPTIONS'])
@token_required
def update_tenant(tenant_id):
    """
    Update a tenant's information
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        data = request.get_json() or {}
        
        # Build dynamic update query
        update_fields = []
        params = {'tenant_id': tenant_id}
        
        allowed_fields = {
            'tenant_name': 'tenant_name',
            'tenant_address': 'tenant_address',
            'tenant_phone': 'tenant_phone',
            'tenant_email': 'tenant_email',
            'tenant_website': 'tenant_website',
            'is_active': 'is_active'
        }
        
        for api_field, db_field in allowed_fields.items():
            if api_field in data:
                update_fields.append(f'{db_field} = :{api_field}')
                params[api_field] = data[api_field]
        
        if not update_fields:
            return jsonify({
                'success': False,
                'error': 'No fields to update'
            }), 400
        
        update_query = text(f'''
            UPDATE "StreemLyne_MT"."Tenant_Master"
            SET {', '.join(update_fields)}
            WHERE tenant_id = :tenant_id
            RETURNING tenant_id, tenant_name, tenant_address, tenant_phone, 
                      tenant_email, tenant_website, created_at, is_active
        ''')
        
        result = session.execute(update_query, params).first()
        
        if not result:
            return jsonify({
                'success': False,
                'error': 'Tenant not found'
            }), 404
        
        session.commit()
        
        tenant = {
            'tenant_id': result.tenant_id,
            'tenant_name': result.tenant_name,
            'tenant_address': result.tenant_address,
            'tenant_phone': result.tenant_phone,
            'tenant_email': result.tenant_email,
            'tenant_website': result.tenant_website,
            'created_at': result.created_at.isoformat() if result.created_at else None,
            'is_active': result.is_active
        }
        
        logger.info(f"✅ Updated tenant {tenant_id}")
        
        return jsonify({
            'success': True,
            'message': 'Tenant updated successfully',
            'tenant': tenant
        }), 200
        
    except Exception as e:
        session.rollback()
        logger.exception(f"❌ Error updating tenant {tenant_id}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()


@tenant_bp.route('/<int:tenant_id>', methods=['DELETE', 'OPTIONS'])
@token_required
def delete_tenant(tenant_id):
    """
    Delete a tenant (soft delete - sets is_active to FALSE)
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        # Soft delete - just deactivate
        update_query = text('''
            UPDATE "StreemLyne_MT"."Tenant_Master"
            SET is_active = FALSE
            WHERE tenant_id = :tenant_id
            RETURNING tenant_id
        ''')
        
        result = session.execute(update_query, {'tenant_id': tenant_id}).first()
        
        if not result:
            return jsonify({
                'success': False,
                'error': 'Tenant not found'
            }), 404
        
        session.commit()
        
        logger.info(f"✅ Deactivated tenant {tenant_id}")
        
        return jsonify({
            'success': True,
            'message': 'Tenant deactivated successfully'
        }), 200
        
    except Exception as e:
        session.rollback()
        logger.exception(f"❌ Error deleting tenant {tenant_id}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()


@tenant_bp.route('/<int:tenant_id>/stats', methods=['GET', 'OPTIONS'])
@token_required
def get_tenant_stats(tenant_id):
    """
    Get statistics for a specific tenant
    - Total properties
    - Properties by status
    - Total agents
    - Total monthly income
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        # Get property stats
        stats_query = text('''
            SELECT 
                COUNT(*) as total_properties,
                COUNT(CASE WHEN s.stage_name = 'Available' THEN 1 END) as available,
                COUNT(CASE WHEN s.stage_name = 'Occupied' THEN 1 END) as occupied,
                COUNT(CASE WHEN s.stage_name = 'Under Maintenance' THEN 1 END) as maintenance,
                COUNT(CASE WHEN p.assigned_agent_id IS NOT NULL THEN 1 END) as assigned_count,
                SUM(CASE WHEN s.stage_name = 'Occupied' AND p.monthly_rent IS NOT NULL 
                    THEN p.monthly_rent ELSE 0 END) as total_monthly_income
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Stage_Master" s 
                ON p.status_id = s.stage_id
            WHERE p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
        ''')
        
        stats = session.execute(stats_query, {'tenant_id': tenant_id}).first()
        
        # Get agent count
        agent_query = text('''
            SELECT COUNT(*) as agent_count
            FROM "StreemLyne_MT"."Employee_Master"
            WHERE tenant_id = :tenant_id
        ''')
        
        agents = session.execute(agent_query, {'tenant_id': tenant_id}).first()
        
        return jsonify({
            'success': True,
            'stats': {
                'total_properties': int(stats.total_properties or 0),
                'available': int(stats.available or 0),
                'occupied': int(stats.occupied or 0),
                'maintenance': int(stats.maintenance or 0),
                'assigned_count': int(stats.assigned_count or 0),
                'total_monthly_income': float(stats.total_monthly_income or 0),
                'total_agents': int(agents.agent_count or 0)
            }
        }), 200
        
    except Exception as e:
        logger.exception(f"❌ Error fetching tenant stats for {tenant_id}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()


@tenant_bp.route('/<int:tenant_id>/toggle-status', methods=['POST', 'OPTIONS'])
@token_required
def toggle_tenant_status(tenant_id):
    """
    Toggle tenant active status (activate/deactivate)
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        # Get current status
        query = text('''
            SELECT is_active
            FROM "StreemLyne_MT"."Tenant_Master"
            WHERE tenant_id = :tenant_id
            LIMIT 1
        ''')
        
        result = session.execute(query, {'tenant_id': tenant_id}).first()
        
        if not result:
            return jsonify({
                'success': False,
                'error': 'Tenant not found'
            }), 404
        
        new_status = not result.is_active
        
        # Update status
        update_query = text('''
            UPDATE "StreemLyne_MT"."Tenant_Master"
            SET is_active = :new_status
            WHERE tenant_id = :tenant_id
        ''')
        
        session.execute(update_query, {
            'tenant_id': tenant_id,
            'new_status': new_status
        })
        
        session.commit()
        
        logger.info(f"✅ Tenant {tenant_id} status changed to: {new_status}")
        
        return jsonify({
            'success': True,
            'message': f'Tenant {"activated" if new_status else "deactivated"} successfully',
            'is_active': new_status
        }), 200
        
    except Exception as e:
        session.rollback()
        logger.exception(f"❌ Error toggling tenant status {tenant_id}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()