# -*- coding: utf-8 -*-
"""
Tenant Routes (Property Leaseholders)
Manage tenants who rent properties - stored in Client_Master
"""
from flask import Blueprint, request, jsonify, g
from backend.routes.auth_helpers import token_required, get_current_tenant_id
from backend.db import SessionLocal
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

tenant_bp = Blueprint('tenants', __name__, url_prefix='/api/tenants')

@tenant_bp.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Tenant-ID')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response


@tenant_bp.route('', methods=['POST', 'OPTIONS'])
@token_required
def create_tenant():
    """
    Create a new tenant (property leaseholder) in Client_Master
    Optionally assign to a property
    
    Body: {
        "tenant_name": "John Smith",
        "tenant_contact": "+44 7700 900000",
        "tenant_email": "john@example.com",
        "property_id": 123,  // Optional
        "lease_start_date": "2024-01-01",  // Optional
        "lease_end_date": "2025-01-01"  // Optional
    }
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    tenant_id = get_current_tenant_id()
    if not tenant_id:
        return jsonify({'error': 'Invalid tenant context'}), 403
    
    session = SessionLocal()
    
    try:
        data = request.get_json() or {}
        
        # Validate required fields
        if not data.get('tenant_name'):
            return jsonify({'error': 'tenant_name is required'}), 400
        if not data.get('tenant_contact'):
            return jsonify({'error': 'tenant_contact is required'}), 400
        
        # Check if client already exists by email or phone
        check_query = text('''
            SELECT client_id, client_contact_name, client_email, client_phone
            FROM "StreemLyne_MT"."Client_Master"
            WHERE tenant_id = :tenant_id
            AND is_deleted = FALSE
            AND (
                (:email IS NOT NULL AND client_email = :email)
                OR (:phone IS NOT NULL AND client_phone = :phone)
            )
            LIMIT 1
        ''')
        
        existing = session.execute(check_query, {
            'tenant_id': tenant_id,
            'email': data.get('tenant_email'),
            'phone': data.get('tenant_contact')
        }).first()
        
        if existing:
            client_id = existing.client_id
            logger.info(f"✅ Using existing client: {existing.client_contact_name} (ID: {client_id})")
        else:
            # Create new client (tenant)
            insert_query = text('''
                INSERT INTO "StreemLyne_MT"."Client_Master" (
                    tenant_id,
                    client_company_name,
                    client_contact_name,
                    client_email,
                    client_phone,
                    client_mobile,
                    created_at,
                    is_deleted
                )
                VALUES (
                    :tenant_id,
                    :company_name,
                    :contact_name,
                    :email,
                    :phone,
                    :phone,
                    NOW(),
                    FALSE
                )
                RETURNING client_id, client_contact_name, client_email, client_phone
            ''')
            
            result = session.execute(insert_query, {
                'tenant_id': tenant_id,
                'company_name': data['tenant_name'],  # Use name as company for individuals
                'contact_name': data['tenant_name'],
                'email': data.get('tenant_email'),
                'phone': data.get('tenant_contact')
            }).first()
            
            client_id = result.client_id
            logger.info(f"✅ Created new client: {result.client_contact_name} (ID: {client_id})")
        
        # If property_id provided, assign tenant to property
        if data.get('property_id'):
            update_property = text('''
                UPDATE "StreemLyne_MT"."Property_Master"
                SET 
                    client_id = :client_id,
                    tenant_name = :tenant_name,
                    tenant_email = :tenant_email,
                    tenant_contact = :tenant_contact,
                    lease_start_date = :lease_start_date,
                    lease_end_date = :lease_end_date,
                    occupancy_status = 'Occupied'
                WHERE property_id = :property_id
                AND tenant_id = :tenant_id
                RETURNING property_id, property_name
            ''')
            
            property_result = session.execute(update_property, {
                'client_id': client_id,
                'tenant_name': data['tenant_name'],
                'tenant_email': data.get('tenant_email'),
                'tenant_contact': data.get('tenant_contact'),
                'lease_start_date': data.get('lease_start_date'),
                'lease_end_date': data.get('lease_end_date'),
                'property_id': data['property_id'],
                'tenant_id': tenant_id
            }).first()
            
            if property_result:
                logger.info(f"✅ Assigned client {client_id} to property {property_result.property_name}")
        
        session.commit()
        
        return jsonify({
            'success': True,
            'message': 'Tenant created successfully',
            'client_id': client_id
        }), 201
        
    except Exception as e:
        session.rollback()
        logger.exception("❌ Error creating tenant")
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


@tenant_bp.route('', methods=['GET', 'OPTIONS'])
@token_required
def get_all_tenants():
    """
    Get all tenants (clients who are renting or will rent properties)
    Shows both assigned and unassigned tenants
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    tenant_id = get_current_tenant_id()
    if not tenant_id:
        return jsonify({'error': 'Invalid tenant context'}), 403
    
    session = SessionLocal()
    
    try:
        query = text('''
            SELECT 
                c.client_id,
                c.client_contact_name as tenant_name,
                c.client_email as tenant_email,
                c.client_phone as tenant_contact,
                p.property_id,
                p.property_name,
                p.address,
                p.lease_start_date,
                p.lease_end_date,
                p.monthly_rent,
                p.occupancy_status
            FROM "StreemLyne_MT"."Client_Master" c
            LEFT JOIN "StreemLyne_MT"."Property_Master" p 
                ON c.client_id = p.client_id 
                AND p.is_deleted = FALSE
            WHERE c.tenant_id = :tenant_id
            AND c.is_deleted = FALSE
            ORDER BY c.client_contact_name ASC
        ''')
        
        result = session.execute(query, {'tenant_id': tenant_id})
        
        tenants = []
        for row in result:
            tenants.append({
                'client_id': row.client_id,
                'tenant_name': row.tenant_name,
                'tenant_email': row.tenant_email,
                'tenant_contact': row.tenant_contact,
                'property_id': row.property_id,
                'property_name': row.property_name,
                'address': row.address,
                'lease_start_date': row.lease_start_date.isoformat() if row.lease_start_date else None,
                'lease_end_date': row.lease_end_date.isoformat() if row.lease_end_date else None,
                'monthly_rent': float(row.monthly_rent) if row.monthly_rent else None,
                'occupancy_status': row.occupancy_status
            })
        
        return jsonify({
            'success': True,
            'tenants': tenants,
            'count': len(tenants)
        }), 200
        
    except Exception as e:
        logger.exception("❌ Error fetching tenants")
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@tenant_bp.route('/<int:client_id>', methods=['GET', 'OPTIONS'])
@token_required
def get_tenant(client_id):
    """
    Get a specific tenant by client_id
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    tenant_id = get_current_tenant_id()
    if not tenant_id:
        return jsonify({'error': 'Invalid tenant context'}), 403
    
    session = SessionLocal()
    
    try:
        query = text('''
            SELECT 
                c.client_id,
                c.client_contact_name as tenant_name,
                c.client_email as tenant_email,
                c.client_phone as tenant_contact,
                c.client_mobile,
                c.address,
                c.post_code,
                p.property_id,
                p.property_name,
                p.lease_start_date,
                p.lease_end_date,
                p.monthly_rent
            FROM "StreemLyne_MT"."Client_Master" c
            LEFT JOIN "StreemLyne_MT"."Property_Master" p 
                ON c.client_id = p.client_id 
                AND p.is_deleted = FALSE
            WHERE c.client_id = :client_id
            AND c.tenant_id = :tenant_id
            AND c.is_deleted = FALSE
            LIMIT 1
        ''')
        
        result = session.execute(query, {
            'client_id': client_id,
            'tenant_id': tenant_id
        }).first()
        
        if not result:
            return jsonify({'error': 'Tenant not found'}), 404
        
        tenant = {
            'client_id': result.client_id,
            'tenant_name': result.tenant_name,
            'tenant_email': result.tenant_email,
            'tenant_contact': result.tenant_contact,
            'client_mobile': result.client_mobile,
            'address': result.address,
            'post_code': result.post_code,
            'property_id': result.property_id,
            'property_name': result.property_name,
            'lease_start_date': result.lease_start_date.isoformat() if result.lease_start_date else None,
            'lease_end_date': result.lease_end_date.isoformat() if result.lease_end_date else None,
            'monthly_rent': float(result.monthly_rent) if result.monthly_rent else 0
        }
        
        return jsonify({
            'success': True,
            'tenant': tenant
        }), 200
        
    except Exception as e:
        logger.exception(f"❌ Error fetching tenant {client_id}")
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


@tenant_bp.route('/<int:client_id>', methods=['DELETE', 'OPTIONS'])
@token_required
def delete_tenant(client_id):
    """
    Soft delete a tenant (mark as deleted in Client_Master)
    Also removes them from any assigned properties
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    tenant_id = get_current_tenant_id()
    if not tenant_id:
        return jsonify({'error': 'Invalid tenant context'}), 403
    
    session = SessionLocal()
    
    try:
        # Remove from properties first
        update_properties = text('''
            UPDATE "StreemLyne_MT"."Property_Master"
            SET 
                client_id = NULL,
                tenant_name = NULL,
                tenant_email = NULL,
                tenant_contact = NULL,
                lease_start_date = NULL,
                lease_end_date = NULL,
                occupancy_status = 'Vacant'
            WHERE client_id = :client_id
            AND tenant_id = :tenant_id
        ''')
        
        session.execute(update_properties, {
            'client_id': client_id,
            'tenant_id': tenant_id
        })
        
        # Soft delete client
        delete_query = text('''
            UPDATE "StreemLyne_MT"."Client_Master"
            SET 
                is_deleted = TRUE,
                deleted_at = NOW()
            WHERE client_id = :client_id
            AND tenant_id = :tenant_id
            RETURNING client_id
        ''')
        
        result = session.execute(delete_query, {
            'client_id': client_id,
            'tenant_id': tenant_id
        }).first()
        
        if not result:
            return jsonify({'error': 'Tenant not found'}), 404
        
        session.commit()
        
        logger.info(f"✅ Deleted tenant (client_id: {client_id})")
        
        return jsonify({
            'success': True,
            'message': 'Tenant deleted successfully'
        }), 200
        
    except Exception as e:
        session.rollback()
        logger.exception(f"❌ Error deleting tenant {client_id}")
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()