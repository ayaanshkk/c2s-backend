# -*- coding: utf-8 -*-
"""
Property Interaction Routes
Handle property viewings, inspections, maintenance notes, and callbacks
"""
from flask import Blueprint, request, jsonify, g
from datetime import datetime
from sqlalchemy import text
from backend.properties.db import SessionLocal
from backend.properties.routes.auth_helpers import token_required
import logging

logger = logging.getLogger(__name__)

interaction_bp = Blueprint('interactions', __name__, url_prefix='/api/interactions')

# ✅ CORS support
@interaction_bp.after_request
def after_request(response):
    """Add CORS headers to all responses"""
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Tenant-ID')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response


@interaction_bp.route('/properties/<int:property_id>', methods=['POST', 'OPTIONS'])
@token_required
def create_interaction(property_id):
    """
    Create a new property interaction (viewing, inspection, note, callback)
    
    Expected JSON: {
        "interaction_type": "viewing|inspection|note|maintenance|callback",
        "interaction_date": "2026-04-15",
        "reminder_date": "2026-04-20",  # optional
        "notes": "Client very interested",
        "next_steps": "Follow up next week"  # optional
    }
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        user = g.user
        tenant_id = user.tenant_id
        data = request.get_json() or {}
        
        # Validate required fields
        interaction_type = data.get('interaction_type')
        if not interaction_type:
            return jsonify({'error': 'interaction_type is required'}), 400
        
        valid_types = ['viewing', 'inspection', 'note', 'maintenance', 'callback']
        if interaction_type not in valid_types:
            return jsonify({'error': f'interaction_type must be one of: {", ".join(valid_types)}'}), 400
        
        # Verify property exists and belongs to tenant
        property_check = text('''
            SELECT property_id 
            FROM "StreemLyne_MT"."Property_Master" 
            WHERE property_id = :property_id 
            AND tenant_id = :tenant_id 
            AND is_deleted = FALSE
            LIMIT 1
        ''')
        
        property_exists = session.execute(
            property_check, 
            {'property_id': property_id, 'tenant_id': tenant_id}
        ).first()
        
        if not property_exists:
            return jsonify({'error': 'Property not found or access denied'}), 404
        
        # Parse dates
        interaction_date = None
        if data.get('interaction_date'):
            try:
                interaction_date = datetime.strptime(data['interaction_date'], '%Y-%m-%d').date()
            except ValueError:
                return jsonify({'error': 'Invalid interaction_date format. Use YYYY-MM-DD'}), 400
        
        reminder_date = None
        if data.get('reminder_date'):
            try:
                reminder_date = datetime.strptime(data['reminder_date'], '%Y-%m-%d').date()
            except ValueError:
                return jsonify({'error': 'Invalid reminder_date format. Use YYYY-MM-DD'}), 400
        
        # Insert interaction
        insert_query = text('''
            INSERT INTO "StreemLyne_MT"."Client_Interactions" (
                property_id,
                interaction_type,
                interaction_date,
                reminder_date,
                notes,
                next_steps,
                contact_method,
                created_at
            )
            VALUES (
                :property_id,
                :interaction_type,
                :interaction_date,
                :reminder_date,
                :notes,
                :next_steps,
                1,
                NOW()
            )
            RETURNING interaction_id
        ''')
        
        result = session.execute(insert_query, {
            'property_id': property_id,
            'interaction_type': interaction_type,
            'interaction_date': interaction_date or datetime.utcnow().date(),
            'reminder_date': reminder_date,
            'notes': data.get('notes', ''),
            'next_steps': data.get('next_steps')
        })
        
        interaction_id = result.scalar()
        session.commit()
        
        logger.info(f"✅ Created {interaction_type} for property {property_id}")
        
        return jsonify({
            'success': True,
            'message': 'Interaction created successfully',
            'interaction_id': interaction_id
        }), 201
        
    except Exception as e:
        session.rollback()
        logger.exception(f"❌ Error creating interaction: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


@interaction_bp.route('/properties/<int:property_id>', methods=['GET', 'OPTIONS'])
@token_required
def get_property_interactions(property_id):
    """
    Get all interactions for a specific property
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        user = g.user
        tenant_id = user.tenant_id
        
        # Verify property access
        query = text('''
            SELECT 
                ci.interaction_id,
                ci.interaction_type,
                ci.interaction_date,
                ci.reminder_date,
                ci.notes,
                ci.next_steps,
                ci.created_at,
                em.employee_name as created_by
            FROM "StreemLyne_MT"."Client_Interactions" ci
            INNER JOIN "StreemLyne_MT"."Property_Master" p 
                ON ci.property_id = p.property_id
            LEFT JOIN "StreemLyne_MT"."Employee_Master" em 
                ON ci.employee_id = em.employee_id
            WHERE ci.property_id = :property_id
            AND p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
            ORDER BY ci.interaction_date DESC, ci.created_at DESC
        ''')
        
        result = session.execute(query, {
            'property_id': property_id,
            'tenant_id': tenant_id
        })
        
        interactions = []
        for row in result:
            interactions.append({
                'interaction_id': row.interaction_id,
                'interaction_type': row.interaction_type,
                'interaction_date': str(row.interaction_date) if row.interaction_date else None,
                'reminder_date': str(row.reminder_date) if row.reminder_date else None,
                'notes': row.notes,
                'next_steps': row.next_steps,
                'created_at': row.created_at.isoformat() if row.created_at else None,
                'created_by': row.created_by
            })
        
        return jsonify({
            'success': True,
            'interactions': interactions,
            'count': len(interactions)
        }), 200
        
    except Exception as e:
        logger.exception(f"❌ Error fetching interactions: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


@interaction_bp.route('/<int:interaction_id>', methods=['DELETE', 'OPTIONS'])
@token_required
def delete_interaction(interaction_id):
    """
    Delete a property interaction
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        user = g.user
        tenant_id = user.tenant_id
        
        # Verify interaction exists and belongs to tenant's property
        delete_query = text('''
            DELETE FROM "StreemLyne_MT"."Client_Interactions" ci
            USING "StreemLyne_MT"."Property_Master" p
            WHERE ci.property_id = p.property_id
            AND ci.interaction_id = :interaction_id
            AND p.tenant_id = :tenant_id
            RETURNING ci.interaction_id
        ''')
        
        result = session.execute(delete_query, {
            'interaction_id': interaction_id,
            'tenant_id': tenant_id
        })
        
        deleted_id = result.scalar()
        
        if not deleted_id:
            return jsonify({'error': 'Interaction not found or access denied'}), 404
        
        session.commit()
        
        logger.info(f"✅ Deleted interaction {interaction_id}")
        
        return jsonify({
            'success': True,
            'message': 'Interaction deleted successfully'
        }), 200
        
    except Exception as e:
        session.rollback()
        logger.exception(f"❌ Error deleting interaction: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


@interaction_bp.route('/<int:interaction_id>', methods=['PUT', 'OPTIONS'])
@token_required
def update_interaction(interaction_id):
    """
    Update a property interaction
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        user = g.user
        tenant_id = user.tenant_id
        data = request.get_json() or {}
        
        # Build dynamic update query
        update_fields = []
        params = {'interaction_id': interaction_id, 'tenant_id': tenant_id}
        
        if 'notes' in data:
            update_fields.append('notes = :notes')
            params['notes'] = data['notes']
        
        if 'next_steps' in data:
            update_fields.append('next_steps = :next_steps')
            params['next_steps'] = data['next_steps']
        
        if 'reminder_date' in data:
            if data['reminder_date']:
                try:
                    reminder_date = datetime.strptime(data['reminder_date'], '%Y-%m-%d').date()
                    update_fields.append('reminder_date = :reminder_date')
                    params['reminder_date'] = reminder_date
                except ValueError:
                    return jsonify({'error': 'Invalid reminder_date format'}), 400
            else:
                update_fields.append('reminder_date = NULL')
        
        if not update_fields:
            return jsonify({'error': 'No fields to update'}), 400
        
        update_query = text(f'''
            UPDATE "StreemLyne_MT"."Client_Interactions" ci
            SET {', '.join(update_fields)}
            FROM "StreemLyne_MT"."Property_Master" p
            WHERE ci.property_id = p.property_id
            AND ci.interaction_id = :interaction_id
            AND p.tenant_id = :tenant_id
            RETURNING ci.interaction_id
        ''')
        
        result = session.execute(update_query, params)
        updated_id = result.scalar()
        
        if not updated_id:
            return jsonify({'error': 'Interaction not found or access denied'}), 404
        
        session.commit()
        
        logger.info(f"✅ Updated interaction {interaction_id}")
        
        return jsonify({
            'success': True,
            'message': 'Interaction updated successfully'
        }), 200
        
    except Exception as e:
        session.rollback()
        logger.exception(f"❌ Error updating interaction: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()