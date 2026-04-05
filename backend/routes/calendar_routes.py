# -*- coding: utf-8 -*-
"""
Property Calendar Routes
Calendar view for property viewings, inspections, and maintenance schedules
"""
from flask import Blueprint, g, jsonify, request
from backend.routes.auth_helpers import token_required
from backend.db import SessionLocal
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

calendar_bp = Blueprint('calendar', __name__, url_prefix='/api/calendar')

# ✅ CORS support
@calendar_bp.after_request
def after_request(response):
    """Add CORS headers to all responses"""
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Tenant-ID')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response


@calendar_bp.route('/events', methods=['GET', 'OPTIONS'])
@token_required
def get_calendar_events():
    """
    Get all calendar events for property management
    - Property viewings
    - Maintenance schedules
    - Inspection dates
    - Lease expiry reminders
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        user = g.user
        tenant_id = user.tenant_id
        
        # Get filter parameters
        agent_id = request.args.get('agent_id', type=int)
        event_type = request.args.get('event_type')  # 'viewing', 'maintenance', 'inspection'
        
        logger.info(f"📅 Fetching calendar events for tenant_id: {tenant_id}")
        
        # Build filters
        filters = []
        params = {'tenant_id': tenant_id}
        
        if agent_id:
            filters.append("p.assigned_agent_id = :agent_id")
            params['agent_id'] = agent_id
        
        filter_clause = " AND " + " AND ".join(filters) if filters else ""
        
        # Query property interactions (viewings, inspections, maintenance)
        query = text(f'''
            SELECT 
                ci.interaction_id,
                ci.property_id,
                p.property_name,
                p.address,
                p.city,
                p.postcode,
                ci.interaction_type,
                ci.interaction_date as event_date,
                ci.notes,
                ci.next_steps,
                ci.reminder_date,
                em.employee_name as agent_name,
                s.stage_name as property_status
            FROM "StreemLyne_MT"."Client_Interactions" ci
            INNER JOIN "StreemLyne_MT"."Property_Master" p 
                ON ci.property_id = p.property_id
            LEFT JOIN "StreemLyne_MT"."Employee_Master" em 
                ON p.assigned_agent_id = em.employee_id
            LEFT JOIN "StreemLyne_MT"."Stage_Master" s 
                ON p.status_id = s.stage_id
            WHERE p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
            AND ci.reminder_date IS NOT NULL
            AND ci.reminder_date >= CURRENT_DATE
            {filter_clause}
            ORDER BY ci.reminder_date ASC
        ''')
        
        result = session.execute(query, params)
        interactions = [dict(row._mapping) for row in result]
        
        # Transform to calendar events
        events = []
        for interaction in interactions:
            event = {
                'id': f"interaction-{interaction['interaction_id']}",
                'property_id': interaction['property_id'],
                'type': interaction['interaction_type'],
                'title': f"{interaction['property_name']} - {interaction['interaction_type']}",
                'property_name': interaction['property_name'],
                'address': interaction['address'],
                'city': interaction['city'],
                'postcode': interaction['postcode'],
                'event_date': str(interaction['event_date']) if interaction.get('event_date') else None,
                'reminder_date': str(interaction['reminder_date']) if interaction.get('reminder_date') else None,
                'notes': interaction.get('notes'),
                'next_steps': interaction.get('next_steps'),
                'agent_name': interaction.get('agent_name'),
                'property_status': interaction.get('property_status'),
                'display_date': str(interaction['reminder_date']),
                'display_type': interaction['interaction_type'],
            }
            events.append(event)
        
        logger.info(f"✅ Found {len(events)} calendar events")
        
        return jsonify({
            'success': True,
            'events': events,
            'count': len(events)
        }), 200
        
    except Exception as e:
        logger.exception("❌ Error fetching calendar events")
        return jsonify({
            'success': False,
            'error': 'Failed to fetch calendar events',
            'message': str(e)
        }), 500
    finally:
        session.close()


@calendar_bp.route('/properties', methods=['GET', 'OPTIONS'])
@token_required
def get_properties_calendar():
    """
    Get properties for calendar view (simplified list for dropdowns)
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        user = g.user
        tenant_id = user.tenant_id
        
        query = text('''
            SELECT 
                property_id as id,
                property_name as name,
                address,
                city,
                postcode,
                s.stage_name as status
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Stage_Master" s 
                ON p.status_id = s.stage_id
            WHERE p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
            ORDER BY property_name
        ''')
        
        result = session.execute(query, {'tenant_id': tenant_id})
        properties = [dict(row._mapping) for row in result]
        
        return jsonify({
            'success': True,
            'properties': properties,
            'count': len(properties)
        }), 200
        
    except Exception as e:
        logger.exception("❌ Error fetching properties")
        return jsonify({
            'success': False,
            'error': 'Failed to fetch properties',
            'message': str(e)
        }), 500
    finally:
        session.close()


@calendar_bp.route('/agents', methods=['GET', 'OPTIONS'])
@token_required
def get_agents_calendar():
    """
    Get agents for calendar filtering
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        user = g.user
        tenant_id = user.tenant_id
        
        query = text('''
            SELECT 
                employee_id as id,
                employee_name as name,
                email,
                phone
            FROM "StreemLyne_MT"."Employee_Master"
            WHERE tenant_id = :tenant_id
            ORDER BY employee_name
        ''')
        
        result = session.execute(query, {'tenant_id': tenant_id})
        agents = [dict(row._mapping) for row in result]
        
        return jsonify({
            'success': True,
            'agents': agents,
            'count': len(agents)
        }), 200
        
    except Exception as e:
        logger.exception("❌ Error fetching agents")
        return jsonify({
            'success': False,
            'error': 'Failed to fetch agents',
            'message': str(e)
        }), 500
    finally:
        session.close()