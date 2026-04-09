# -*- coding: utf-8 -*-
"""
Property Calendar Routes
Calendar view for property rent due dates and lease events
"""
from flask import Blueprint, g, jsonify, request
from backend.routes.auth_helpers import token_required, get_current_tenant_id
from backend.db import SessionLocal
from sqlalchemy import text
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

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
    - Rent due dates (based on rent_due_day)
    - Lease start dates
    - Lease end dates
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context',
                'message': 'tenant_id missing in token or X-Tenant-ID mismatch',
            }), 403

        # Get filter parameters
        agent_id = request.args.get('agent_id', type=int)
        start_date = request.args.get('start_date')  # Optional: filter from this date
        end_date = request.args.get('end_date')      # Optional: filter to this date
        
        logger.info(f"📅 Fetching calendar events for tenant_id: {tenant_id}")
        
        # Build filters
        filters = ["p.is_deleted = FALSE"]
        params = {'tenant_id': tenant_id}
        
        if agent_id:
            filters.append("p.assigned_agent_id = :agent_id")
            params['agent_id'] = agent_id
        
        filter_clause = " AND ".join(filters)
        
        # Get properties with rent due dates and lease dates
        query = text(f'''
            SELECT 
                p.property_id,
                p.property_name,
                p.address,
                p.city,
                p.postcode, 
                p.monthly_rent,
                p.rent_due_day,
                p.lease_start_date,
                p.lease_end_date,
                p.tenant_name,
                em.employee_name as agent_name,
                s.stage_name as property_status
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Employee_Master" em 
                ON p.assigned_agent_id = em.employee_id AND p.tenant_id = em.tenant_id
            LEFT JOIN "StreemLyne_MT"."Stage_Master" s 
                ON p.status_id = s.stage_id
            WHERE p.tenant_id = :tenant_id
            AND {filter_clause}
            ORDER BY p.property_name
        ''')
        
        result = session.execute(query, params)
        properties = [dict(row._mapping) for row in result]
        
        # Generate calendar events
        events = []
        today = datetime.now().date()
        
        # Calculate date range for rent due events (next 12 months)
        if start_date:
            range_start = datetime.strptime(start_date, '%Y-%m-%d').date()
        else:
            range_start = today
            
        if end_date:
            range_end = datetime.strptime(end_date, '%Y-%m-%d').date()
        else:
            range_end = today + relativedelta(months=12)
        
        for prop in properties:
            # Generate rent due events
            if prop['rent_due_day'] and prop['monthly_rent']:
                current_month = range_start.replace(day=1)
                end_month = range_end.replace(day=1)
                
                while current_month <= end_month:
                    try:
                        # Create rent due date for this month
                        rent_due_date = current_month.replace(day=prop['rent_due_day'])
                        
                        # Only include if within range
                        if range_start <= rent_due_date <= range_end:
                            event = {
                                'id': f"rent-{prop['property_id']}-{rent_due_date.strftime('%Y-%m')}",
                                'property_id': prop['property_id'],
                                'type': 'rent_due',
                                'event_type': 'rent_due',
                                'title': f"{prop['property_name']} - Rent Due",
                                'property_name': prop['property_name'],
                                'tenant_name': prop.get('tenant_name'),
                                'address': prop.get('address'),
                                'city': prop.get('city'),
                                'postcode': prop.get('postcode'),
                                'event_date': rent_due_date.strftime('%Y-%m-%d'),
                                'amount': float(prop['monthly_rent']) if prop['monthly_rent'] else None,
                                'agent_name': prop.get('agent_name'),
                                'property_status': prop.get('property_status'),
                                'notes': f"Rent due on day {prop['rent_due_day']} of the month",
                            }
                            events.append(event)
                    except ValueError:
                        # Handle invalid dates (e.g., day 31 in February)
                        pass
                    
                    # Move to next month
                    current_month = current_month + relativedelta(months=1)
            
            # Add lease start event
            if prop['lease_start_date']:
                lease_start = prop['lease_start_date']
                if isinstance(lease_start, str):
                    lease_start = datetime.strptime(lease_start.split('T')[0], '%Y-%m-%d').date()
                
                if range_start <= lease_start <= range_end:
                    event = {
                        'id': f"lease-start-{prop['property_id']}",
                        'property_id': prop['property_id'],
                        'type': 'lease_start',
                        'event_type': 'lease_start',
                        'title': f"{prop['property_name']} - Lease Start",
                        'property_name': prop['property_name'],
                        'tenant_name': prop.get('tenant_name'),
                        'address': prop.get('address'),
                        'city': prop.get('city'),
                        'postcode': prop.get('postcode'),
                        'event_date': lease_start.strftime('%Y-%m-%d'),
                        'agent_name': prop.get('agent_name'),
                        'property_status': prop.get('property_status'),
                        'notes': 'Lease commencement date',
                    }
                    events.append(event)
            
            # Add lease end event
            if prop['lease_end_date']:
                lease_end = prop['lease_end_date']
                if isinstance(lease_end, str):
                    lease_end = datetime.strptime(lease_end.split('T')[0], '%Y-%m-%d').date()
                
                if range_start <= lease_end <= range_end:
                    event = {
                        'id': f"lease-end-{prop['property_id']}",
                        'property_id': prop['property_id'],
                        'type': 'lease_end',
                        'event_type': 'lease_end',
                        'title': f"{prop['property_name']} - Lease End",
                        'property_name': prop['property_name'],
                        'tenant_name': prop.get('tenant_name'),
                        'address': prop.get('address'),
                        'city': prop.get('city'),
                        'postcode': prop.get('postcode'),
                        'event_date': lease_end.strftime('%Y-%m-%d'),
                        'agent_name': prop.get('agent_name'),
                        'property_status': prop.get('property_status'),
                        'notes': 'Lease expiration date - renewal needed',
                    }
                    events.append(event)
        
        # Sort events by date
        events.sort(key=lambda x: x['event_date'])
        
        logger.info(f"✅ Generated {len(events)} calendar events")
        
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
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context',
                'message': 'tenant_id missing in token or X-Tenant-ID mismatch',
            }), 403

        query = text('''
            SELECT 
                property_id as id,
                property_name as name,
                address,
                city,
                postcode as postcode,
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
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context',
                'message': 'tenant_id missing in token or X-Tenant-ID mismatch',
            }), 403

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