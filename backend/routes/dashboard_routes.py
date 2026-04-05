# -*- coding: utf-8 -*-
"""
Property Dashboard Routes
Analytics and statistics for property management
"""
from flask import Blueprint, request, jsonify, g
from backend.routes.auth_helpers import token_required
from backend.db import SessionLocal
from sqlalchemy import text, func
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/api/dashboard')


# ========================================
# CORS SUPPORT
# ========================================

@dashboard_bp.after_request
def after_request(response):
    """Add CORS headers to all responses"""
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Tenant-ID')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response


# ========================================
# DASHBOARD OVERVIEW
# ========================================

@dashboard_bp.route('/overview', methods=['GET', 'OPTIONS'])
@token_required
def get_dashboard_overview():
    """
    Main dashboard overview statistics
    
    Returns:
    - Total properties
    - Properties by status (Available, Occupied, Maintenance)
    - Total monthly income
    - Occupancy rate
    - Recent activity
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        user = g.user
        tenant_id = user.tenant_id
        
        # Optional agent filter
        agent_id = request.args.get('agent_id', type=int)
        
        # Build base query with optional agent filter
        agent_filter = "AND p.assigned_agent_id = :agent_id" if agent_id else ""
        params = {'tenant_id': tenant_id}
        if agent_id:
            params['agent_id'] = agent_id
        
        # Main stats query
        stats_query = text(f'''
            SELECT 
                COUNT(*) as total_properties,
                COUNT(CASE WHEN s.stage_name = 'Available' THEN 1 END) as available,
                COUNT(CASE WHEN s.stage_name = 'Occupied' THEN 1 END) as occupied,
                COUNT(CASE WHEN s.stage_name = 'Under Maintenance' THEN 1 END) as maintenance,
                COUNT(CASE WHEN s.stage_name = 'Listed' THEN 1 END) as listed,
                COUNT(CASE WHEN s.stage_name = 'Reserved' THEN 1 END) as reserved,
                COUNT(CASE WHEN p.assigned_agent_id IS NOT NULL THEN 1 END) as assigned,
                COALESCE(SUM(CASE 
                    WHEN s.stage_name = 'Occupied' AND p.monthly_rent IS NOT NULL 
                    THEN p.monthly_rent 
                    ELSE 0 
                END), 0) as total_monthly_income,
                COALESCE(AVG(p.monthly_rent), 0) as avg_monthly_rent
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Stage_Master" s 
                ON p.status_id = s.stage_id
            WHERE p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
            {agent_filter}
        ''')
        
        stats = session.execute(stats_query, params).first()
        
        # Occupancy rate
        total = stats.total_properties or 1
        occupancy_rate = round((stats.occupied / total * 100), 1) if total > 0 else 0.0
        
        # Recent activity (properties added in last 30 days)
        recent_query = text(f'''
            SELECT COUNT(*)
            FROM "StreemLyne_MT"."Property_Master" p
            WHERE p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
            AND p.created_at >= CURRENT_DATE - INTERVAL '30 days'
            {agent_filter}
        ''')
        
        recent_properties = session.execute(recent_query, params).scalar() or 0
        
        return jsonify({
            'success': True,
            'stats': {
                'total_properties': int(stats.total_properties or 0),
                'available': int(stats.available or 0),
                'occupied': int(stats.occupied or 0),
                'maintenance': int(stats.maintenance or 0),
                'listed': int(stats.listed or 0),
                'reserved': int(stats.reserved or 0),
                'assigned': int(stats.assigned or 0),
                'total_monthly_income': float(stats.total_monthly_income or 0),
                'avg_monthly_rent': float(stats.avg_monthly_rent or 0),
                'occupancy_rate': occupancy_rate,
                'recent_properties_30d': recent_properties
            }
        }), 200
        
    except Exception as e:
        logger.exception("❌ Error fetching dashboard overview")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()


# ========================================
# AGENT PERFORMANCE
# ========================================

@dashboard_bp.route('/agent-performance', methods=['GET', 'OPTIONS'])
@token_required
def get_agent_performance():
    """
    Performance breakdown by agent
    
    Shows:
    - Properties managed per agent
    - Occupancy rates
    - Total income managed
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        user = g.user
        tenant_id = user.tenant_id
        
        query = text('''
            SELECT
                em.employee_id,
                em.employee_name,
                COUNT(p.property_id) as total_properties,
                COUNT(CASE WHEN s.stage_name = 'Occupied' THEN 1 END) as occupied,
                COUNT(CASE WHEN s.stage_name = 'Available' THEN 1 END) as available,
                COUNT(CASE WHEN s.stage_name = 'Under Maintenance' THEN 1 END) as maintenance,
                COALESCE(SUM(CASE 
                    WHEN s.stage_name = 'Occupied' AND p.monthly_rent IS NOT NULL 
                    THEN p.monthly_rent 
                    ELSE 0 
                END), 0) as total_income
            FROM "StreemLyne_MT"."Employee_Master" em
            LEFT JOIN "StreemLyne_MT"."Property_Master" p
                ON em.employee_id = p.assigned_agent_id
                AND p.tenant_id = :tenant_id
                AND p.is_deleted = FALSE
            LEFT JOIN "StreemLyne_MT"."Stage_Master" s
                ON p.status_id = s.stage_id
            WHERE em.tenant_id = :tenant_id
            GROUP BY em.employee_id, em.employee_name
            HAVING COUNT(p.property_id) > 0
            ORDER BY total_income DESC
        ''')
        
        results = session.execute(query, {'tenant_id': tenant_id}).fetchall()
        
        performance = []
        for r in results:
            total = r.total_properties or 1
            occupancy_rate = round((r.occupied / total * 100), 1) if total > 0 else 0.0
            
            performance.append({
                'agent_id': r.employee_id,
                'agent_name': r.employee_name,
                'total_properties': r.total_properties or 0,
                'occupied': r.occupied or 0,
                'available': r.available or 0,
                'maintenance': r.maintenance or 0,
                'total_income': float(r.total_income or 0),
                'occupancy_rate': occupancy_rate
            })
        
        return jsonify({
            'success': True,
            'performance': performance,
            'count': len(performance)
        }), 200
        
    except Exception as e:
        logger.exception("❌ Error fetching agent performance")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()


# ========================================
# PROPERTY STATUS BREAKDOWN
# ========================================

@dashboard_bp.route('/status-breakdown', methods=['GET', 'OPTIONS'])
@token_required
def get_status_breakdown():
    """
    Detailed breakdown by property status
    
    Returns count and details for each status
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        user = g.user
        tenant_id = user.tenant_id
        
        query = text('''
            SELECT
                s.stage_id,
                s.stage_name,
                COUNT(p.property_id) as count,
                COALESCE(SUM(p.monthly_rent), 0) as total_rent
            FROM "StreemLyne_MT"."Stage_Master" s
            LEFT JOIN "StreemLyne_MT"."Property_Master" p
                ON s.stage_id = p.status_id
                AND p.tenant_id = :tenant_id
                AND p.is_deleted = FALSE
            WHERE s.stage_type = 3
            GROUP BY s.stage_id, s.stage_name
            ORDER BY count DESC
        ''')
        
        results = session.execute(query, {'tenant_id': tenant_id}).fetchall()
        
        breakdown = []
        for r in results:
            breakdown.append({
                'status_id': r.stage_id,
                'status_name': r.stage_name,
                'count': r.count or 0,
                'total_rent': float(r.total_rent or 0)
            })
        
        return jsonify({
            'success': True,
            'breakdown': breakdown
        }), 200
        
    except Exception as e:
        logger.exception("❌ Error fetching status breakdown")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()


# ========================================
# CITY/LOCATION BREAKDOWN
# ========================================

@dashboard_bp.route('/location-breakdown', methods=['GET', 'OPTIONS'])
@token_required
def get_location_breakdown():
    """
    Properties grouped by city/location
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        user = g.user
        tenant_id = user.tenant_id
        
        query = text('''
            SELECT
                COALESCE(p.city, 'Unknown') as city,
                COUNT(p.property_id) as count,
                COUNT(CASE WHEN s.stage_name = 'Occupied' THEN 1 END) as occupied,
                COUNT(CASE WHEN s.stage_name = 'Available' THEN 1 END) as available,
                COALESCE(SUM(CASE 
                    WHEN s.stage_name = 'Occupied' THEN p.monthly_rent 
                    ELSE 0 
                END), 0) as total_income
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Stage_Master" s
                ON p.status_id = s.stage_id
            WHERE p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
            GROUP BY p.city
            ORDER BY count DESC
        ''')
        
        results = session.execute(query, {'tenant_id': tenant_id}).fetchall()
        
        breakdown = []
        for r in results:
            total = r.count or 1
            occupancy_rate = round((r.occupied / total * 100), 1) if total > 0 else 0.0
            
            breakdown.append({
                'city': r.city,
                'total_properties': r.count or 0,
                'occupied': r.occupied or 0,
                'available': r.available or 0,
                'total_income': float(r.total_income or 0),
                'occupancy_rate': occupancy_rate
            })
        
        return jsonify({
            'success': True,
            'breakdown': breakdown
        }), 200
        
    except Exception as e:
        logger.exception("❌ Error fetching location breakdown")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()


# ========================================
# PROPERTY TYPE BREAKDOWN
# ========================================

@dashboard_bp.route('/property-type-breakdown', methods=['GET', 'OPTIONS'])
@token_required
def get_property_type_breakdown():
    """
    Properties grouped by type (apartment, house, commercial, etc.)
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        user = g.user
        tenant_id = user.tenant_id
        
        query = text('''
            SELECT
                COALESCE(p.property_type, 'Unknown') as property_type,
                COUNT(p.property_id) as count,
                COUNT(CASE WHEN s.stage_name = 'Occupied' THEN 1 END) as occupied,
                COALESCE(AVG(p.monthly_rent), 0) as avg_rent,
                COALESCE(SUM(CASE 
                    WHEN s.stage_name = 'Occupied' THEN p.monthly_rent 
                    ELSE 0 
                END), 0) as total_income
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Stage_Master" s
                ON p.status_id = s.stage_id
            WHERE p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
            GROUP BY p.property_type
            ORDER BY count DESC
        ''')
        
        results = session.execute(query, {'tenant_id': tenant_id}).fetchall()
        
        breakdown = []
        for r in results:
            breakdown.append({
                'property_type': r.property_type,
                'count': r.count or 0,
                'occupied': r.occupied or 0,
                'avg_rent': float(r.avg_rent or 0),
                'total_income': float(r.total_income or 0)
            })
        
        return jsonify({
            'success': True,
            'breakdown': breakdown
        }), 200
        
    except Exception as e:
        logger.exception("❌ Error fetching property type breakdown")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()


# ========================================
# RECENT ACTIVITY
# ========================================

@dashboard_bp.route('/recent-activity', methods=['GET', 'OPTIONS'])
@token_required
def get_recent_activity():
    """
    Recent property activity (last 30 days)
    - New properties added
    - Status changes
    - Assignments
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        user = g.user
        tenant_id = user.tenant_id
        
        # Recent properties (last 30 days)
        query = text('''
            SELECT
                p.property_id,
                p.property_name,
                p.address,
                p.city,
                s.stage_name as status,
                em.employee_name as agent_name,
                p.created_at,
                p.monthly_rent
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Stage_Master" s
                ON p.status_id = s.stage_id
            LEFT JOIN "StreemLyne_MT"."Employee_Master" em
                ON p.assigned_agent_id = em.employee_id
            WHERE p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
            AND p.created_at >= CURRENT_DATE - INTERVAL '30 days'
            ORDER BY p.created_at DESC
            LIMIT 20
        ''')
        
        results = session.execute(query, {'tenant_id': tenant_id}).fetchall()
        
        activity = []
        for r in results:
            activity.append({
                'property_id': r.property_id,
                'property_name': r.property_name,
                'address': r.address,
                'city': r.city,
                'status': r.status,
                'agent_name': r.agent_name,
                'created_at': r.created_at.isoformat() if r.created_at else None,
                'monthly_rent': float(r.monthly_rent or 0)
            })
        
        return jsonify({
            'success': True,
            'activity': activity,
            'count': len(activity)
        }), 200
        
    except Exception as e:
        logger.exception("❌ Error fetching recent activity")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()