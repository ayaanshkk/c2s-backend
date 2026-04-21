# -*- coding: utf-8 -*-
"""
Property Dashboard Routes
Analytics and statistics for property management
"""
from flask import Blueprint, request, jsonify, g
from backend.routes.auth_helpers import token_required, get_current_tenant_id
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
    """Main dashboard overview statistics"""
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context',
            }), 403

        # Optional agent filter
        agent_id = request.args.get('agent_id', type=int)
        agent_filter = "AND p.assigned_agent_id = :agent_id" if agent_id else ""
        params = {'tenant_id': tenant_id}
        if agent_id:
            params['agent_id'] = agent_id
        
        # Main stats query
        stats_query = text(f'''
            SELECT 
                COUNT(*) as total_properties,
                COUNT(CASE WHEN s.stage_name = 'Available' THEN 1 END) as available,
                COUNT(CASE WHEN p.occupancy_status = 'Occupied' THEN 1 END) as occupied,
                COUNT(CASE WHEN s.stage_name = 'Under Maintenance' THEN 1 END) as maintenance,
                COUNT(CASE WHEN s.stage_name = 'Listed' THEN 1 END) as listed,
                COUNT(CASE WHEN s.stage_name = 'Reserved' THEN 1 END) as reserved,
                COUNT(CASE WHEN p.assigned_agent_id IS NOT NULL THEN 1 END) as assigned,
                COALESCE(SUM(CASE 
                    WHEN p.occupancy_status = 'Occupied' AND p.monthly_rent IS NOT NULL 
                    THEN p.monthly_rent 
                    ELSE 0 
                END), 0) as total_monthly_income,
                COALESCE(AVG(p.monthly_rent), 0) as avg_monthly_rent,
                COALESCE(SUM(p.purchase_price), 0) as total_purchase_value
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Stage_Master" s 
                ON p.status_id = s.stage_id
            WHERE p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
            {agent_filter}
        ''')
        
        stats = session.execute(stats_query, params).first()
        
        # Get total expenses across all properties
        expenses_query = text('''
            SELECT COALESCE(SUM(e.amount), 0) as total_expenses
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Property_Expenses" e
                ON p.property_id = e.property_id
                AND p.tenant_id = e.tenant_id
            WHERE p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
        ''')
        
        expenses_result = session.execute(expenses_query, {'tenant_id': tenant_id}).first()
        total_expenses = float(expenses_result.total_expenses or 0)
        
        # Get total rent collected across all properties
        rent_collected_query = text('''
            SELECT COALESCE(SUM(pp.amount), 0) as total_collected
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Property_Payments" pp
                ON p.property_id = pp.property_id
                AND p.tenant_id = pp.tenant_id
            WHERE p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
            AND pp.status = 'PAID'
        ''')
        
        rent_collected_result = session.execute(rent_collected_query, {'tenant_id': tenant_id}).first()
        total_rent_collected = float(rent_collected_result.total_collected or 0)
        
        # Get total rent pending across all properties (only past months)
        # Dynamically calculate current financial year
        now = datetime.now()
        if now.month >= 4:  # April onwards = current year to next year
            fy_start = f"{now.year}-04-01"
            fy_end = f"{now.year + 1}-03-01"
        else:  # Jan-March = previous year to current year
            fy_start = f"{now.year - 1}-04-01"
            fy_end = f"{now.year}-03-01"
        
        current_month = now.strftime('%Y-%m')
        
        rent_pending_query = text('''
            WITH month_count AS (
                SELECT 
                    p.property_id,
                    COUNT(*) as months_passed
                FROM "StreemLyne_MT"."Property_Master" p
                CROSS JOIN generate_series(
                    CAST(:fy_start AS date),
                    LEAST(CURRENT_DATE, CAST(:fy_end AS date)),
                    '1 month'::interval
                ) AS month
                WHERE p.tenant_id = :tenant_id 
                    AND p.is_deleted = FALSE
                    AND p.monthly_rent > 0
                GROUP BY p.property_id
            )
            SELECT 
                p.property_id,
                p.monthly_rent,
                COALESCE(mc.months_passed, 0) as months_passed,
                COALESCE(SUM(pp.amount), 0) as total_paid,
                (p.monthly_rent * COALESCE(mc.months_passed, 0)) - COALESCE(SUM(pp.amount), 0) as pending
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN month_count mc ON mc.property_id = p.property_id
            LEFT JOIN "StreemLyne_MT"."Property_Payments" pp
                ON p.property_id = pp.property_id
                AND p.tenant_id = pp.tenant_id
                AND pp.month <= :current_month
            WHERE p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
            AND p.monthly_rent > 0
            GROUP BY p.property_id, p.monthly_rent, mc.months_passed
        ''')
        
        rent_pending_results = session.execute(
            rent_pending_query, 
            {
                'tenant_id': tenant_id,
                'fy_start': fy_start,
                'fy_end': fy_end,
                'current_month': current_month
            }
        ).fetchall()
        total_rent_pending = sum(max(0, float(r.pending or 0)) for r in rent_pending_results)
        
        # Occupancy rate
        total = stats.total_properties or 1
        occupancy_rate = round((stats.occupied / total * 100), 1) if total > 0 else 0.0
        
        # Recent activity
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
                'recent_properties_30d': recent_properties,
                'total_expenses': total_expenses,
                'total_rent_collected': total_rent_collected,
                'total_rent_pending': total_rent_pending,
                'total_purchase_value': float(stats.total_purchase_value or 0),  
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
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context',
                'message': 'tenant_id missing in token or X-Tenant-ID mismatch',
            }), 403

        query = text('''
            SELECT
                em.employee_id,
                em.employee_name,
                COUNT(p.property_id) as total_properties,
                COUNT(CASE WHEN p.occupancy_status = 'Occupied' THEN 1 END) as occupied,
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
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context',
                'message': 'tenant_id missing in token or X-Tenant-ID mismatch',
            }), 403

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
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context',
                'message': 'tenant_id missing in token or X-Tenant-ID mismatch',
            }), 403

        query = text('''
            SELECT
                COALESCE(p.city, 'Unknown') as city,
                COUNT(p.property_id) as count,
                COUNT(CASE WHEN p.occupancy_status = 'Occupied' THEN 1 END) as occupied,
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
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context',
                'message': 'tenant_id missing in token or X-Tenant-ID mismatch',
            }), 403

        query = text('''
            SELECT
                COALESCE(p.property_type, 'Unknown') as property_type,
                COUNT(p.property_id) as count,
                COUNT(CASE WHEN p.occupancy_status = 'Occupied' THEN 1 END) as occupied,
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
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context',
                'message': 'tenant_id missing in token or X-Tenant-ID mismatch',
            }), 403

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

@dashboard_bp.route('/expenses-breakdown', methods=['GET', 'OPTIONS'])
@token_required
def get_expenses_breakdown():
    """
    Get expenses breakdown by property
    Returns list of properties with their total expenses
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
            }), 403

        query = text('''
            SELECT 
                p.property_id,
                p.property_name,
                p.address,
                p.city,
                COALESCE(SUM(e.amount), 0) as total_expenses,
                COUNT(e.id) as expense_count
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Property_Expenses" e
                ON p.property_id = e.property_id
                AND p.tenant_id = e.tenant_id
            WHERE p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
            GROUP BY p.property_id, p.property_name, p.address, p.city
            HAVING COUNT(e.id) > 0
            ORDER BY total_expenses DESC
        ''')
        
        result = session.execute(query, {'tenant_id': tenant_id}).fetchall()
        
        breakdown = []
        for row in result:
            breakdown.append({
                'property_id': row.property_id,
                'property_name': row.property_name,
                'address': row.address,
                'city': row.city,
                'total_expenses': float(row.total_expenses or 0),
                'expense_count': row.expense_count or 0
            })
        
        return jsonify({
            'success': True,
            'breakdown': breakdown,
            'total': sum(b['total_expenses'] for b in breakdown)
        }), 200
        
    except Exception as e:
        logger.exception("❌ Error fetching expenses breakdown")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()

@dashboard_bp.route('/rent-collection-breakdown', methods=['GET', 'OPTIONS'])
@token_required
def get_rent_collection_breakdown():
    """
    Get rent collection breakdown by property
    Returns list of properties with their total collected rent
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
            }), 403

        query = text('''
            SELECT 
                p.property_id,
                p.property_name,
                p.address,
                p.city,
                p.monthly_rent,
                COALESCE(SUM(CASE WHEN pp.status = 'PAID' THEN pp.amount ELSE 0 END), 0) as total_collected,
                COUNT(CASE WHEN pp.status = 'PAID' THEN 1 END) as payment_count
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Property_Payments" pp
                ON p.property_id = pp.property_id
                AND p.tenant_id = pp.tenant_id
            WHERE p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
            GROUP BY p.property_id, p.property_name, p.address, p.city, p.monthly_rent
            HAVING COUNT(CASE WHEN pp.status = 'PAID' THEN 1 END) > 0
            ORDER BY total_collected DESC
        ''')
        
        result = session.execute(query, {'tenant_id': tenant_id}).fetchall()
        
        breakdown = []
        for row in result:
            breakdown.append({
                'property_id': row.property_id,
                'property_name': row.property_name,
                'address': row.address,
                'city': row.city,
                'monthly_rent': float(row.monthly_rent or 0),
                'total_collected': float(row.total_collected or 0),
                'payment_count': row.payment_count or 0
            })
        
        return jsonify({
            'success': True,
            'breakdown': breakdown,
            'total': sum(b['total_collected'] for b in breakdown)
        }), 200
        
    except Exception as e:
        logger.exception("❌ Error fetching rent collection breakdown")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()