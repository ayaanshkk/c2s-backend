"""
Property analytics routes for rent collection and pending rent calculations
"""

import logging
from flask import Blueprint, jsonify, request
from sqlalchemy import text
from datetime import datetime
from backend.db import SessionLocal
from backend.routes.auth_helpers import token_required, get_current_tenant_id

logger = logging.getLogger(__name__)

analytics_bp = Blueprint('analytics', __name__, url_prefix='/api/analytics')

PROPERTY_TABLE = '"StreemLyne_MT"."Property_Master"'
PAYMENTS_TABLE = '"StreemLyne_MT"."Property_Payments"'


@analytics_bp.route('/total-rent-pending', methods=['GET', 'OPTIONS'])
@token_required
def get_total_rent_pending():
    """
    Calculate total pending rent across all properties for a financial year.
    Only counts months that have ALREADY PASSED (not future months).
    
    Query params:
    - year: Financial year (e.g., "2025-2026")
    """
    if request.method == 'OPTIONS':
        return '', 204

    tenant_id = get_current_tenant_id()
    if not tenant_id:
        return jsonify({
            'success': False,
            'error': 'Invalid tenant context'
        }), 403

    financial_year = request.args.get('year')
    if not financial_year:
        return jsonify({
            'success': False,
            'error': 'year parameter is required (format: YYYY-YYYY)'
        }), 400

    session = SessionLocal()
    try:
        # Parse financial year
        try:
            start_year, end_year = financial_year.split('-')
            start_month = f"{start_year}-04-01"
            end_month = f"{end_year}-03-01"
        except ValueError:
            return jsonify({
                'success': False,
                'error': 'Invalid year format. Expected YYYY-YYYY'
            }), 400

        # Get current month (only count up to current month, not future)
        current_month = datetime.now().strftime('%Y-%m')
        
        # Calculate pending rent only for months that have passed
        query = text(f"""
            WITH month_count AS (
                SELECT 
                    p.property_id,
                    COUNT(*) as months_passed
                FROM {PROPERTY_TABLE} p
                CROSS JOIN generate_series(
                    CAST(:start_m AS date),
                    LEAST(CURRENT_DATE, CAST(:end_m AS date)),
                    '1 month'::interval
                ) AS month
                WHERE p.tenant_id = :tid 
                    AND p.is_deleted = false
                    AND p.monthly_rent > 0
                GROUP BY p.property_id
            )
            SELECT 
                p.property_id,
                p.display_id,
                p.property_name,
                p.address,
                p.monthly_rent,
                p.occupancy_status,
                COALESCE(mc.months_passed, 0) as months_passed,
                COALESCE(SUM(pay.amount), 0) as total_paid,
                (p.monthly_rent * COALESCE(mc.months_passed, 0)) - COALESCE(SUM(pay.amount), 0) as total_pending
            FROM {PROPERTY_TABLE} p
            LEFT JOIN month_count mc ON mc.property_id = p.property_id
            LEFT JOIN {PAYMENTS_TABLE} pay ON 
                pay.property_id = p.property_id 
                AND pay.tenant_id = :tid
                AND pay.month >= TO_CHAR(CAST(:start_m AS date), 'YYYY-MM')
                AND pay.month <= :current_m
            WHERE p.tenant_id = :tid 
                AND p.is_deleted = false
                AND p.monthly_rent > 0
            GROUP BY 
                p.property_id, 
                p.display_id,
                p.property_name, 
                p.address, 
                p.monthly_rent,
                p.occupancy_status,
                mc.months_passed
            HAVING (p.monthly_rent * COALESCE(mc.months_passed, 0)) - COALESCE(SUM(pay.amount), 0) > 0
            ORDER BY total_pending DESC
        """)

        rows = session.execute(
            query,
            {
                'tid': str(tenant_id),
                'start_m': start_month,
                'end_m': end_month,
                'current_m': current_month
            }
        )

        properties = []
        for r in rows:
            properties.append({
                'property_id': r.property_id,
                'display_id': r.display_id,
                'property_name': r.property_name,
                'address': r.address,
                'monthly_rent': float(r.monthly_rent) if r.monthly_rent else 0,
                'months_passed': int(r.months_passed) if r.months_passed else 0,
                'total_paid': float(r.total_paid) if r.total_paid else 0,
                'total_pending': float(r.total_pending) if r.total_pending else 0,
                'occupancy_status': r.occupancy_status,
            })

        total_pending = sum(p['total_pending'] for p in properties)

        return jsonify({
            'success': True,
            'properties': properties,
            'total_pending': total_pending,
            'count': len(properties),
            'year': financial_year,
        }), 200

    except Exception as e:
        session.rollback()
        logger.exception('get_total_rent_pending: %s', e)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()


@analytics_bp.route('/total-rent-collected', methods=['GET', 'OPTIONS'])
@token_required
def get_total_rent_collected():
    """
    Calculate total rent collected across all properties for a financial year.
    Returns list of properties with their collection amounts.
    
    Query params:
    - year: Financial year (e.g., "2025-2026")
    """
    if request.method == 'OPTIONS':
        return '', 204

    tenant_id = get_current_tenant_id()
    if not tenant_id:
        return jsonify({
            'success': False,
            'error': 'Invalid tenant context'
        }), 403

    financial_year = request.args.get('year')
    if not financial_year:
        return jsonify({
            'success': False,
            'error': 'year parameter is required (format: YYYY-YYYY)'
        }), 400

    session = SessionLocal()
    try:
        # Parse financial year
        try:
            start_year, end_year = financial_year.split('-')
            start_month = f"{start_year}-04"
            end_month = f"{end_year}-03"
        except ValueError:
            return jsonify({
                'success': False,
                'error': 'Invalid year format. Expected YYYY-YYYY'
            }), 400

        # Get all properties with their payment totals
        query = text(f"""
            SELECT 
                p.property_id,
                p.display_id,
                p.property_name,
                p.address,
                p.monthly_rent,
                p.occupancy_status,
                COALESCE(SUM(pay.amount), 0) as total_collected
            FROM {PROPERTY_TABLE} p
            LEFT JOIN {PAYMENTS_TABLE} pay ON 
                pay.property_id = p.property_id 
                AND pay.tenant_id = :tid
                AND pay.month >= :start_m 
                AND pay.month <= :end_m
            WHERE p.tenant_id = :tid 
                AND p.is_deleted = false
            GROUP BY 
                p.property_id, 
                p.display_id,
                p.property_name, 
                p.address, 
                p.monthly_rent,
                p.occupancy_status
            HAVING COALESCE(SUM(pay.amount), 0) > 0
            ORDER BY total_collected DESC
        """)

        rows = session.execute(
            query,
            {
                'tid': str(tenant_id),
                'start_m': start_month,
                'end_m': end_month
            }
        )

        properties = []
        for r in rows:
            properties.append({
                'property_id': r.property_id,
                'display_id': r.display_id,
                'property_name': r.property_name,
                'address': r.address,
                'monthly_rent': float(r.monthly_rent) if r.monthly_rent else 0,
                'total_collected': float(r.total_collected) if r.total_collected else 0,
                'occupancy_status': r.occupancy_status,
            })

        total_collected = sum(p['total_collected'] for p in properties)

        return jsonify({
            'success': True,
            'properties': properties,
            'total_collected': total_collected,
            'count': len(properties),
            'year': financial_year,
        }), 200

    except Exception as e:
        session.rollback()
        logger.exception('get_total_rent_collected: %s', e)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()


@analytics_bp.route('/total-property-value', methods=['GET', 'OPTIONS'])
@token_required
def get_total_property_value():
    """
    Get total property value breakdown.
    Returns list of all properties with their purchase prices.
    """
    if request.method == 'OPTIONS':
        return '', 204

    tenant_id = get_current_tenant_id()
    if not tenant_id:
        return jsonify({
            'success': False,
            'error': 'Invalid tenant context'
        }), 403

    session = SessionLocal()
    try:
        # Get all properties with their purchase prices
        query = text(f"""
            SELECT 
                p.property_id,
                p.display_id,
                p.property_name,
                p.address,
                p.purchase_price,
                p.occupancy_status
            FROM {PROPERTY_TABLE} p
            WHERE p.tenant_id = :tid 
                AND p.is_deleted = false
                AND p.purchase_price IS NOT NULL
                AND p.purchase_price > 0
            ORDER BY p.purchase_price DESC
        """)

        rows = session.execute(query, {'tid': str(tenant_id)})

        properties = []
        for r in rows:
            properties.append({
                'property_id': r.property_id,
                'display_id': r.display_id,
                'property_name': r.property_name,
                'address': r.address,
                'purchase_price': float(r.purchase_price) if r.purchase_price else 0,
                'occupancy_status': r.occupancy_status,
            })

        total_value = sum(p['purchase_price'] for p in properties)

        return jsonify({
            'success': True,
            'properties': properties,
            'total_value': total_value,
            'count': len(properties),
        }), 200

    except Exception as e:
        session.rollback()
        logger.exception('get_total_property_value: %s', e)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()