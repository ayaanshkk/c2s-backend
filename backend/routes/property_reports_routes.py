# -*- coding: utf-8 -*-
"""
Property Reports Routes
Monthly performance and financial analytics
"""
from flask import Blueprint, jsonify, request
from backend.routes.auth_helpers import token_required, get_current_tenant_id
from backend.properties.supabase_client import supabase
import logging
import calendar

logger = logging.getLogger(__name__)

property_reports_bp = Blueprint('property_reports', __name__)

SCHEMA_PM = "StreemLyne_MT"
PAYMENTS_TABLE = f'"{SCHEMA_PM}"."Property_Payments"'


@property_reports_bp.route('/api/properties/<int:property_id>/monthly-performance', methods=['GET'])
@token_required
def get_property_monthly_performance(property_id):
    """
    Get monthly performance for a specific property
    Query params: year, month (optional - defaults to current)
    """
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({'error': 'Invalid tenant context'}), 403
        
        year = request.args.get('year', type=int)
        month = request.args.get('month', type=int)
        
        if not year or not month:
            from datetime import datetime
            now = datetime.now()
            year = year or now.year
            month = month or now.month
        
        # Format month as YYYY-MM
        month_str = f"{year}-{month:02d}"
        
        # Get property details
        property_query = f'''
            SELECT 
                property_id,
                property_name,
                property_purchase_name,
                monthly_rent,
                occupancy_status,
                monthly_mortgage_payment
            FROM "{SCHEMA_PM}"."Property_Master"
            WHERE property_id = %s
            AND tenant_id = %s
            AND is_deleted = FALSE
        '''
        
        property_data = supabase.execute_query(
            property_query, 
            (property_id, tenant_id), 
            fetch_one=True
        )
        
        if not property_data:
            return jsonify({'error': 'Property not found'}), 404
        
        # Get payment record for this month
        payment_query = f'''
            SELECT 
                amount as rent_collected,
                total_rent as expected_rent,
                status
            FROM {PAYMENTS_TABLE}
            WHERE property_id = %s
            AND tenant_id = %s
            AND month = %s
        '''
        
        payment_result = supabase.execute_query(
            payment_query,
            (property_id, tenant_id, month_str),
            fetch_one=True
        )
        
        # Calculate rent values
        monthly_rent = property_data.get('monthly_rent', 0) or 0
        
        if payment_result:
            # Use data from payment record
            rent_collected = payment_result.get('rent_collected', 0) or 0
            expected_rent = payment_result.get('expected_rent', 0) or monthly_rent
        else:
            # No payment record for this month
            rent_collected = 0
            is_occupied = property_data.get('occupancy_status', '').lower() == 'occupied'
            expected_rent = monthly_rent if is_occupied else 0
        
        # Get maintenance expenses for this month
        # Calculate month range
        start_date = f"{year}-{month:02d}-01"
        last_day = calendar.monthrange(year, month)[1]
        end_date = f"{year}-{month:02d}-{last_day}"
        
        maintenance_query = f'''
            SELECT COALESCE(SUM(amount), 0) as total_maintenance
            FROM "{SCHEMA_PM}"."Maintenance_Expense"
            WHERE property_id = %s
            AND tenant_id = %s
            AND expense_date >= %s
            AND expense_date <= %s
        '''
        
        try:
            maintenance_result = supabase.execute_query(
                maintenance_query,
                (property_id, tenant_id, start_date, end_date),
                fetch_one=True
            )
            maintenance = maintenance_result.get('total_maintenance', 0) if maintenance_result else 0
        except Exception as e:
            logger.warning(f"Maintenance query failed: {e}. Using 0 for maintenance.")
            maintenance = 0
        
        # Get mortgage payment
        mortgage = property_data.get('monthly_mortgage_payment', 0) or 0
        
        # Calculate net income
        net_income = rent_collected - maintenance - mortgage
        expected_net = expected_rent - maintenance - mortgage
        
        # Calculate collection rate
        collection_rate = (rent_collected / expected_rent * 100) if expected_rent > 0 else 0
        
        return jsonify({
            'success': True,
            'property_id': property_id,
            'property_name': property_data.get('property_name'),
            'year': year,
            'month': month,
            'month_name': calendar.month_name[month],
            'performance': {
                'rent_collected': float(rent_collected),
                'expected_rent': float(expected_rent),
                'maintenance': float(maintenance),
                'mortgage': float(mortgage),
                'net_income': float(net_income),
                'expected_net': float(expected_net),
                'collection_rate': float(collection_rate)
            }
        })
        
    except Exception as e:
        logger.error(f"Error fetching monthly performance for property {property_id}: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500