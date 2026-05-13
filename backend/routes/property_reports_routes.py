# -*- coding: utf-8 -*-
"""
Property Reports Routes
Monthly performance and financial analytics with statement generation
"""
from flask import Blueprint, jsonify, request
from backend.routes.auth_helpers import token_required, get_current_tenant_id
from backend.properties.supabase_client import supabase
import logging
import calendar
from datetime import datetime

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
            # Expected rent comes from property master
            expected_rent = monthly_rent
        else:
            # No payment record for this month
            rent_collected = 0
            is_occupied = property_data.get('occupancy_status', '').lower() == 'occupied'
            expected_rent = monthly_rent if is_occupied else 0
        
        # Get maintenance expenses for this month (if table exists)
        # Calculate month range
        start_date = f"{year}-{month:02d}-01"
        last_day = calendar.monthrange(year, month)[1]
        end_date = f"{year}-{month:02d}-{last_day}"
        
        # Try to query maintenance expenses, but handle if table doesn't exist yet
        maintenance = 0
        try:
            maintenance_query = f'''
                SELECT COALESCE(SUM(amount), 0) as total_maintenance
                FROM "{SCHEMA_PM}"."Maintenance_Expense"
                WHERE property_id = %s
                AND tenant_id = %s
                AND expense_date >= %s
                AND expense_date <= %s
            '''
            maintenance_result = supabase.execute_query(
                maintenance_query,
                (property_id, tenant_id, start_date, end_date),
                fetch_one=True
            )
            maintenance = maintenance_result.get('total_maintenance', 0) if maintenance_result else 0
        except Exception as e:
            # Table might not exist yet - that's okay, just use 0
            logger.debug(f"Maintenance table not available: {e}")
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


@property_reports_bp.route('/api/properties/<int:property_id>/statement', methods=['GET'])
@token_required
def get_property_statement(property_id):
    """
    Get statement-style report for a property over a date range
    Query params: from_year, from_month, to_year, to_month
    Returns all transactions (rent payments, maintenance) in chronological order
    """
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({'error': 'Invalid tenant context'}), 403
        
        # Get date range parameters
        from_year = request.args.get('from_year', type=int)
        from_month = request.args.get('from_month', type=int)
        to_year = request.args.get('to_year', type=int)
        to_month = request.args.get('to_month', type=int)
        
        # Validate parameters
        if not all([from_year, from_month, to_year, to_month]):
            return jsonify({'error': 'Missing date range parameters'}), 400
        
        # Get property details
        property_query = f'''
            SELECT 
                property_id,
                property_name,
                property_purchase_name,
                address,
                city,
                postcode,
                monthly_rent,
                monthly_mortgage_payment,
                tenant_name
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
        
        # Calculate date range
        from_date = f"{from_year}-{from_month:02d}-01"
        to_last_day = calendar.monthrange(to_year, to_month)[1]
        to_date = f"{to_year}-{to_month:02d}-{to_last_day}"
        
        # Get all rent payments in the date range
        payments_query = f'''
            SELECT 
                month,
                amount,
                status,
                notes
            FROM {PAYMENTS_TABLE}
            WHERE property_id = %s
            AND tenant_id = %s
            AND month >= %s
            AND month <= %s
            ORDER BY month ASC
        '''
        
        from_month_str = f"{from_year}-{from_month:02d}"
        to_month_str = f"{to_year}-{to_month:02d}"
        
        # Fix: Use the correct method signature - execute_query returns all rows by default
        # when fetch_one is not specified
        payments_result = supabase.execute_query(
            payments_query,
            (property_id, tenant_id, from_month_str, to_month_str)
        )
        
        # Ensure payments_result is a list
        if payments_result and not isinstance(payments_result, list):
            payments_result = [payments_result]
        
        # Get all maintenance expenses in the date range (if table exists)
        maintenance_result = []
        try:
            maintenance_query = f'''
                SELECT 
                    expense_date,
                    amount,
                    category,
                    description,
                    vendor
                FROM "{SCHEMA_PM}"."Maintenance_Expense"
                WHERE property_id = %s
                AND tenant_id = %s
                AND expense_date >= %s
                AND expense_date <= %s
                ORDER BY expense_date ASC
            '''
            
            maintenance_result = supabase.execute_query(
                maintenance_query,
                (property_id, tenant_id, from_date, to_date)
            )
            
            # Ensure maintenance_result is a list
            if maintenance_result and not isinstance(maintenance_result, list):
                maintenance_result = [maintenance_result]
        except Exception as e:
            # Table might not exist yet - that's okay
            logger.debug(f"Maintenance table not available: {e}")
            maintenance_result = []
        
        # Build transaction list
        transactions = []
        
        # Add rent payments as income transactions
        for payment in (payments_result or []):
            month_str = payment.get('month', '')
            # Parse month string (YYYY-MM)
            if month_str:
                year_part, month_part = month_str.split('-')
                date_obj = datetime(int(year_part), int(month_part), 1)
                
                transactions.append({
                    'date': month_str + '-01',
                    'type': 'rent',
                    'description': f"Rent payment - {calendar.month_name[int(month_part)]} {year_part}",
                    'category': 'Rental Income',
                    'amount': float(payment.get('amount', 0) or 0),
                    'status': payment.get('status', 'unknown'),
                    'balance_impact': 'credit'
                })
        
        # Add maintenance expenses as debit transactions
        for expense in (maintenance_result or []):
            transactions.append({
                'date': str(expense.get('expense_date', '')),
                'type': 'maintenance',
                'description': expense.get('description', 'Maintenance expense'),
                'category': expense.get('category', 'Maintenance'),
                'vendor': expense.get('vendor', ''),
                'amount': float(expense.get('amount', 0) or 0),
                'balance_impact': 'debit'
            })
        
        # Sort all transactions by date
        transactions.sort(key=lambda x: x['date'])
        
        # Calculate running balance and totals
        running_balance = 0
        total_income = 0
        total_expenses = 0
        
        for transaction in transactions:
            if transaction['balance_impact'] == 'credit':
                running_balance += transaction['amount']
                total_income += transaction['amount']
            else:
                running_balance -= transaction['amount']
                total_expenses += transaction['amount']
            
            transaction['running_balance'] = running_balance
        
        # Calculate summary statistics
        months_in_range = []
        current_year = from_year
        current_month = from_month
        
        while (current_year < to_year) or (current_year == to_year and current_month <= to_month):
            months_in_range.append({
                'year': current_year,
                'month': current_month,
                'month_str': f"{current_year}-{current_month:02d}"
            })
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1
        
        mortgage_total = (property_data.get('monthly_mortgage_payment', 0) or 0) * len(months_in_range)
        net_income = total_income - total_expenses - mortgage_total
        
        return jsonify({
            'success': True,
            'property': {
                'property_id': property_id,
                'name': property_data.get('property_purchase_name') or property_data.get('property_name'),
                'address': property_data.get('address', ''),
                'city': property_data.get('city', ''),
                'postcode': property_data.get('postcode', ''),
                'tenant_name': property_data.get('tenant_name', ''),
                'monthly_rent': float(property_data.get('monthly_rent', 0) or 0),
                'monthly_mortgage': float(property_data.get('monthly_mortgage_payment', 0) or 0)
            },
            'period': {
                'from': f"{from_year}-{from_month:02d}",
                'to': f"{to_year}-{to_month:02d}",
                'from_label': f"{calendar.month_name[from_month]} {from_year}",
                'to_label': f"{calendar.month_name[to_month]} {to_year}",
                'months_count': len(months_in_range)
            },
            'summary': {
                'total_income': float(total_income),
                'total_expenses': float(total_expenses),
                'mortgage_total': float(mortgage_total),
                'net_income': float(net_income),
                'transaction_count': len(transactions)
            },
            'transactions': transactions
        })
        
    except Exception as e:
        logger.error(f"Error generating statement for property {property_id}: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500