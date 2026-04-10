import logging
import json
from datetime import datetime
from flask import Blueprint, request, jsonify, g
from sqlalchemy import text
from backend.db import SessionLocal
from backend.routes.auth_helpers import (
    token_required,
    require_admin,
    get_current_tenant_id,
)

logger = logging.getLogger(__name__)

property_expense_bp = Blueprint('property_expense', __name__)

SCHEMA = "StreemLyne_MT"
EXPENSES_TABLE = f'"{SCHEMA}"."Property_Expenses"'

@property_expense_bp.route('/<int:property_id>/expenses', methods=['GET', 'OPTIONS'])
@token_required
def get_property_expenses(property_id):
    """Get all expenses for a property"""
    if request.method == 'OPTIONS':
        return '', 204
    
    tenant_id = get_current_tenant_id()
    if not tenant_id:
        return jsonify({'success': False, 'error': 'Invalid tenant context'}), 403
    
    session = SessionLocal()
    try:
        # Verify property belongs to tenant
        property_check = session.execute(
            text(f'''
                SELECT 1 FROM "{SCHEMA}"."Property_Master"
                WHERE property_id = :pid AND tenant_id = :tid AND is_deleted = FALSE
            '''),
            {'pid': property_id, 'tid': tenant_id}
        ).first()
        
        if not property_check:
            return jsonify({'success': False, 'error': 'Property not found'}), 404
        
        # Get expenses - FIXED: Added invoice_url to SELECT
        rows = session.execute(
            text(f'''
                SELECT 
                    id,
                    property_id,
                    expense_date,
                    expense_type,
                    amount,
                    notes,
                    invoice_url,
                    created_at
                FROM {EXPENSES_TABLE}
                WHERE property_id = :pid AND tenant_id = :tid
                ORDER BY expense_date DESC
            '''),
            {'pid': property_id, 'tid': tenant_id}
        )
        
        expenses = []
        for r in rows:
            expenses.append({
                'expense_id': str(r.id),  # UUID to string
                'property_id': r.property_id,
                'expense_date': r.expense_date.isoformat() if r.expense_date else None,
                'expense_type': r.expense_type,
                'amount': float(r.amount) if r.amount else 0,
                'notes': r.notes,
                'invoice_url': r.invoice_url,
                'created_at': r.created_at.isoformat() if r.created_at else None,
            })
        
        return jsonify({
            'success': True,
            'expenses': expenses,
            'count': len(expenses)
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching expenses: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        session.close()

@property_expense_bp.route('/<int:property_id>/expenses', methods=['POST', 'OPTIONS'])
@token_required
def create_property_expense(property_id):
    """Create a new expense for a property"""
    if request.method == 'OPTIONS':
        return '', 204
    
    tenant_id = get_current_tenant_id()
    if not tenant_id:
        return jsonify({'success': False, 'error': 'Invalid tenant context'}), 403
    
    data = request.get_json()
    
    session = SessionLocal()
    try:
        # Verify property belongs to tenant
        property_check = session.execute(
            text(f'''
                SELECT 1 FROM "{SCHEMA}"."Property_Master"
                WHERE property_id = :pid AND tenant_id = :tid AND is_deleted = FALSE
            '''),
            {'pid': property_id, 'tid': tenant_id}
        ).first()
        
        if not property_check:
            return jsonify({'success': False, 'error': 'Property not found'}), 404
        
        # Validate required fields
        if not data.get('expense_date'):
            return jsonify({'success': False, 'error': 'expense_date is required'}), 400
        
        if not data.get('expense_type'):
            return jsonify({'success': False, 'error': 'expense_type is required'}), 400
        
        amount = float(data.get('amount', 0))
        if amount < 0:
            return jsonify({'success': False, 'error': 'amount must be >= 0'}), 400
        
        # FIXED: Added invoice_url to INSERT statement
        result = session.execute(
            text(f'''
                INSERT INTO {EXPENSES_TABLE} (
                    tenant_id,
                    property_id,
                    expense_date,
                    expense_type,
                    amount,
                    notes,
                    invoice_url
                )
                VALUES (
                    :tid, :pid, :exp_date, :exp_type, :amt, :notes, :invoice
                )
                RETURNING id
            '''),
            {
                'tid': tenant_id,
                'pid': property_id,
                'exp_date': data.get('expense_date'),
                'exp_type': data.get('expense_type'),
                'amt': amount,
                'notes': data.get('notes'),
                'invoice': data.get('invoice_url'),  # FIXED: was missing
            }
        )
        
        expense_id = result.scalar()
        session.commit()
        
        return jsonify({
            'success': True,
            'expense_id': str(expense_id),
            'message': 'Expense created successfully'
        }), 201
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error creating expense: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        session.close()

@property_expense_bp.route('/<int:property_id>/expenses/<expense_id>', methods=['PUT', 'OPTIONS'])
@token_required
def update_property_expense(property_id, expense_id):
    """Update an expense"""
    if request.method == 'OPTIONS':
        return '', 204
    
    tenant_id = get_current_tenant_id()
    if not tenant_id:
        return jsonify({'success': False, 'error': 'Invalid tenant context'}), 403
    
    data = request.get_json()
    
    session = SessionLocal()
    try:
        amount = float(data.get('amount', 0))
        if amount < 0:
            return jsonify({'success': False, 'error': 'amount must be >= 0'}), 400
        
        # FIXED: Added invoice_url to UPDATE statement
        result = session.execute(
            text(f'''
                UPDATE {EXPENSES_TABLE}
                SET 
                    expense_date = :exp_date,
                    expense_type = :exp_type,
                    amount = :amt,
                    notes = :notes,
                    invoice_url = :invoice
                WHERE id = :eid 
                  AND property_id = :pid 
                  AND tenant_id = :tid
                RETURNING id
            '''),
            {
                'exp_date': data.get('expense_date'),
                'exp_type': data.get('expense_type'),
                'amt': amount,
                'notes': data.get('notes'),
                'invoice': data.get('invoice_url'),  # FIXED: now properly mapped
                'eid': expense_id,
                'pid': property_id,
                'tid': tenant_id
            }
        )
        
        if result.scalar() is None:
            session.rollback()
            return jsonify({'success': False, 'error': 'Expense not found'}), 404
        
        session.commit()
        
        return jsonify({
            'success': True,
            'message': 'Expense updated successfully'
        }), 200
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating expense: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        session.close()

@property_expense_bp.route('/<int:property_id>/expenses/<expense_id>', methods=['DELETE', 'OPTIONS'])
@token_required
def delete_property_expense(property_id, expense_id):
    """Delete an expense"""
    if request.method == 'OPTIONS':
        return '', 204
    
    tenant_id = get_current_tenant_id()
    if not tenant_id:
        return jsonify({'success': False, 'error': 'Invalid tenant context'}), 403
    
    session = SessionLocal()
    try:
        result = session.execute(
            text(f'''
                DELETE FROM {EXPENSES_TABLE}
                WHERE id = :eid 
                  AND property_id = :pid 
                  AND tenant_id = :tid
                RETURNING id
            '''),
            {'eid': expense_id, 'pid': property_id, 'tid': tenant_id}
        )
        
        if result.scalar() is None:
            session.rollback()
            return jsonify({'success': False, 'error': 'Expense not found'}), 404
        
        session.commit()
        
        return jsonify({
            'success': True,
            'message': 'Expense deleted successfully'
        }), 200
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error deleting expense: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        session.close()