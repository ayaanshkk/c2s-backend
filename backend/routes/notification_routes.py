import logging
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify, g
from sqlalchemy import text
from backend.db import SessionLocal
from backend.routes.auth_helpers import token_required, get_current_tenant_id

logger = logging.getLogger(__name__)

notifications_bp = Blueprint('notifications', __name__)

SCHEMA = "StreemLyne_MT"
NOTIFICATIONS_TABLE = f'"{SCHEMA}"."Notification_Master"'
PROPERTY_TABLE = f'"{SCHEMA}"."Property_Master"'

@notifications_bp.route('/', methods=['GET', 'OPTIONS'])
@token_required
def get_notifications():
    """Get all notifications for current user"""
    if request.method == 'OPTIONS':
        return '', 204
    
    session = SessionLocal()
    
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context'
            }), 403
        
        current_user = g.user
        employee_id = current_user.employee_id
        
        # Get filter parameters
        is_read = request.args.get('is_read')
        notification_type = request.args.get('type')
        limit = request.args.get('limit', 50, type=int)
        
        # Build query
        filters = ["n.tenant_id = :tenant_id", "n.dismissed = FALSE"]
        params = {'tenant_id': tenant_id}
        
        # Show notifications for this employee or property-level notifications
        filters.append("(n.employee_id = :employee_id OR n.employee_id IS NULL)")
        params['employee_id'] = employee_id
        
        if is_read is not None:
            filters.append("n.read = :is_read")
            params['is_read'] = is_read.lower() == 'true'
        
        if notification_type:
            filters.append("n.notification_type = :notification_type")
            params['notification_type'] = notification_type
        
        where_clause = " AND ".join(filters)
        
        query = text(f'''
            SELECT 
                n.notification_id,
                n.property_id,
                n.client_id,
                n.contract_id,
                p.property_name,
                n.notification_type,
                n.message,
                n.priority,
                n.read,
                n.dismissed,
                n.created_at,
                n.read_at
            FROM {NOTIFICATIONS_TABLE} n
            LEFT JOIN {PROPERTY_TABLE} p ON n.property_id = p.property_id
            WHERE {where_clause}
            ORDER BY 
                CASE WHEN n.priority = 'urgent' THEN 0 ELSE 1 END,
                n.created_at DESC
            LIMIT :limit
        ''')
        
        params['limit'] = limit
        
        result = session.execute(query, params)
        notifications = []
        
        for row in result:
            notifications.append({
                'id': str(row.notification_id),
                'notification_id': row.notification_id,
                'property_id': row.property_id,
                'client_id': row.client_id,
                'contract_id': row.contract_id,
                'property_name': row.property_name,
                'notification_type': row.notification_type,
                'message': row.message,
                'priority': row.priority,
                'read': row.read,
                'dismissed': row.dismissed,
                'created_at': row.created_at.isoformat() if row.created_at else None,
                'read_at': row.read_at.isoformat() if row.read_at else None,
            })
        
        # Get unread count
        unread_query = text(f'''
            SELECT COUNT(*) 
            FROM {NOTIFICATIONS_TABLE}
            WHERE tenant_id = :tenant_id 
            AND (employee_id = :employee_id OR employee_id IS NULL)
            AND read = FALSE
            AND dismissed = FALSE
        ''')
        
        unread_count = session.execute(
            unread_query, 
            {'tenant_id': tenant_id, 'employee_id': employee_id}
        ).scalar()
        
        return jsonify({
            'success': True,
            'notifications': notifications,
            'unread_count': unread_count or 0,
            'total': len(notifications)
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching notifications: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()

@notifications_bp.route('/<int:notification_id>/read', methods=['POST', 'OPTIONS'])
@token_required
def mark_notification_read(notification_id):
    """Mark a notification as read"""
    if request.method == 'OPTIONS':
        return '', 204
    
    session = SessionLocal()
    
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context'
            }), 403
        
        current_user = g.user
        employee_id = current_user.employee_id
        now = datetime.utcnow()
        
        result = session.execute(
            text(f'''
                UPDATE {NOTIFICATIONS_TABLE}
                SET read = TRUE, read_at = :read_at
                WHERE notification_id = :notification_id
                AND tenant_id = :tenant_id
                AND (employee_id = :employee_id OR employee_id IS NULL)
                RETURNING notification_id
            '''),
            {
                'notification_id': notification_id,
                'tenant_id': tenant_id,
                'employee_id': employee_id,
                'read_at': now
            }
        )
        
        if result.scalar() is None:
            session.rollback()
            return jsonify({
                'success': False,
                'error': 'Notification not found'
            }), 404
        
        session.commit()
        
        return jsonify({
            'success': True,
            'message': 'Notification marked as read'
        }), 200
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error marking notification as read: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()

@notifications_bp.route('/mark-all-read', methods=['POST', 'OPTIONS'])
@token_required
def mark_all_read():
    """Mark all notifications as read for current user"""
    if request.method == 'OPTIONS':
        return '', 204
    
    session = SessionLocal()
    
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context'
            }), 403
        
        current_user = g.user
        employee_id = current_user.employee_id
        now = datetime.utcnow()
        
        result = session.execute(
            text(f'''
                UPDATE {NOTIFICATIONS_TABLE}
                SET read = TRUE, read_at = :read_at
                WHERE tenant_id = :tenant_id
                AND (employee_id = :employee_id OR employee_id IS NULL)
                AND read = FALSE
                AND dismissed = FALSE
                RETURNING notification_id
            '''),
            {
                'tenant_id': tenant_id,
                'employee_id': employee_id,
                'read_at': now
            }
        )
        
        count = len(result.fetchall())
        session.commit()
        
        return jsonify({
            'success': True,
            'message': f'{count} notification(s) marked as read',
            'count': count
        }), 200
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error marking all notifications as read: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()

@notifications_bp.route('/<int:notification_id>/dismiss', methods=['POST', 'OPTIONS'])
@token_required
def dismiss_notification(notification_id):
    """Dismiss a notification"""
    if request.method == 'OPTIONS':
        return '', 204
    
    session = SessionLocal()
    
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context'
            }), 403
        
        current_user = g.user
        employee_id = current_user.employee_id
        
        result = session.execute(
            text(f'''
                UPDATE {NOTIFICATIONS_TABLE}
                SET dismissed = TRUE
                WHERE notification_id = :notification_id
                AND tenant_id = :tenant_id
                AND (employee_id = :employee_id OR employee_id IS NULL)
                RETURNING notification_id
            '''),
            {
                'notification_id': notification_id,
                'tenant_id': tenant_id,
                'employee_id': employee_id
            }
        )
        
        if result.scalar() is None:
            session.rollback()
            return jsonify({
                'success': False,
                'error': 'Notification not found'
            }), 404
        
        session.commit()
        
        return jsonify({
            'success': True,
            'message': 'Notification dismissed'
        }), 200
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error dismissing notification: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()

@notifications_bp.route('/<int:notification_id>', methods=['DELETE', 'OPTIONS'])
@token_required
def delete_notification(notification_id):
    """Delete a notification"""
    if request.method == 'OPTIONS':
        return '', 204
    
    session = SessionLocal()
    
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context'
            }), 403
        
        current_user = g.user
        employee_id = current_user.employee_id
        
        result = session.execute(
            text(f'''
                DELETE FROM {NOTIFICATIONS_TABLE}
                WHERE notification_id = :notification_id
                AND tenant_id = :tenant_id
                AND (employee_id = :employee_id OR employee_id IS NULL)
                RETURNING notification_id
            '''),
            {
                'notification_id': notification_id,
                'tenant_id': tenant_id,
                'employee_id': employee_id
            }
        )
        
        if result.scalar() is None:
            session.rollback()
            return jsonify({
                'success': False,
                'error': 'Notification not found'
            }), 404
        
        session.commit()
        
        return jsonify({
            'success': True,
            'message': 'Notification deleted'
        }), 200
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error deleting notification: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()

@notifications_bp.route('/clear-all', methods=['DELETE', 'OPTIONS'])
@token_required
def clear_all_notifications():
    """Clear all notifications for current user"""
    if request.method == 'OPTIONS':
        return '', 204
    
    session = SessionLocal()
    
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context'
            }), 403
        
        current_user = g.user
        employee_id = current_user.employee_id
        
        result = session.execute(
            text(f'''
                DELETE FROM {NOTIFICATIONS_TABLE}
                WHERE tenant_id = :tenant_id
                AND (employee_id = :employee_id OR employee_id IS NULL)
                RETURNING notification_id
            '''),
            {
                'tenant_id': tenant_id,
                'employee_id': employee_id
            }
        )
        
        count = len(result.fetchall())
        session.commit()
        
        return jsonify({
            'success': True,
            'message': f'{count} notification(s) deleted',
            'count': count
        }), 200
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error clearing notifications: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()

@notifications_bp.route('/generate-rent-reminders', methods=['POST', 'OPTIONS'])
@token_required
def generate_rent_reminders():
    """
    Generate rent reminder notifications for properties where rent is due in 7 days
    This should be called by a cron job daily
    """
    if request.method == 'OPTIONS':
        return '', 204
    
    session = SessionLocal()
    
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context'
            }), 403
        
        # Calculate the target date (7 days from now)
        target_date = datetime.utcnow() + timedelta(days=7)
        target_day = target_date.day
        
        # Find properties where rent_due_day matches the target day
        query = text(f'''
            SELECT 
                p.property_id,
                p.property_name,
                p.tenant_name,
                p.monthly_rent,
                p.rent_due_day,
                p.assigned_agent_id,
                p.address,
                p.city
            FROM {PROPERTY_TABLE} p
            WHERE p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
            AND p.occupancy_status = 'Occupied'
            AND p.rent_due_day = :target_day
            AND p.monthly_rent IS NOT NULL
            AND p.monthly_rent > 0
        ''')
        
        properties = session.execute(query, {
            'tenant_id': tenant_id,
            'target_day': target_day
        }).fetchall()
        
        created_count = 0
        now = datetime.utcnow()
        
        for prop in properties:
            # Check if notification already exists for this property this month
            existing = session.execute(
                text(f'''
                    SELECT 1 FROM {NOTIFICATIONS_TABLE}
                    WHERE tenant_id = :tenant_id
                    AND property_id = :property_id
                    AND notification_type = 'rent_reminder'
                    AND created_at >= DATE_TRUNC('month', CURRENT_DATE)
                    AND dismissed = FALSE
                    LIMIT 1
                '''),
                {
                    'tenant_id': tenant_id,
                    'property_id': prop.property_id
                }
            ).first()
            
            if existing:
                continue  # Skip if already notified this month
            
            # Create notification message
            location = f"{prop.address}, {prop.city}" if prop.city else prop.address
            message = (
                f"💰 Rent payment due in 7 days\n"
                f"🏠 Property: {prop.property_name}\n"
                f"📍 Location: {location}\n"
                f"👤 Tenant: {prop.tenant_name or 'N/A'}\n"
                f"💷 Amount: £{prop.monthly_rent:,.2f}\n"
                f"📅 Due: {target_date.strftime('%d %B %Y')}"
            )
            
            session.execute(
                text(f'''
                    INSERT INTO {NOTIFICATIONS_TABLE} (
                        tenant_id,
                        property_id,
                        employee_id,
                        client_id,
                        contract_id,
                        notification_type,
                        priority,
                        message,
                        read,
                        dismissed,
                        created_at
                    ) VALUES (
                        :tenant_id,
                        :property_id,
                        :employee_id,
                        NULL,
                        NULL,
                        'rent_reminder',
                        'normal',
                        :message,
                        FALSE,
                        FALSE,
                        :created_at
                    )
                '''),
                {
                    'tenant_id': tenant_id,
                    'property_id': prop.property_id,
                    'employee_id': prop.assigned_agent_id,
                    'message': message,
                    'created_at': now
                }
            )
            created_count += 1
        
        session.commit()
        
        logger.info(f"✅ Generated {created_count} rent reminders for tenant {tenant_id}")
        
        return jsonify({
            'success': True,
            'message': f'{created_count} rent reminder(s) created',
            'count': created_count
        }), 200
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error generating rent reminders: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()