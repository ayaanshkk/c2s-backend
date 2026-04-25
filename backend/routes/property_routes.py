# backend/routes/property_routes.py

import logging
import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from backend.models import Property_Master
from flask import Blueprint, request, jsonify, g
from sqlalchemy import text
from backend.db import SessionLocal
from backend.routes.auth_helpers import (
    token_required,
    require_admin,
    get_current_tenant_id,
)
from backend.properties.services.property_service import PropertyService

logger = logging.getLogger(__name__)

property_bp = Blueprint('property', __name__)

SCHEMA_PM = "StreemLyne_MT"
PAYMENTS_TABLE = f'"{SCHEMA_PM}"."Property_Payments"'


def _month_format_ok(s: str) -> bool:
    return bool(s and re.match(r"^\d{4}-(0[1-9]|1[0-2])$", s))


def _property_belongs_to_tenant(session, property_id: int, tenant_id: str) -> bool:
    row = session.execute(
        text(
            f"""
            SELECT 1 FROM "{SCHEMA_PM}"."Property_Master"
            WHERE property_id = :pid AND tenant_id = :tid AND is_deleted = FALSE
            LIMIT 1
            """
        ),
        {"pid": property_id, "tid": tenant_id},
    ).first()
    return bool(row)


def _tenant_or_403():
    tid = get_current_tenant_id()
    if not tid:
        return None, (
            jsonify({
                'success': False,
                'error': 'Invalid tenant context',
                'message': 'tenant_id missing in token or X-Tenant-ID does not match JWT',
            }),
            403,
        )
    return tid, None

@property_bp.route('/statuses', methods=['GET', 'OPTIONS'])
@token_required
def get_property_statuses(): 
    """Get all property statuses (stage_type=3)"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        service = PropertyService()
        statuses = service.get_all_statuses()
        
        return jsonify({
            'success': True,
            'statuses': statuses
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching property statuses: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@property_bp.route('/', methods=['GET', 'OPTIONS'])
@token_required
def get_properties():
    """Get all properties for the current tenant"""
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
                p.property_id,
                p.display_id,
                p.tenant_id,
                p.property_name,
                p.property_type,
                p.occupancy_status,
                p.rent_due_day,
                p.address,
                p.city,
                p.state,
                p.postcode,
                p.country_id,
                p.status_id,
                s.stage_name as status_name,
                p.assigned_agent_id,
                em.employee_name as assigned_agent_name,
                p.monthly_rent,
                p.deposit_amount,
                p.purchase_price,
                p.mortgage_provider,
                p.mortgage_rate,
                p.mortgage_end_date,
                p.monthly_mortgage_payment,
                p.insurance_provider,
                p.monthly_insurance_payment,
                p.bedrooms,
                p.bathrooms,
                p.square_feet,
                p.year_built,
                p.lease_start_date,
                p.lease_end_date,
                p.tenant_name,
                p.tenant_contact,
                p.tenant_email,
                p.description,
                p.amenities,
                p.parking_spaces,
                p.pet_friendly,
                p.furnished,
                p.document_details,
                p.main_photo_url,
                p.photo_urls,
                p.created_at,
                p.updated_at,
                p.is_deleted
            FROM "StreemLyne_MT"."Property_Master" p
            LEFT JOIN "StreemLyne_MT"."Stage_Master" s 
                ON p.status_id = s.stage_id
            LEFT JOIN "StreemLyne_MT"."Employee_Master" em 
                ON p.assigned_agent_id = em.employee_id AND p.tenant_id = em.tenant_id
            WHERE p.tenant_id = :tenant_id
            AND p.is_deleted = FALSE
            ORDER BY p.display_id ASC
        ''')
        
        result = session.execute(query, {'tenant_id': tenant_id})
        properties = [dict(row._mapping) for row in result]
        
        logger.info(f"✅ Fetched {len(properties)} properties for tenant {tenant_id}")
        
        return jsonify({
            'success': True,
            'properties': properties,
            'count': len(properties)
        }), 200
    
    except Exception as e:
        logger.error(f"❌ Error fetching properties: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    
    finally:
        session.close()

@property_bp.route('/<int:property_id>', methods=['GET', 'OPTIONS'])
@token_required
def get_property(property_id):
    """Get single property by ID"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context'
            }), 403

        service = PropertyService()
        property_data = service.get_property_by_id(property_id, tenant_id)
        
        if not property_data:
            return jsonify({
                'success': False,
                'error': 'Property not found'
            }), 404
        
        return jsonify({
            'success': True,
            'property': property_data
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching property: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    
@property_bp.route('/', methods=['POST', 'OPTIONS'])
@token_required
def create_property():
    """Create a new property"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context'
            }), 403
        
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'No data provided'
            }), 400
        
        # Get current user's employee_id
        current_user = g.user
        employee_id = current_user.employee_id
        
        # Use PropertyService to create the property
        service = PropertyService()
        new_property = service.create_property(data, employee_id, tenant_id)
        
        logger.info(f"✅ Property created: {new_property.get('property_id')} for tenant {tenant_id}")
        
        return jsonify({
            'success': True,
            'property': new_property,
            'message': 'Property created successfully'
        }), 201
        
    except Exception as e:
        logger.error(f"❌ Error creating property: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@property_bp.route('/<int:property_id>', methods=['PUT', 'OPTIONS'])
@token_required
def update_property(property_id):
    """Update a property"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context'
            }), 403
        
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'No data provided'
            }), 400
        
        # Use PropertyService to update the property
        service = PropertyService()
        updated_property = service.update_property(property_id, tenant_id, data)
        
        logger.info(f"✅ Property updated: {property_id} for tenant {tenant_id}")
        
        return jsonify({
            'success': True,
            'property': updated_property,
            'message': 'Property updated successfully'
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Error updating property: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@property_bp.route('/<int:property_id>', methods=['DELETE', 'OPTIONS'])
@token_required
@require_admin
def delete_property(property_id):  
    """Delete property (admin only)"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context'
            }), 403

        current_user = g.user
        service = PropertyService()
        success = service.delete_property(
            property_id, current_user.employee_id, tenant_id
        )
        
        if not success:
            return jsonify({
                'success': False,
                'error': 'Property not found'
            }), 404
        
        return jsonify({
            'success': True,
            'message': 'Property deleted successfully'
        }), 200
        
    except Exception as e:
        logger.error(f"Error deleting property: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@property_bp.route('/<int:property_id>/assign', methods=['POST', 'OPTIONS'])
@token_required
def assign_property(property_id):
    """Assign property to agent"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        data = request.get_json()
        agent_id = data.get('agent_id')
        
        if not agent_id:
            return jsonify({
                'success': False,
                'error': 'Agent ID is required'
            }), 400

        tenant_id, err = _tenant_or_403()
        if err:
            return err
        
        service = PropertyService()
        property_data = service.assign_to_agent(property_id, agent_id, tenant_id)
        
        return jsonify({
            'success': True,
            'property': property_data,
            'message': 'Property assigned successfully'
        }), 200

    except PermissionError as e:
        return jsonify({'success': False, 'error': str(e)}), 403
        
    except Exception as e:
        logger.error(f"Error assigning property: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@property_bp.route('/<int:property_id>/payments', methods=['GET', 'POST', 'OPTIONS'])
@token_required
def Property_Payments_collection(property_id):
    """List or create payments for a property (tenant from JWT only)."""
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
        if not _property_belongs_to_tenant(session, property_id, tenant_id):
            return jsonify({'success': False, 'error': 'Property not found'}), 404

        if request.method == 'GET':
            # Accept optional year filter (e.g., "2025-2026")
            financial_year = request.args.get('year')
            
            if financial_year:
                try:
                    start_year, end_year = financial_year.split('-')
                    start_month = f"{start_year}-04"
                    end_month = f"{end_year}-03"
                    
                    rows = session.execute(
                        text(
                            f"""
                            SELECT 
                                id as payment_id, 
                                month, 
                                amount, 
                                total_rent,
                                status, 
                                notes, 
                                created_at, 
                                updated_at
                            FROM {PAYMENTS_TABLE}
                            WHERE tenant_id = :tid AND property_id = :pid
                              AND month >= :start_m AND month <= :end_m
                            ORDER BY month ASC
                            """
                        ),
                        {
                            'tid': tenant_id, 
                            'pid': property_id, 
                            'start_m': start_month, 
                            'end_m': end_month
                        },
                    )
                except ValueError:
                    return jsonify({
                        'success': False,
                        'error': 'Invalid year format. Expected YYYY-YYYY'
                    }), 400
            else:
                # Return all payments
                rows = session.execute(
                    text(
                        f"""
                        SELECT 
                            id as payment_id, 
                            month, 
                            amount, 
                            total_rent,
                            status, 
                            notes, 
                            created_at, 
                            updated_at
                        FROM {PAYMENTS_TABLE}
                        WHERE tenant_id = :tid AND property_id = :pid
                        ORDER BY month DESC
                        """
                    ),
                    {'tid': tenant_id, 'pid': property_id},
                )
            
            payments = []
            for r in rows:
                total = float(r.total_rent) if r.total_rent is not None else 0
                paid = float(r.amount) if r.amount is not None else 0
                pending = max(0, total - paid)  # Calculate pending amount
                
                payments.append({
                    'payment_id': r.payment_id,
                    'month': r.month,
                    'amount': paid,
                    'total_rent': total,
                    'rent_pending': pending,
                    'status': r.status,
                    'notes': r.notes,
                    'created_at': r.created_at.isoformat() if r.created_at else None,
                    'updated_at': r.updated_at.isoformat() if r.updated_at else None,
                })
            return jsonify({
                'success': True,
                'payments': payments,
                'count': len(payments),
            }), 200

        # POST - Create or Update payment
        data = request.get_json() or {}
        month = str(data.get('month') or '').strip()
        if not _month_format_ok(month):
            return jsonify({
                'success': False,
                'error': 'month is required (YYYY-MM)',
            }), 400

        raw_status = str(data.get('status') or '').strip().upper()
        if raw_status not in ('PAID', 'NOT_PAID'):
            return jsonify({
                'success': False,
                'error': 'status must be PAID or NOT_PAID',
            }), 400

        try:
            amount = Decimal(str(data.get('amount', 0)))
            total_rent = Decimal(str(data.get('total_rent', 0)))  
        except InvalidOperation:
            return jsonify({'success': False, 'error': 'Invalid amount'}), 400
        if amount < 0:
            return jsonify({'success': False, 'error': 'amount must be >= 0'}), 400
        if total_rent < 0:
            return jsonify({'success': False, 'error': 'total_rent must be >= 0'}), 400

        notes = data.get('notes')
        if notes is not None and not isinstance(notes, str):
            notes = str(notes)
        notes = (notes or '').strip() or None

        now = datetime.utcnow()
        
        # ✅ CHECK IF PAYMENT ALREADY EXISTS FOR THIS MONTH
        existing = session.execute(
            text(f"""
                SELECT id FROM {PAYMENTS_TABLE}
                WHERE tenant_id = :tid 
                AND property_id = :pid 
                AND month = :m
                LIMIT 1
            """),
            {'tid': tenant_id, 'pid': property_id, 'm': month}
        ).first()
        
        if existing:
            # ✅ UPDATE EXISTING PAYMENT
            session.execute(
                text(f"""
                    UPDATE {PAYMENTS_TABLE}
                    SET amount = :amt, 
                        total_rent = :total,
                        status = :st, 
                        notes = :notes,
                        updated_at = :ua
                    WHERE id = :payment_id
                """),
                {
                    'amt': float(amount),
                    'total': float(total_rent),
                    'st': raw_status,
                    'notes': notes,
                    'ua': now,
                    'payment_id': existing.id
                }
            )
            session.commit()
            return jsonify({
                'success': True,
                'payment_id': existing.id,
                'message': 'Payment updated',
            }), 200
        else:
            # ✅ CREATE NEW PAYMENT
            try:
                res = session.execute(
                    text(
                        f"""
                        INSERT INTO {PAYMENTS_TABLE} (
                            tenant_id, property_id, month, amount, total_rent, status, notes,
                            created_at, updated_at
                        )
                        VALUES (:tid, :pid, :m, :amt, :total, :st, :notes, :ca, :ua)
                        RETURNING id
                        """
                    ),
                    {
                        'tid': tenant_id,
                        'pid': property_id,
                        'm': month,
                        'amt': float(amount),
                        'total': float(total_rent),  
                        'st': raw_status,
                        'notes': notes,
                        'ca': now,
                        'ua': now,
                    },
                )
                pid = res.scalar()
                session.commit()
                return jsonify({
                    'success': True,
                    'payment_id': pid,
                    'message': 'Payment created',
                }), 201
            except Exception as insert_err:
                session.rollback()
                err_s = str(insert_err).lower()
                if 'uq_Property_Payments_month' in err_s or 'unique' in err_s:
                    return jsonify({
                        'success': False,
                        'error': 'A payment for this month already exists',
                    }), 409
                raise

    except Exception as e:
        session.rollback()
        logger.exception('Property_Payments_collection: %s', e)
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        session.close()


@property_bp.route('/<int:property_id>/payments/<payment_id>', methods=['PUT', 'OPTIONS'])
@token_required
def property_payment_update(property_id, payment_id):
    """Update existing payment record"""
    if request.method == 'OPTIONS':
        return '', 204

    tenant_id, err = _tenant_or_403()
    if err:
        return err

    session = SessionLocal()
    try:
        if not _property_belongs_to_tenant(session, property_id, tenant_id):
            return jsonify({'success': False, 'error': 'Property not found'}), 404

        data = request.get_json() or {}
        
        try:
            amount = Decimal(str(data.get('amount', 0)))
            total_rent = Decimal(str(data.get('total_rent', 0)))  
        except InvalidOperation:
            return jsonify({'success': False, 'error': 'Invalid amount'}), 400
            
        if amount < 0:
            return jsonify({'success': False, 'error': 'amount must be >= 0'}), 400
        if total_rent < 0:
            return jsonify({'success': False, 'error': 'total_rent must be >= 0'}), 400
        
        status = str(data.get('status', '')).strip().upper()
        if status not in ('PAID', 'NOT_PAID'):
            return jsonify({'success': False, 'error': 'Invalid status'}), 400
        
        notes = data.get('notes')
        if notes is not None and not isinstance(notes, str):
            notes = str(notes)
        notes = (notes or '').strip() or None
        
        now = datetime.utcnow()
        
        res = session.execute(
            text(f"""
                UPDATE {PAYMENTS_TABLE}
                SET amount = :amt, 
                    total_rent = :total,
                    status = :st,
                    notes = :notes, 
                    updated_at = :ua
                WHERE id = :pay_id AND tenant_id = :tid AND property_id = :pid
                RETURNING id
            """),
            {
                'amt': float(amount),
                'total': float(total_rent),  
                'st': status,
                'notes': notes,
                'ua': now,
                'pay_id': payment_id,
                'tid': tenant_id,
                'pid': property_id
            }
        )
        
        if res.scalar() is None:
            session.rollback()
            return jsonify({'success': False, 'error': 'Payment not found'}), 404
            
        session.commit()
        return jsonify({'success': True, 'message': 'Payment updated'}), 200
        
    except Exception as e:
        session.rollback()
        logger.exception('property_payment_update: %s', e)
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        session.close()

@property_bp.route('/<int:property_id>/payments/<payment_id>', methods=['DELETE', 'OPTIONS'])
@token_required
def property_payment_delete(property_id, payment_id):
    """Delete a payment record"""
    if request.method == 'OPTIONS':
        return '', 204

    tenant_id, err = _tenant_or_403()
    if err:
        return err

    session = SessionLocal()
    try:
        if not _property_belongs_to_tenant(session, property_id, tenant_id):
            return jsonify({'success': False, 'error': 'Property not found'}), 404

        res = session.execute(
            text(
                f"""
                DELETE FROM {PAYMENTS_TABLE}
                WHERE id = :pay_id
                  AND tenant_id = :tid
                  AND property_id = :pid
                RETURNING id
                """
            ),
            {'pay_id': payment_id, 'tid': tenant_id, 'pid': property_id},
        )
        if res.scalar() is None:
            session.rollback()
            return jsonify({'success': False, 'error': 'Payment not found'}), 404
        session.commit()
        return jsonify({'success': True, 'message': 'Payment deleted'}), 200
    except Exception as e:
        session.rollback()
        logger.exception('property_payment_delete: %s', e)
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        session.close()

@property_bp.route('/dashboard/stats', methods=['GET', 'OPTIONS'])
@token_required
def get_dashboard_stats():  
    """Get property dashboard statistics"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id, err = _tenant_or_403()
        if err:
            return err

        service = PropertyService()
        stats = service.get_dashboard_stats(tenant_id)
        
        return jsonify({
            'success': True,
            'stats': stats
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching dashboard stats: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@property_bp.route('/agents/<int:agent_id>/properties', methods=['GET', 'OPTIONS'])
@token_required
def get_agent_properties(agent_id):  
    """Get properties assigned to specific agent"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id, err = _tenant_or_403()
        if err:
            return err

        service = PropertyService()
        properties = service.get_properties_by_agent(agent_id, tenant_id)
        
        return jsonify({
            'success': True,
            'properties': properties
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching agent properties: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    
@property_bp.route('/<int:property_id>/photos', methods=['POST', 'OPTIONS'])
@token_required
def save_property_photos(property_id):
    """Save property photo URLs (already uploaded to Vercel Blob via frontend)"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id, err = _tenant_or_403()
        if err:
            return err
            
        data = request.get_json()
        photo_urls = data.get('photo_urls', [])
        
        if not photo_urls or not isinstance(photo_urls, list):
            return jsonify({"success": False, "error": "Invalid photo_urls"}), 400
        
        session = SessionLocal()
        try:
            # Check if property exists
            row = session.execute(
                text(f"""
                    SELECT photo_urls, main_photo_url 
                    FROM "{SCHEMA_PM}"."Property_Master"
                    WHERE property_id = :pid AND tenant_id = :tid AND is_deleted = FALSE
                """),
                {"pid": property_id, "tid": tenant_id}
            ).first()
            
            if not row:
                return jsonify({"success": False, "error": "Property not found"}), 404
            
            # Get existing photos
            existing_photos = []
            if row.photo_urls:
                try:
                    existing_photos = json.loads(row.photo_urls)
                except:
                    existing_photos = []
            
            # Append new photos
            all_photos = existing_photos + photo_urls
            
            # Set first uploaded photo as main if no main photo exists
            main_photo = row.main_photo_url
            if not main_photo and len(photo_urls) > 0:
                main_photo = photo_urls[0]
            
            # Update property
            session.execute(
                text(f"""
                    UPDATE "{SCHEMA_PM}"."Property_Master"
                    SET photo_urls = :photos, 
                        main_photo_url = :main_photo,
                        updated_at = :updated
                    WHERE property_id = :pid AND tenant_id = :tid
                """),
                {
                    "photos": json.dumps(all_photos),
                    "main_photo": main_photo,
                    "updated": datetime.utcnow(),
                    "pid": property_id,
                    "tid": tenant_id
                }
            )
            session.commit()
            
            return jsonify({
                "success": True,
                "message": f"{len(photo_urls)} photo(s) saved successfully",
                "photo_urls": all_photos
            }), 200
            
        finally:
            session.close()
        
    except Exception as e:
        logger.error(f"Error saving photos: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500


@property_bp.route('/<int:property_id>/photos', methods=['DELETE', 'OPTIONS'])
@token_required
def remove_property_photo(property_id):
    """Remove a property photo URL from database (already deleted from Vercel Blob via frontend)"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id, err = _tenant_or_403()
        if err:
            return err
            
        data = request.get_json()
        photo_url = data.get('photo_url')
        
        if not photo_url:
            return jsonify({"success": False, "error": "Photo URL required"}), 400
        
        session = SessionLocal()
        try:
            # Check if property exists
            row = session.execute(
                text(f"""
                    SELECT photo_urls, main_photo_url 
                    FROM "{SCHEMA_PM}"."Property_Master"
                    WHERE property_id = :pid AND tenant_id = :tid AND is_deleted = FALSE
                """),
                {"pid": property_id, "tid": tenant_id}
            ).first()
            
            if not row:
                return jsonify({"success": False, "error": "Property not found"}), 404
            
            # Get existing photos
            existing_photos = []
            if row.photo_urls:
                try:
                    existing_photos = json.loads(row.photo_urls)
                except:
                    existing_photos = []
            
            # Remove the photo URL
            if photo_url in existing_photos:
                existing_photos.remove(photo_url)
                
                # If deleted photo was the main photo, set a new main photo
                main_photo = row.main_photo_url
                if main_photo == photo_url:
                    main_photo = existing_photos[0] if len(existing_photos) > 0 else None
                
                # Update property
                session.execute(
                    text(f"""
                        UPDATE "{SCHEMA_PM}"."Property_Master"
                        SET photo_urls = :photos,
                            main_photo_url = :main_photo,
                            updated_at = :updated
                        WHERE property_id = :pid AND tenant_id = :tid
                    """),
                    {
                        "photos": json.dumps(existing_photos),
                        "main_photo": main_photo,
                        "updated": datetime.utcnow(),
                        "pid": property_id,
                        "tid": tenant_id
                    }
                )
                session.commit()
                
                return jsonify({
                    "success": True,
                    "message": "Photo removed successfully"
                }), 200
            else:
                return jsonify({"success": False, "error": "Photo not found"}), 404
        
        finally:
            session.close()
        
    except Exception as e:
        logger.error(f"Error removing photo: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500


@property_bp.route('/<int:property_id>/photos/main', methods=['PUT', 'OPTIONS'])
@token_required
def set_main_property_photo(property_id):
    """Set the main property photo"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id, err = _tenant_or_403()
        if err:
            return err
            
        data = request.get_json()
        photo_url = data.get('photo_url')
        
        if not photo_url:
            return jsonify({"success": False, "error": "Photo URL required"}), 400
        
        session = SessionLocal()
        try:
            # Check if property exists and photo is in the list
            row = session.execute(
                text(f"""
                    SELECT photo_urls 
                    FROM "{SCHEMA_PM}"."Property_Master"
                    WHERE property_id = :pid AND tenant_id = :tid AND is_deleted = FALSE
                """),
                {"pid": property_id, "tid": tenant_id}
            ).first()
            
            if not row:
                return jsonify({"success": False, "error": "Property not found"}), 404
            
            # Verify photo exists in property photos
            existing_photos = []
            if row.photo_urls:
                try:
                    existing_photos = json.loads(row.photo_urls)
                except:
                    existing_photos = []
            
            if photo_url not in existing_photos:
                return jsonify({"success": False, "error": "Photo not found in property photos"}), 404
            
            # Set as main photo
            session.execute(
                text(f"""
                    UPDATE "{SCHEMA_PM}"."Property_Master"
                    SET main_photo_url = :main_photo,
                        updated_at = :updated
                    WHERE property_id = :pid AND tenant_id = :tid
                """),
                {
                    "main_photo": photo_url,
                    "updated": datetime.utcnow(),
                    "pid": property_id,
                    "tid": tenant_id
                }
            )
            session.commit()
            
            return jsonify({
                "success": True,
                "message": "Main photo updated successfully"
            }), 200
        
        finally:
            session.close()
        
    except Exception as e:
        logger.error(f"Error setting main photo: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@property_bp.route('/bulk-delete', methods=['POST', 'OPTIONS'])
@token_required
@require_admin
def bulk_delete_properties():
    """Bulk delete properties (admin only)"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        data = request.get_json()
        property_ids = data.get('property_ids', [])
        
        if not property_ids or not isinstance(property_ids, list):
            return jsonify({
                'success': False,
                'error': 'property_ids array is required'
            }), 400
        
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context'
            }), 403

        current_user = g.user
        service = PropertyService()
        
        deleted_count = 0
        failed_ids = []
        
        for property_id in property_ids:
            try:
                success = service.delete_property(
                    property_id, current_user.employee_id, tenant_id
                )
                if success:
                    deleted_count += 1
                else:
                    failed_ids.append(property_id)
            except Exception as e:
                logger.error(f"Failed to delete property {property_id}: {str(e)}")
                failed_ids.append(property_id)
        
        return jsonify({
            'success': True,
            'deleted_count': deleted_count,
            'failed_count': len(failed_ids),
            'failed_ids': failed_ids,
            'message': f'{deleted_count} propert{"y" if deleted_count == 1 else "ies"} deleted successfully'
        }), 200
        
    except Exception as e:
        logger.error(f"Error bulk deleting properties: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    
@property_bp.route('/reset-sequence', methods=['POST', 'OPTIONS'])
@token_required
@require_admin
def reset_property_sequence():
    """Reset property_id sequence to continue from max existing ID (admin only)"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context'
            }), 403

        session = SessionLocal()
        try:
            # Get the maximum property_id for this tenant
            result = session.execute(
                text(f"""
                    SELECT COALESCE(MAX(property_id), 0) as max_id
                    FROM "{SCHEMA_PM}"."Property_Master"
                    WHERE tenant_id = :tid
                """),
                {"tid": tenant_id}
            ).first()
            
            max_id = result.max_id if result else 0
            next_id = max_id + 1
            
            # Reset the sequence
            session.execute(
                text(f"""
                    SELECT setval(
                        '"{SCHEMA_PM}"."Property_Master_property_id_seq"',
                        :next_id,
                        false
                    )
                """),
                {"next_id": next_id}
            )
            session.commit()
            
            logger.info(f"✅ Reset property sequence for tenant {tenant_id} to {next_id}")
            
            return jsonify({
                'success': True,
                'message': f'Property ID sequence reset to {next_id}',
                'next_id': next_id
            }), 200
            
        finally:
            session.close()
        
    except Exception as e:
        logger.error(f"Error resetting property sequence: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@property_bp.route('/fix-occupancy-status', methods=['POST', 'OPTIONS'])
@token_required
@require_admin
def fix_occupancy_status():
    """Fix occupancy_status for existing properties based on tenant_name (admin only)"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context'
            }), 403

        session = SessionLocal()
        try:
            # Update properties: if tenant_name exists -> Occupied, else -> Vacant
            result = session.execute(
                text(f"""
                    UPDATE "{SCHEMA_PM}"."Property_Master"
                    SET occupancy_status = CASE
                        WHEN tenant_name IS NOT NULL AND tenant_name != '' THEN 'Occupied'
                        ELSE 'Vacant'
                    END,
                    updated_at = :updated
                    WHERE tenant_id = :tid AND is_deleted = FALSE
                    RETURNING property_id, property_name, occupancy_status
                """),
                {"tid": tenant_id, "updated": datetime.utcnow()}
            )
            
            updated_properties = result.fetchall()
            session.commit()
            
            logger.info(f"✅ Fixed occupancy_status for {len(updated_properties)} properties in tenant {tenant_id}")
            
            return jsonify({
                'success': True,
                'message': f'Updated {len(updated_properties)} properties',
                'updated_count': len(updated_properties)
            }), 200
            
        finally:
            session.close()
        
    except Exception as e:
        logger.error(f"Error fixing occupancy status: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    
@property_bp.route('/recalculate-display-ids', methods=['POST', 'OPTIONS'])
@token_required
@require_admin
def recalculate_display_ids_route():
    """One-time migration: Set display_ids for existing properties (admin only)"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context'
            }), 403

        from backend.properties.services.property_display_id_service import recalculate_display_ids
        recalculate_display_ids(tenant_id)
        
        return jsonify({
            'success': True,
            'message': 'Display IDs recalculated successfully'
        }), 200
        
    except Exception as e:
        logger.error(f"Error recalculating display IDs: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    
@property_bp.route('/fix-rent-due-days', methods=['POST', 'OPTIONS'])
@token_required
@require_admin
def fix_rent_due_days():
    """Set rent_due_day = 10 for all properties with monthly_rent but no rent_due_day (admin only)"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context'
            }), 403

        session = SessionLocal()
        try:
            result = session.execute(
                text(f"""
                    UPDATE "{SCHEMA_PM}"."Property_Master"
                    SET rent_due_day = 10,
                        updated_at = :updated
                    WHERE tenant_id = :tid 
                      AND is_deleted = FALSE
                      AND monthly_rent IS NOT NULL
                      AND monthly_rent > 0
                      AND rent_due_day IS NULL
                    RETURNING property_id, property_name
                """),
                {"tid": tenant_id, "updated": datetime.utcnow()}
            )
            
            updated_properties = result.fetchall()
            session.commit()
            
            logger.info(f"✅ Set rent_due_day for {len(updated_properties)} properties in tenant {tenant_id}")
            
            return jsonify({
                'success': True,
                'message': f'Updated {len(updated_properties)} properties',
                'updated_count': len(updated_properties),
                'properties': [{'id': p.property_id, 'name': p.property_name} for p in updated_properties]
            }), 200
            
        finally:
            session.close()
        
    except Exception as e:
        logger.error(f"Error fixing rent_due_days: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500