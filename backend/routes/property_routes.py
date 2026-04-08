# backend/routes/property_routes.py

from flask import Blueprint, request, jsonify, g
from backend.routes.auth_helpers import (
    token_required,
    require_admin,
    get_current_tenant_id,
)
from backend.properties.services.property_service import PropertyService
import logging

logger = logging.getLogger(__name__)

property_bp = Blueprint('property', __name__)


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
def get_properties():  # ✅ NO current_user parameter
    """Get all properties with optional filters"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id, err = _tenant_or_403()
        if err:
            return err

        filters = {
            'city': request.args.get('city'),
            'status_id': request.args.get('status_id', type=int),
            'agent_id': request.args.get('agent_id', type=int),
            'property_type': request.args.get('property_type')
        }
        
        filters = {k: v for k, v in filters.items() if v is not None}
        
        service = PropertyService()
        properties = service.get_all_properties(tenant_id, filters)
        
        return jsonify({
            'success': True,
            'properties': properties
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching properties: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@property_bp.route('/<int:property_id>', methods=['GET', 'OPTIONS'])
@token_required
def get_property(property_id):
    """Get single property by ID"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id, err = _tenant_or_403()
        if err:
            return err

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
    """Create new property"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        data = request.get_json()
        logger.info(f"📥 Received data: {data}")
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'No data provided'
            }), 400

        tenant_id, terr = _tenant_or_403()
        if terr:
            return terr
        
        current_user = g.user
        logger.info(f"👤 Current user employee_id: {current_user.employee_id}")
        
        service = PropertyService()
        logger.info("🔧 Calling service.create_property...")
        
        property_data = service.create_property(
            data, current_user.employee_id, tenant_id
        )
        
        logger.info(f"📊 Property data type: {type(property_data)}")
        logger.info(f"📊 Property data value: {property_data}")
        
        # ✅ Check what we got back
        if property_data is None:
            logger.error("❌ Property data is None!")
            return jsonify({
                'success': False,
                'error': 'Failed to create property'
            }), 500
        
        if isinstance(property_data, int):
            logger.error(f"❌ Property data is an int: {property_data}")
            return jsonify({
                'success': False,
                'error': 'Property created but failed to retrieve details'
            }), 500
        
        if not isinstance(property_data, dict):
            logger.error(f"❌ Property data is not a dict: {type(property_data)}")
            return jsonify({
                'success': False,
                'error': f'Unexpected return type: {type(property_data)}'
            }), 500
        
        logger.info("✅ Property created successfully!")
        
        return jsonify({
            'success': True,
            'property': property_data,
            'message': 'Property created successfully'
        }), 201

    except PermissionError as e:
        return jsonify({'success': False, 'error': str(e)}), 403
        
    except Exception as e:
        logger.error(f"❌ Error creating property: {str(e)}")
        import traceback
        logger.error(f"📋 Full traceback:\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@property_bp.route('/<int:property_id>', methods=['PUT', 'OPTIONS'])
@token_required
def update_property(property_id):
    """Update property"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False,
                'error': 'No data provided'
            }), 400

        tenant_id, err = _tenant_or_403()
        if err:
            return err
        
        service = PropertyService()
        property_data = service.update_property(property_id, tenant_id, data)
        
        return jsonify({
            'success': True,
            'property': property_data,
            'message': 'Property updated successfully'
        }), 200

    except PermissionError as e:
        return jsonify({'success': False, 'error': str(e)}), 403
        
    except Exception as e:
        logger.error(f"Error updating property: {str(e)}")
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
        tenant_id, err = _tenant_or_403()
        if err:
            return err

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