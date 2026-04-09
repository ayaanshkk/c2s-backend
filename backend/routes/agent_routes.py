# backend/routes/agent_routes.py

from flask import Blueprint, jsonify, request, g
from backend.routes.auth_helpers import token_required, get_current_tenant_id
from backend.properties.repositories.user_repository import UserRepository
import logging

logger = logging.getLogger(__name__)

agent_bp = Blueprint('agents', __name__)

@agent_bp.route('/', methods=['GET', 'OPTIONS'])
@token_required
def get_agents():  # ✅ NO current_user parameter
    """Get all property agents from Employee_Master"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context',
                'message': 'tenant_id missing in token or X-Tenant-ID mismatch',
            }), 403

        repo = UserRepository()
        agents = repo.get_all_agents(tenant_id)

        return jsonify({
            'success': True,
            'data': agents
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching agents: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@agent_bp.route('/<int:agent_id>', methods=['GET', 'OPTIONS'])
@token_required
def get_agent(agent_id):  # ✅ NO current_user parameter
    """Get single agent by ID"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context',
                'message': 'tenant_id missing in token or X-Tenant-ID mismatch',
            }), 403

        repo = UserRepository()
        agent = repo.get_agent_by_id(agent_id, tenant_id)

        if not agent:
            return jsonify({
                'success': False,
                'error': 'Agent not found'
            }), 404
        
        return jsonify({
            'success': True,
            'agent': agent
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching agent: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@agent_bp.route('/<int:agent_id>/properties', methods=['GET', 'OPTIONS'])
@token_required
def get_agent_properties(agent_id):  # ✅ NO current_user parameter
    """Get all properties assigned to this agent"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context',
                'message': 'tenant_id missing in token or X-Tenant-ID mismatch',
            }), 403

        from backend.properties.services.property_service import PropertyService

        service = PropertyService()
        properties = service.get_properties_by_agent(agent_id, tenant_id)

        return jsonify({
            'success': True,
            'properties': properties,
            'count': len(properties) if properties else 0
        }), 200

    except PermissionError as e:
        return jsonify({'success': False, 'error': str(e)}), 403

    except Exception as e:
        logger.error(f"Error fetching agent properties: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@agent_bp.route('/<int:agent_id>/stats', methods=['GET', 'OPTIONS'])
@token_required
def get_agent_stats(agent_id):  # ✅ NO current_user parameter
    """Get statistics for specific agent"""
    if request.method == 'OPTIONS':
        return '', 204
        
    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context',
                'message': 'tenant_id missing in token or X-Tenant-ID mismatch',
            }), 403

        from backend.properties.services.property_service import PropertyService

        service = PropertyService()
        properties = service.get_properties_by_agent(agent_id, tenant_id)

        # Calculate stats
        total_properties = len(properties) if properties else 0
        available = sum(1 for p in properties if p.get('status_name', '').lower() == 'available') if properties else 0
        occupied = sum(1 for p in properties if p.get('status_name', '').lower() == 'occupied') if properties else 0
        maintenance = sum(1 for p in properties if p.get('status_name', '').lower() == 'under maintenance') if properties else 0
        
        # Calculate total monthly income
        total_income = sum(p.get('monthly_rent', 0) or 0 for p in properties if p.get('status_name', '').lower() == 'occupied') if properties else 0
        
        stats = {
            'agent_id': agent_id,
            'total_properties': total_properties,
            'available': available,
            'occupied': occupied,
            'under_maintenance': maintenance,
            'monthly_income': total_income,
            'occupancy_rate': round((occupied / total_properties * 100), 2) if total_properties > 0 else 0
        }
        
        return jsonify({
            'success': True,
            'stats': stats
        }), 200

    except PermissionError as e:
        return jsonify({'success': False, 'error': str(e)}), 403
        
    except Exception as e:
        logger.error(f"Error fetching agent stats: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@agent_bp.route('/', methods=['POST', 'OPTIONS'])
@token_required
def create_agent():
    """Create an employee record for the current tenant (agent directory)."""
    if request.method == 'OPTIONS':
        return '', 204

    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context',
                'message': 'tenant_id missing in token or X-Tenant-ID mismatch',
            }), 403

        data = request.get_json() or {}
        name = (data.get('employee_name') or data.get('name') or '').strip()
        email = (data.get('email') or '').strip()
        phone = (data.get('phone') or '').strip() or None

        if not name:
            return jsonify({'success': False, 'error': 'employee_name is required'}), 400
        if not email:
            return jsonify({'success': False, 'error': 'email is required'}), 400

        repo = UserRepository()
        row = repo.create_employee_agent(tenant_id, name, email, phone)
        if not row:
            return jsonify({'success': False, 'error': 'Could not create agent'}), 500

        return jsonify({
            'success': True,
            'agent': row,
            'message': 'Agent created successfully',
        }), 201

    except Exception as e:
        err = str(e).lower()
        if 'unique' in err or 'duplicate' in err:
            return jsonify({
                'success': False,
                'error': 'An employee with this email may already exist',
            }), 409
        logger.error('Error creating agent: %s', e)
        return jsonify({'success': False, 'error': str(e)}), 500