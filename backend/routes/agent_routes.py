# backend/routes/agent_routes.py

from flask import Blueprint, jsonify, request, g
from backend.routes.auth_helpers import token_required, get_current_tenant_id
from backend.properties.repositories.user_repository import UserRepository
import logging
import os

logger = logging.getLogger(__name__)

agent_bp = Blueprint('agents', __name__)

@agent_bp.route('/', methods=['GET', 'OPTIONS'])
@token_required
def get_agents():
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
def get_agent(agent_id):
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
def get_agent_properties(agent_id):
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
def get_agent_stats(agent_id):
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

        from backend.db import SessionLocal
        from sqlalchemy import text
        
        session = SessionLocal()
        
        try:
            # Get agent stats with correct occupancy_status field
            stats_query = text('''
                SELECT 
                    COUNT(*) as total_properties,
                    COUNT(CASE WHEN p.occupancy_status = 'Occupied' THEN 1 END) as occupied,
                    COUNT(CASE WHEN s.stage_name = 'Available' THEN 1 END) as available,
                    COUNT(CASE WHEN s.stage_name = 'Under Maintenance' THEN 1 END) as maintenance,
                    COALESCE(SUM(CASE 
                        WHEN p.occupancy_status = 'Occupied' AND p.monthly_rent IS NOT NULL 
                        THEN p.monthly_rent 
                        ELSE 0 
                    END), 0) as total_monthly_income
                FROM "StreemLyne_MT"."Property_Master" p
                LEFT JOIN "StreemLyne_MT"."Stage_Master" s 
                    ON p.status_id = s.stage_id
                WHERE p.tenant_id = :tenant_id
                AND p.assigned_agent_id = :agent_id
                AND p.is_deleted = FALSE
            ''')
            
            result = session.execute(stats_query, {
                'tenant_id': tenant_id,
                'agent_id': agent_id
            }).first()
            
            total_properties = int(result.total_properties or 0)
            occupied = int(result.occupied or 0)
            available = int(result.available or 0)
            maintenance = int(result.maintenance or 0)
            total_income = float(result.total_monthly_income or 0)
            
            occupancy_rate = round((occupied / total_properties * 100), 1) if total_properties > 0 else 0.0

            stats = {
                'agent_id': agent_id,
                'total_properties': total_properties,
                'available': available,
                'occupied': occupied,
                'under_maintenance': maintenance,
                'monthly_income': total_income,
                'occupancy_rate': occupancy_rate
            }
            
            return jsonify({
                'success': True,
                'stats': stats
            }), 200
            
        finally:
            session.close()

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
        email = (data.get('email') or '').strip() or None
        phone = (data.get('phone') or '').strip()

        if not name:
            return jsonify({'success': False, 'error': 'employee_name is required'}), 400
        if not phone:
            return jsonify({'success': False, 'error': 'phone is required'}), 400

        repo = UserRepository()
        row = repo.create_employee_agent(tenant_id, name, email, phone)
        if not row:
            return jsonify({'success': False, 'error': 'Could not create agent'}), 500

        # Extract invite token and build link
        invite_token = row.pop("invite_token", None)  # remove from agent payload
        frontend_url = os.environ.get("FRONTEND_URL", "http://localhost:3000")
        invite_link = (
            f"{frontend_url}/accept-invite?token={invite_token}"
            if invite_token else None
        )

        logger.info("Agent created — invite_token=%s invite_link=%s", invite_token, invite_link)

        return jsonify({
            'success': True,
            'agent': row,
            'invite_link': invite_link,
            'message': 'Agent created successfully',
        }), 201

    except Exception as e:
        err = str(e).lower()
        if 'unique' in err or 'duplicate' in err:
            return jsonify({
                'success': False,
                'error': 'An agent with this phone number or email already exists',
            }), 409
        logger.error('Error creating agent: %s', e)
        return jsonify({'success': False, 'error': str(e)}), 500


@agent_bp.route('/<int:agent_id>', methods=['DELETE', 'OPTIONS'])
@token_required
def delete_agent(agent_id):
    """Delete agent and associated user/role records."""
    if request.method == 'OPTIONS':
        return '', 204

    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context',
            }), 403

        repo = UserRepository()
        repo.delete_agent(agent_id, tenant_id)
        return jsonify({'success': True, 'message': 'Agent deleted successfully'}), 200

    except Exception as e:
        logger.error('Error deleting agent %s: %s', agent_id, e)
        return jsonify({'success': False, 'error': str(e)}), 500

@agent_bp.route('/<int:agent_id>/regenerate-invite', methods=['POST', 'OPTIONS'])
@token_required
def regenerate_invite(agent_id):
    """Regenerate invite token and link for an agent with pending invite"""
    if request.method == 'OPTIONS':
        return '', 204

    try:
        tenant_id = get_current_tenant_id()
        if not tenant_id:
            return jsonify({
                'success': False,
                'error': 'Invalid tenant context',
            }), 403

        repo = UserRepository()
        result = repo.regenerate_agent_invite(agent_id, tenant_id)
        
        if not result:
            return jsonify({
                'success': False,
                'error': 'Agent not found or invite already accepted'
            }), 404

        # Build invite link
        invite_token = result.get("invite_token")
        frontend_url = os.environ.get("FRONTEND_URL", "http://localhost:3000")
        invite_link = (
            f"{frontend_url}/accept-invite?token={invite_token}"
            if invite_token else None
        )

        logger.info(f"Invite regenerated for agent {agent_id} — token={invite_token}")

        return jsonify({
            'success': True,
            'invite_link': invite_link,
            'invite_token': invite_token,
            'message': 'Invite link regenerated successfully',
        }), 200

    except Exception as e:
        logger.error(f'Error regenerating invite for agent {agent_id}: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500