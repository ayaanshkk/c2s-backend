# -*- coding: utf-8 -*-
"""
Property Management Controllers
Request handling layer for property operations
"""
from flask import request, jsonify, g
from typing import Dict, Any
from backend.properties.services.property_service import PropertyService


class PropertyController:
    """
    Property Controller
    Handles HTTP requests and responses for property management operations
    """
    
    def __init__(self):
        self.property_service = PropertyService()
    
    # ========================================
    # PROPERTY ENDPOINTS
    # ========================================
    
    def get_properties(self) -> tuple:
        """
        GET /api/properties
        Get all properties for the current tenant
        """
        try:
            tenant_id = g.tenant_id
            
            # Extract query parameters for filtering
            filters = {}
            if request.args.get('city'):
                filters['city'] = request.args.get('city')
            if request.args.get('postcode'):
                filters['postcode'] = request.args.get('postcode')
            if request.args.get('assigned_agent_id'):
                filters['assigned_agent_id'] = int(request.args.get('assigned_agent_id'))
            if request.args.get('status'):
                filters['status'] = request.args.get('status')
            
            result = self.property_service.get_properties(tenant_id, filters if filters else None)
            return jsonify(result), 200
        
        except Exception as e:
            return jsonify({
                'success': False,
                'error': 'Internal server error',
                'message': str(e)
            }), 500
    
    def get_property_detail(self, property_id: int) -> tuple:
        """
        GET /api/properties/<property_id>
        Get details of a specific property
        """
        try:
            tenant_id = g.tenant_id
            result = self.property_service.get_property_detail(tenant_id, property_id)
            
            if not result.get('success'):
                return jsonify(result), 404
            
            return jsonify(result), 200
        
        except Exception as e:
            return jsonify({
                'success': False,
                'error': 'Internal server error',
                'message': str(e)
            }), 500
    
    def create_property(self) -> tuple:
        """
        POST /api/properties
        Create a new property
        """
        try:
            tenant_id = g.tenant_id
            property_data = request.get_json()
            
            if not property_data:
                return jsonify({
                    'success': False,
                    'error': 'Invalid request',
                    'message': 'Request body is required'
                }), 400
            
            # Validate required fields
            required_fields = ['property_name', 'city', 'postcode']
            missing_fields = [field for field in required_fields if not property_data.get(field)]
            
            if missing_fields:
                return jsonify({
                    'success': False,
                    'error': 'Validation error',
                    'message': f'Missing required fields: {", ".join(missing_fields)}'
                }), 400
            
            result = self.property_service.create_property(tenant_id, property_data)
            
            if not result.get('success'):
                return jsonify(result), 400
            
            return jsonify(result), 201
        
        except Exception as e:
            return jsonify({
                'success': False,
                'error': 'Internal server error',
                'message': str(e)
            }), 500
    
    def update_property(self, property_id: int) -> tuple:
        """
        PUT /api/properties/<property_id>
        Update an existing property
        """
        try:
            tenant_id = g.tenant_id
            property_data = request.get_json()
            
            if not property_data:
                return jsonify({
                    'success': False,
                    'error': 'Invalid request',
                    'message': 'Request body is required'
                }), 400
            
            result = self.property_service.update_property(tenant_id, property_id, property_data)
            
            if not result.get('success'):
                return jsonify(result), 404
            
            return jsonify(result), 200
        
        except Exception as e:
            return jsonify({
                'success': False,
                'error': 'Internal server error',
                'message': str(e)
            }), 500
    
    def delete_property(self, property_id: int) -> tuple:
        """
        DELETE /api/properties/<property_id>
        Delete a property (soft delete)
        """
        try:
            tenant_id = g.tenant_id
            result = self.property_service.delete_property(tenant_id, property_id)
            
            if not result.get('success'):
                return jsonify(result), 404
            
            return jsonify(result), 200
        
        except Exception as e:
            return jsonify({
                'success': False,
                'error': 'Internal server error',
                'message': str(e)
            }), 500
    
    def assign_property_to_agent(self, property_id: int) -> tuple:
        """
        PATCH /api/properties/<property_id>/assign
        Assign property to a real estate agent
        """
        try:
            tenant_id = g.tenant_id
            payload = request.get_json()
            
            if not payload or 'agent_id' not in payload:
                return jsonify({
                    'success': False,
                    'error': 'Validation error',
                    'message': 'agent_id is required'
                }), 400
            
            agent_id = payload.get('agent_id')
            
            result = self.property_service.assign_property_to_agent(
                tenant_id, property_id, agent_id
            )
            
            if not result.get('success'):
                return jsonify(result), 404
            
            return jsonify(result), 200
        
        except Exception as e:
            return jsonify({
                'success': False,
                'error': 'Internal server error',
                'message': str(e)
            }), 500
    
    def upload_property_photo(self, property_id: int) -> tuple:
        """
        POST /api/properties/<property_id>/photo
        Upload property photo
        """
        try:
            tenant_id = g.tenant_id
            
            if 'file' not in request.files:
                return jsonify({
                    'success': False,
                    'error': 'No file provided'
                }), 400
            
            file = request.files['file']
            
            if file.filename == '':
                return jsonify({
                    'success': False,
                    'error': 'No file selected'
                }), 400
            
            result = self.property_service.upload_property_photo(
                tenant_id, property_id, file
            )
            
            if not result.get('success'):
                return jsonify(result), 400
            
            return jsonify(result), 200
        
        except Exception as e:
            return jsonify({
                'success': False,
                'error': 'Internal server error',
                'message': str(e)
            }), 500
    
    # ========================================
    # AGENT ENDPOINTS
    # ========================================
    
    def get_agents(self) -> tuple:
        """
        GET /api/agents
        Get all real estate agents for the current tenant
        """
        try:
            tenant_id = g.tenant_id
            result = self.property_service.get_agents(tenant_id)
            return jsonify(result), 200
        
        except Exception as e:
            return jsonify({
                'success': False,
                'error': 'Internal server error',
                'message': str(e)
            }), 500
    
    # ========================================
    # DASHBOARD
    # ========================================
    
    def get_dashboard(self) -> tuple:
        """
        GET /api/dashboard
        Get property management dashboard summary
        """
        try:
            tenant_id = g.tenant_id
            result = self.property_service.get_dashboard_summary(tenant_id)
            return jsonify(result), 200
        
        except Exception as e:
            return jsonify({
                'success': False,
                'error': 'Internal server error',
                'message': str(e)
            }), 500