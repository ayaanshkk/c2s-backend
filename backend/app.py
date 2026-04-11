# -*- coding: utf-8 -*-
import sys
import io
import os
import logging
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")

load_dotenv(ENV_PATH)

print("DEBUG ENV PATH =", ENV_PATH)
print("DEBUG DATABASE_URL =", os.getenv("DATABASE_URL"))
print("DEBUG SUPABASE_URL =", os.getenv("SUPABASE_URL"))

if sys.platform == 'win32':
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    except Exception:
        pass

from flask import Flask, request, jsonify
from flask_cors import CORS

from backend.db import Base, engine, SessionLocal, test_connection, init_db


def create_app():
    app = Flask(__name__)

    app.url_map.strict_slashes = False

    # ============================================
    # CONFIG
    # ============================================
    jwt_secret = os.getenv("JWT_SECRET_KEY") or os.getenv("SECRET_KEY")
    if not jwt_secret:
        raise ValueError("JWT_SECRET_KEY must be set in environment variables")
    app.config["SECRET_KEY"] = jwt_secret

    # Property Management Configuration
    app.config["BLOB_READ_WRITE_TOKEN"] = os.getenv("BLOB_READ_WRITE_TOKEN")
    app.config["SUPABASE_URL"] = os.getenv("SUPABASE_URL")
    app.config["SUPABASE_SERVICE_ROLE_KEY"] = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    app.config["SUPABASE_DB_URL"] = os.getenv("SUPABASE_DB_URL")
    app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB

    # ============================================
    # DATABASE INITIALIZATION
    # ============================================
    logging.info("Initializing database schema...")

    try:
        from backend import models
        
        logging.info("📋 Registered CRM models")
        
        if "sqlite" in str(engine.url):
            Base.metadata.create_all(bind=engine, checkfirst=True)
        
        from sqlalchemy import inspect
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        logging.info(f"✅ Database schema initialized - {len(tables)} tables exist")
        
    except Exception as e:
        logging.error("Database initialization failed: %s", e)
        import traceback
        traceback.print_exc()

    # Import Property Management models
    try:
        from backend.properties import models as property_models
        logging.info("📋 Property Management models registered")
    except Exception as e:
        logging.error(f"Property Management models failed to load: {e}")

    # ============================================
    # CORS
    # ============================================
    CORS(
        app,
        origins="*",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=[
            "Content-Type",
            "Authorization",
            "X-Requested-With",
            "X-Tenant-ID"
        ],
        expose_headers=["Content-Type", "Authorization"],
        supports_credentials=False,
        max_age=3600,
        automatic_options=True
    )

    @app.after_request
    def add_cors_headers(response):
        """Ensure CORS headers are always present"""
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, PATCH, DELETE, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Requested-With, X-Tenant-ID'
        response.headers['Access-Control-Max-Age'] = '3600'
        return response

    # ============================================
    # BLUEPRINTS - CRM SYSTEM
    # ============================================
    from backend.routes import (
        auth_routes, db_routes,
        notification_routes, dashboard_routes,
        document_routes, calendar_routes,
        property_interactions_routes, tenant_routes, property_expense_routes,
    )

    app.register_blueprint(auth_routes.auth_bp, url_prefix='/auth')
    app.register_blueprint(tenant_routes.tenant_bp)
    app.register_blueprint(dashboard_routes.dashboard_bp)
    app.register_blueprint(db_routes.db_bp)
    app.register_blueprint(notification_routes.notification_bp)
    app.register_blueprint(document_routes.document_bp)
    app.register_blueprint(calendar_routes.calendar_bp)
    app.register_blueprint(property_interactions_routes.interaction_bp)
    app.register_blueprint(property_expense_routes.property_expense_bp, url_prefix='/api/properties')

    logging.info("✅ CRM Blueprints registered")

    # ============================================
    # BLUEPRINTS - PROPERTY MANAGEMENT SYSTEM
    # ============================================
    print("\n🔍 DEBUG: Attempting to import property management routes...")
    
    try:
        print("   Importing property_routes...")
        from backend.routes import property_routes
        print(f"   ✅ property_routes imported: {property_routes}")
        print(f"   ✅ property_bp exists: {hasattr(property_routes, 'property_bp')}")
    except Exception as e:
        print(f"   ❌ Failed to import property_routes: {e}")
        import traceback
        traceback.print_exc()
        property_routes = None

    try:
        print("   Importing agent_routes...")
        from backend.routes import agent_routes
        print(f"   ✅ agent_routes imported: {agent_routes}")
        print(f"   ✅ agent_bp exists: {hasattr(agent_routes, 'agent_bp')}")
    except Exception as e:
        print(f"   ❌ Failed to import agent_routes: {e}")
        import traceback
        traceback.print_exc()
        agent_routes = None

    if property_routes and agent_routes:
        try:
            print("   Registering blueprints...")
            app.register_blueprint(property_routes.property_bp, url_prefix='/api/properties')
            print("   ✅ property_bp registered at /api/properties")
            
            app.register_blueprint(agent_routes.agent_bp, url_prefix='/api/agents')
            print("   ✅ agent_bp registered at /api/agents")
            
            logging.info("✅ Property Management Blueprints registered")
            
            # Log registered property routes
            logging.info("📍 Property Management Routes:")
            for rule in app.url_map.iter_rules():
                rule_str = str(rule)
                if '/api/properties' in rule_str or '/api/agents' in rule_str:
                    methods = ', '.join(sorted([m for m in rule.methods if m not in ['HEAD', 'OPTIONS']]))
                    logging.info(f"   [{methods}] {rule_str}")
        except Exception as e:
            print(f"   ❌ Failed to register blueprints: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("   ⚠️ Skipping blueprint registration due to import errors")
    
    # ============================================
    # HEALTH CHECK & INFO ENDPOINTS
    # ============================================
    @app.route("/health", methods=["GET"])
    def health_check():
        return jsonify({"status": "ok", "message": "Server is running"}), 200

    @app.route("/api-info", methods=["GET"])
    def api_info():
        return jsonify({
            "service": "Multi-System Backend",
            "version": "1.0.0",
            "systems": {
                "crm": {"description": "CRM and Training Pipeline"},
                "property_management": {"description": "Property Management System"}
            }
        }), 200

    @app.route("/debug/routes", methods=["GET"])
    def list_routes():
        """List all registered routes"""
        routes = []
        for rule in app.url_map.iter_rules():
            routes.append({
                'endpoint': rule.endpoint,
                'methods': sorted(list(rule.methods - {'HEAD', 'OPTIONS'})),
                'path': str(rule)
            })
        return jsonify(sorted(routes, key=lambda x: x['path'])), 200

    return app


# ============================================
# STANDALONE LAUNCH
# ============================================
if __name__ == "__main__":
    app = create_app()

    logging.info("=" * 60)
    logging.info("🔧 INITIALISING DATABASE...")
    logging.info("=" * 60)

    port = int(os.getenv("PORT", 5000))
    debug_mode = os.getenv("DEV_MODE", "false").lower() == "true"
    
    logging.info(f"\n🚀 Starting server on port {port}")
    logging.info(f"   Debug mode: {debug_mode}")
    logging.info(f"\n📍 Test Routes:")
    logging.info(f"   http://localhost:{port}/health")
    logging.info(f"   http://localhost:{port}/debug/routes")
    logging.info(f"   http://localhost:{port}/api/properties")
    logging.info("\n" + "=" * 60 + "\n")
    
    try:
        app.run(debug=debug_mode, host="0.0.0.0", port=port, threaded=True)
    except Exception as e:
        logging.error("Server error: %s", e)
        import traceback
        traceback.print_exc()