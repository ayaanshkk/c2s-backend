# -*- coding: utf-8 -*-
"""
Property Document Routes
Handle file uploads for properties (contracts, photos, inspection reports)
Uses Vercel Blob Storage
"""
from flask import Blueprint, request, jsonify, current_app, g
from werkzeug.utils import secure_filename
import json
import os
import httpx
from backend.routes.auth_helpers import token_required
from backend.db import SessionLocal
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

document_bp = Blueprint('documents', __name__, url_prefix='/api/documents')


# ========================================
# CORS SUPPORT
# ========================================

@document_bp.after_request
def after_request(response):
    """Add CORS headers to all responses"""
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,X-Tenant-ID')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response


# ========================================
# VERCEL BLOB HELPER
# ========================================

def upload_to_vercel_blob(path: str, data: bytes, content_type: str) -> str:
    """
    Upload file directly to Vercel Blob REST API
    
    Args:
        path: File path in blob storage (e.g., 'properties/123/contract.pdf')
        data: File bytes
        content_type: MIME type (e.g., 'application/pdf', 'image/jpeg')
    
    Returns:
        Public URL of uploaded file
    
    Raises:
        ValueError: If BLOB_READ_WRITE_TOKEN not set
        httpx.HTTPError: If upload fails
    """
    token = os.environ.get("BLOB_READ_WRITE_TOKEN")
    if not token:
        raise ValueError("BLOB_READ_WRITE_TOKEN environment variable not set")
    
    try:
        response = httpx.put(
            f"https://blob.vercel-storage.com/{path}",
            content=data,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": content_type,
                "x-api-version": "7",
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json()["url"]
    
    except httpx.HTTPError as e:
        logger.error(f"❌ Vercel Blob upload failed: {e}")
        raise


# ========================================
# PROPERTY DOCUMENT ENDPOINTS
# ========================================

@document_bp.route('/properties/<int:property_id>/upload', methods=['POST', 'OPTIONS'])
@token_required
def upload_property_documents(property_id):
    """
    Upload documents for a property (photos, contracts, inspection reports)
    
    Expected: multipart/form-data with 'documents' files
    Optional: 'document_type' (photo, contract, inspection, other)
    
    Stores URLs in Property_Master.document_details as JSON array
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        user = g.user
        tenant_id = user.tenant_id
        
        # Verify property exists and belongs to user's tenant
        property_check = session.execute(text('''
            SELECT property_id, property_name
            FROM "StreemLyne_MT"."Property_Master"
            WHERE property_id = :property_id
            AND tenant_id = :tenant_id
            AND is_deleted = FALSE
            LIMIT 1
        '''), {
            'property_id': property_id,
            'tenant_id': tenant_id
        }).first()
        
        if not property_check:
            return jsonify({
                'success': False,
                'error': 'Property not found'
            }), 404
        
        # Get uploaded files
        if 'documents' not in request.files:
            return jsonify({
                'success': False,
                'error': 'No documents provided'
            }), 400
        
        files = request.files.getlist('documents')
        if not files or len(files) == 0:
            return jsonify({
                'success': False,
                'error': 'No documents selected'
            }), 400
        
        document_type = request.form.get('document_type', 'other')
        uploaded_urls = []
        
        # Upload each file to Vercel Blob
        for file in files:
            if file and file.filename:
                try:
                    filename = secure_filename(file.filename)
                    # Path: properties/{property_id}/{document_type}/{filename}
                    path = f"properties/{property_id}/{document_type}/{filename}"
                    
                    url = upload_to_vercel_blob(
                        path=path,
                        data=file.read(),
                        content_type=file.content_type or 'application/octet-stream'
                    )
                    
                    uploaded_urls.append({
                        'url': url,
                        'filename': filename,
                        'type': document_type,
                        'uploaded_at': datetime.utcnow().isoformat()
                    })
                    
                    logger.info(f"✅ Uploaded: {filename} → {url}")
                
                except Exception as upload_error:
                    logger.error(f"❌ Failed to upload {file.filename}: {upload_error}")
                    continue
        
        if not uploaded_urls:
            return jsonify({
                'success': False,
                'error': 'No valid documents uploaded'
            }), 400
        
        # Update Property_Master.document_details
        try:
            # Get existing documents
            existing_result = session.execute(text('''
                SELECT document_details
                FROM "StreemLyne_MT"."Property_Master"
                WHERE property_id = :property_id
            '''), {'property_id': property_id}).first()
            
            existing_docs = []
            if existing_result and existing_result.document_details:
                try:
                    existing_docs = json.loads(existing_result.document_details) if isinstance(existing_result.document_details, str) else existing_result.document_details
                    if not isinstance(existing_docs, list):
                        existing_docs = []
                except Exception:
                    existing_docs = []
            
            # Merge and update
            all_docs = existing_docs + uploaded_urls
            
            session.execute(text('''
                UPDATE "StreemLyne_MT"."Property_Master"
                SET document_details = :docs,
                    updated_at = NOW()
                WHERE property_id = :property_id
            '''), {
                'property_id': property_id,
                'docs': json.dumps(all_docs)
            })
            
            session.commit()
            
        except Exception as db_error:
            session.rollback()
            logger.error(f"❌ Database error: {db_error}")
            # Continue anyway - files are uploaded
        
        return jsonify({
            'success': True,
            'message': f'{len(uploaded_urls)} document(s) uploaded successfully',
            'documents': uploaded_urls
        }), 200
    
    except Exception as e:
        logger.exception(f"❌ Upload error: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()


@document_bp.route('/properties/<int:property_id>/documents', methods=['GET', 'OPTIONS'])
@token_required
def get_property_documents(property_id):
    """
    Get all documents for a property
    
    Returns JSON array of document objects with URLs
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        user = g.user
        tenant_id = user.tenant_id
        
        result = session.execute(text('''
            SELECT document_details
            FROM "StreemLyne_MT"."Property_Master"
            WHERE property_id = :property_id
            AND tenant_id = :tenant_id
            AND is_deleted = FALSE
            LIMIT 1
        '''), {
            'property_id': property_id,
            'tenant_id': tenant_id
        }).first()
        
        if not result:
            return jsonify({
                'success': False,
                'error': 'Property not found'
            }), 404
        
        documents = []
        if result.document_details:
            try:
                documents = json.loads(result.document_details) if isinstance(result.document_details, str) else result.document_details
                if not isinstance(documents, list):
                    documents = []
            except Exception:
                documents = []
        
        return jsonify({
            'success': True,
            'documents': documents,
            'count': len(documents)
        }), 200
    
    except Exception as e:
        logger.exception(f"❌ Error fetching documents: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()


@document_bp.route('/properties/<int:property_id>/documents/<int:document_index>', methods=['DELETE', 'OPTIONS'])
@token_required
def delete_property_document(property_id, document_index):
    """
    Delete a document from a property
    
    Note: This only removes the reference from the database.
    The file remains in Vercel Blob (manual cleanup needed).
    """
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    
    session = SessionLocal()
    
    try:
        user = g.user
        tenant_id = user.tenant_id
        
        # Get current documents
        result = session.execute(text('''
            SELECT document_details
            FROM "StreemLyne_MT"."Property_Master"
            WHERE property_id = :property_id
            AND tenant_id = :tenant_id
            AND is_deleted = FALSE
            LIMIT 1
        '''), {
            'property_id': property_id,
            'tenant_id': tenant_id
        }).first()
        
        if not result:
            return jsonify({
                'success': False,
                'error': 'Property not found'
            }), 404
        
        documents = []
        if result.document_details:
            try:
                documents = json.loads(result.document_details) if isinstance(result.document_details, str) else result.document_details
                if not isinstance(documents, list):
                    documents = []
            except Exception:
                documents = []
        
        if document_index < 0 or document_index >= len(documents):
            return jsonify({
                'success': False,
                'error': 'Invalid document index'
            }), 400
        
        # Remove document at index
        deleted_doc = documents.pop(document_index)
        
        # Update database
        session.execute(text('''
            UPDATE "StreemLyne_MT"."Property_Master"
            SET document_details = :docs,
                updated_at = NOW()
            WHERE property_id = :property_id
        '''), {
            'property_id': property_id,
            'docs': json.dumps(documents)
        })
        
        session.commit()
        
        logger.info(f"✅ Deleted document from property {property_id}: {deleted_doc.get('filename')}")
        
        return jsonify({
            'success': True,
            'message': 'Document deleted successfully',
            'deleted': deleted_doc
        }), 200
    
    except Exception as e:
        session.rollback()
        logger.exception(f"❌ Error deleting document: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    finally:
        session.close()