# -*- coding: utf-8 -*-
"""
Document Repository
Handles database operations for property document METADATA using Customer_Documents table
Files are stored in Vercel Blob, this stores metadata in Supabase
UPDATED: 2025-04-03
"""
import os
import logging
from typing import Optional, Dict, Any, List
from backend.properties.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)


def _supabase_configured() -> bool:
    """True if Supabase env vars are set"""
    if not os.getenv("SUPABASE_URL") or not os.getenv("SUPABASE_SERVICE_ROLE_KEY"):
        return False
    if os.getenv("SUPABASE_DB_URL"):
        return True
    if os.getenv("DATABASE_URL") and "supabase" in (os.getenv("DATABASE_URL") or ""):
        return True
    if os.getenv("SUPABASE_DB_PASSWORD"):
        return True
    return False


class _LocalDBStub:
    """Stub DB adapter when Supabase is not configured"""
    def execute_query(self, query: str, params: tuple = None, fetch_one: bool = False):
        return None if fetch_one else []


class DocumentRepository:
    """
    Repository for property document METADATA using Customer_Documents table
    
    NOTE: Actual files are stored in Vercel Blob.
    This repository only stores metadata (URLs, names, categories) in Supabase.
    """
    
    def __init__(self):
        if _supabase_configured():
            self.db = get_supabase_client()
        else:
            self.db = _LocalDBStub()
    
    def create_document_metadata(
        self, 
        tenant_id: int,
        document_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Store document metadata in Customer_Documents after file uploaded to Vercel Blob
        
        Args:
            tenant_id: Tenant identifier (property management company)
            document_data: Document metadata from Vercel Blob upload:
                - file_name: File name
                - file_url: Vercel Blob URL (for viewing/download)
                - property_id: (optional) Link to property
                - client_id: (optional) Link to tenant/client
                - document_category: Category (PROPERTY_PHOTO, CONTRACT, etc.)
        
        Returns:
            Created metadata record or None
        """
        query = """
            INSERT INTO "StreemLyne_MT"."Customer_Documents" (
                "client_id",
                "property_id",
                "file_url",
                "file_name",
                "document_category",
                "uploaded_at"
            )
            VALUES (%s, %s, %s, %s, %s, NOW())
            RETURNING 
                "id",
                "client_id",
                "property_id",
                "file_url",
                "file_name",
                "document_category",
                "uploaded_at"
        """
        
        try:
            return self.db.execute_query(
                query,
                (
                    document_data.get('client_id'),
                    document_data.get('property_id'),
                    document_data.get('url') or document_data.get('file_url'),
                    document_data.get('document_name') or document_data.get('file_name'),
                    document_data.get('category') or document_data.get('document_category', 'OTHER'),
                ),
                fetch_one=True
            )
        except Exception as e:
            logger.error(f"Error creating document metadata: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_all_documents(
        self, 
        tenant_id: int,
        filters: Optional[Dict] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all document metadata for a tenant
        
        Args:
            tenant_id: Tenant identifier
            filters: Optional filters (property_id, client_id, category)
        
        Returns:
            List of document metadata records
        """
        # Get all properties for this tenant first to filter documents
        query = """
            SELECT 
                cd."id",
                cd."client_id",
                cd."property_id",
                cd."file_url",
                cd."file_name",
                cd."document_category",
                cd."uploaded_at",
                p."property_name",
                p."address" as "property_address",
                c."client_company_name" as "tenant_name"
            FROM "StreemLyne_MT"."Customer_Documents" cd
            LEFT JOIN "StreemLyne_MT"."Property_Master" p 
                ON cd."property_id" = p."property_id"
            LEFT JOIN "StreemLyne_MT"."Client_Master" c 
                ON cd."client_id" = c."client_id"
            WHERE p."tenant_id" = %s
        """
        
        params = [tenant_id]
        
        if filters:
            if filters.get('property_id'):
                query += ' AND cd."property_id" = %s'
                params.append(filters['property_id'])
            
            if filters.get('client_id'):
                query += ' AND cd."client_id" = %s'
                params.append(filters['client_id'])
            
            if filters.get('category') or filters.get('document_category'):
                query += ' AND cd."document_category" = %s'
                params.append(filters.get('category') or filters.get('document_category'))
        
        query += ' ORDER BY cd."uploaded_at" DESC'
        
        try:
            return self.db.execute_query(query, tuple(params))
        except Exception as e:
            logger.error(f"Error fetching documents: {e}")
            return []
    
    def get_document_by_id(
        self, 
        tenant_id: int,
        document_id: int
    ) -> Optional[Dict[str, Any]]:
        """Get document metadata by ID"""
        query = """
            SELECT 
                cd."id",
                cd."client_id",
                cd."property_id",
                cd."file_url",
                cd."file_name",
                cd."document_category",
                cd."uploaded_at",
                p."property_name",
                c."client_company_name" as "tenant_name"
            FROM "StreemLyne_MT"."Customer_Documents" cd
            LEFT JOIN "StreemLyne_MT"."Property_Master" p 
                ON cd."property_id" = p."property_id"
            LEFT JOIN "StreemLyne_MT"."Client_Master" c 
                ON cd."client_id" = c."client_id"
            WHERE p."tenant_id" = %s AND cd."id" = %s
            LIMIT 1
        """
        
        try:
            return self.db.execute_query(
                query, 
                (tenant_id, document_id), 
                fetch_one=True
            )
        except Exception as e:
            logger.error(f"Error fetching document {document_id}: {e}")
            return None
    
    def delete_document_metadata(
        self, 
        tenant_id: int,
        document_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Delete document metadata from database
        
        Returns the deleted record (so caller can delete from Vercel Blob)
        
        NOTE: This does NOT delete the file from Vercel Blob!
        Caller must delete from Blob separately.
        """
        # First verify ownership
        get_query = """
            SELECT cd."id", cd."file_url", cd."file_name"
            FROM "StreemLyne_MT"."Customer_Documents" cd
            INNER JOIN "StreemLyne_MT"."Property_Master" p 
                ON cd."property_id" = p."property_id"
            WHERE p."tenant_id" = %s AND cd."id" = %s
        """
        
        try:
            record = self.db.execute_query(
                get_query, 
                (tenant_id, document_id), 
                fetch_one=True
            )
            
            if not record:
                return None
            
            # Delete the metadata
            delete_query = """
                DELETE FROM "StreemLyne_MT"."Customer_Documents"
                WHERE "id" = %s
            """
            
            self.db.execute_query(delete_query, (document_id,))
            return record
        except Exception as e:
            logger.error(f"Error deleting document metadata {document_id}: {e}")
            return None
    
    def get_property_documents(
        self, 
        tenant_id: int,
        property_id: int,
        category: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all document metadata for a specific property
        
        Args:
            tenant_id: Tenant identifier
            property_id: Property identifier
            category: Optional category filter
        
        Returns:
            List of document metadata records
        """
        query = """
            SELECT 
                cd."id",
                cd."file_name",
                cd."file_url",
                cd."document_category",
                cd."uploaded_at",
                c."client_company_name" as "uploaded_by"
            FROM "StreemLyne_MT"."Customer_Documents" cd
            INNER JOIN "StreemLyne_MT"."Property_Master" p 
                ON cd."property_id" = p."property_id"
            LEFT JOIN "StreemLyne_MT"."Client_Master" c 
                ON cd."client_id" = c."client_id"
            WHERE p."tenant_id" = %s AND cd."property_id" = %s
        """
        
        params = [tenant_id, property_id]
        
        if category:
            query += ' AND cd."document_category" = %s'
            params.append(category)
        
        query += ' ORDER BY cd."uploaded_at" DESC'
        
        try:
            return self.db.execute_query(query, tuple(params))
        except Exception as e:
            logger.error(f"Error fetching property documents: {e}")
            return []
    
    def get_property_photos(
        self, 
        tenant_id: int,
        property_id: int
    ) -> List[Dict[str, Any]]:
        """Get property photo metadata (convenience method)"""
        return self.get_property_documents(
            tenant_id, 
            property_id, 
            category='PROPERTY_PHOTO'
        )
    
    def get_client_documents(
        self,
        tenant_id: int,
        client_id: int
    ) -> List[Dict[str, Any]]:
        """
        Get all documents uploaded by a specific client (tenant/renter)
        
        Args:
            tenant_id: Tenant identifier (property mgmt company)
            client_id: Client identifier (renter)
        
        Returns:
            List of document metadata records
        """
        query = """
            SELECT 
                cd."id",
                cd."file_name",
                cd."file_url",
                cd."document_category",
                cd."property_id",
                cd."uploaded_at",
                p."property_name"
            FROM "StreemLyne_MT"."Customer_Documents" cd
            LEFT JOIN "StreemLyne_MT"."Property_Master" p 
                ON cd."property_id" = p."property_id"
            WHERE p."tenant_id" = %s AND cd."client_id" = %s
            ORDER BY cd."uploaded_at" DESC
        """
        
        try:
            return self.db.execute_query(query, (tenant_id, client_id))
        except Exception as e:
            logger.error(f"Error fetching client documents: {e}")
            return []