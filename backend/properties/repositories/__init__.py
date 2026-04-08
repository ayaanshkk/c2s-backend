# backend/properties/repositories/__init__.py

from backend.properties.repositories.property_repository import PropertyRepository
from backend.properties.repositories.property_status_repository import PropertyStatusRepository
from backend.properties.repositories.tenant_repository import TenantRepository
from backend.properties.repositories.user_repository import UserRepository
from backend.properties.repositories.document_repository import DocumentRepository

__all__ = [
    'PropertyRepository',
    'PropertyStatusRepository',
    'TenantRepository',
    'UserRepository',
    'DocumentRepository',
]