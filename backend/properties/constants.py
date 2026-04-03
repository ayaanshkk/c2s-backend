# -*- coding: utf-8 -*-
"""
Property Management Constants
"""

# Property Status Options
PROPERTY_STATUSES = {
    'AVAILABLE': 'Available for Rent/Sale',
    'OCCUPIED': 'Currently Occupied',
    'MAINTENANCE': 'Under Maintenance',
    'LISTED': 'Listed for Sale/Rent',
    'RESERVED': 'Reserved',
    'SOLD': 'Sold',
    'UNAVAILABLE': 'Unavailable'
}

# Property Types
PROPERTY_TYPES = {
    'APARTMENT': 'Apartment',
    'HOUSE': 'House',
    'CONDO': 'Condominium',
    'TOWNHOUSE': 'Townhouse',
    'STUDIO': 'Studio',
    'COMMERCIAL': 'Commercial Property',
    'LAND': 'Land/Plot'
}

# Document Categories
DOCUMENT_CATEGORIES = {
    'PROPERTY_PHOTO': 'Property Photos',
    'FLOOR_PLAN': 'Floor Plans',
    'TENANCY_CONTRACT': 'Tenancy Contracts',
    'LEASE_AGREEMENT': 'Lease Agreements',
    'INSPECTION_REPORT': 'Inspection Reports',
    'MAINTENANCE_RECORD': 'Maintenance Records',
    'EPC_CERTIFICATE': 'Energy Performance Certificate',
    'GAS_SAFETY': 'Gas Safety Certificate',
    'LEGAL_DOCUMENT': 'Legal Documents',
    'PROOF_OF_OWNERSHIP': 'Title Deeds/Ownership Proof',
    'INSURANCE': 'Insurance Documents',
    'OTHER': 'Other Documents'
}

# Tenancy Statuses
TENANCY_STATUSES = {
    'ACTIVE': 'Active Tenancy',
    'EXPIRING_SOON': 'Expiring Soon',
    'EXPIRED': 'Expired',
    'TERMINATED': 'Terminated',
    'PENDING': 'Pending Start'
}