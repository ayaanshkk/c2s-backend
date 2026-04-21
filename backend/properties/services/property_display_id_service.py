# backend/properties/services/property_display_id_service.py

import logging
from backend.properties.supabase_client import supabase

logger = logging.getLogger(__name__)

SCHEMA = "StreemLyne_MT"

def recalculate_display_ids(tenant_id: str):
    """
    Recalculate display_id for all active properties in a tenant.
    Assigns sequential numbers 1, 2, 3... based on creation order.
    """
    try:
        # Get all active properties ordered by created_at
        query = f'''
            SELECT property_id
            FROM "{SCHEMA}"."Property_Master"
            WHERE tenant_id = %s AND is_deleted = FALSE
            ORDER BY created_at ASC
        '''
        
        properties = supabase.execute_query(query, (tenant_id,))
        
        if not properties:
            logger.info(f"No properties found for tenant {tenant_id}")
            return
        
        # Update each property with sequential display_id
        for idx, row in enumerate(properties, start=1):
            update_query = f'''
                UPDATE "{SCHEMA}"."Property_Master"
                SET display_id = %s
                WHERE property_id = %s AND tenant_id = %s
            '''
            supabase.execute_update(update_query, (idx, row['property_id'], tenant_id))
        
        logger.info(f"✅ Recalculated display_ids for {len(properties)} properties in tenant {tenant_id}")
        
    except Exception as e:
        logger.error(f"❌ Error recalculating display_ids: {str(e)}")
        raise