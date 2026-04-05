# """CRM model shim — exposes UserMaster for imports under
# `backend.properties.models.user_master`.

# This module intentionally re-exports the canonical `UserMaster` declared
# in `backend.models` so higher-level CRM code can import the CRM model
# from `backend.properties.models.user_master` as requested.
# """
# from backend.models import UserMaster  # canonical definition lives in backend.models

# __all__ = ["UserMaster"]
