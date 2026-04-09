-- Property-scoped interactions stored in Client_Interactions (nullable client_id).
ALTER TABLE "StreemLyne_MT"."Client_Interactions"
  ALTER COLUMN client_id DROP NOT NULL;

ALTER TABLE "StreemLyne_MT"."Client_Interactions"
  ADD COLUMN IF NOT EXISTS tenant_slug VARCHAR(128),
  ADD COLUMN IF NOT EXISTS property_id INTEGER,
  ADD COLUMN IF NOT EXISTS interaction_kind VARCHAR(32),
  ADD COLUMN IF NOT EXISTS employee_id INTEGER;

CREATE INDEX IF NOT EXISTS idx_ci_tenant_property
  ON "StreemLyne_MT"."Client_Interactions" (tenant_slug, property_id)
  WHERE property_id IS NOT NULL;
