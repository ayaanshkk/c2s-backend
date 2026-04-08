-- CRM agents: dedicated table for property assignments (separate from Employee_Master).
-- Run in Supabase SQL editor after 003_property_crm_financials.sql

CREATE TABLE IF NOT EXISTS "StreemLyne_MT"."crm_agents" (
  "crm_agent_id" BIGSERIAL PRIMARY KEY,
  "tenant_id" VARCHAR(128) NOT NULL,
  "display_name" VARCHAR(255) NOT NULL,
  "email" VARCHAR(255),
  "phone" VARCHAR(64),
  "notes" TEXT,
  "linked_employee_id" INTEGER,
  "is_active" BOOLEAN NOT NULL DEFAULT TRUE,
  "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  "updated_at" TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_crm_agents_tenant
  ON "StreemLyne_MT"."crm_agents" ("tenant_id", "is_active");

-- Property assignment to a CRM agent (PCRM / property workspace).
ALTER TABLE "StreemLyne_MT"."Property_Master"
  ADD COLUMN IF NOT EXISTS "assigned_crm_agent_id" BIGINT REFERENCES "StreemLyne_MT"."crm_agents" ("crm_agent_id") ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_property_master_assigned_crm_agent
  ON "StreemLyne_MT"."Property_Master" ("tenant_id", "assigned_crm_agent_id");

-- Next id is at least 10000 to reduce overlap with legacy assigned_agent_id (employee_id) values.
SELECT setval(
  pg_get_serial_sequence('"StreemLyne_MT"."crm_agents"', 'crm_agent_id'),
  GREATEST(
    COALESCE((SELECT MAX("crm_agent_id") FROM "StreemLyne_MT"."crm_agents"), 0),
    9999
  ),
  true
);
