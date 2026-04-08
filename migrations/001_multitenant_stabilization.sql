-- =============================================================================
-- C2S / StreemLyne_MT — multitenant stabilization (PostgreSQL / Supabase)
-- Run after backup. Adjust schema name if not using "StreemLyne_MT".
-- =============================================================================
-- Goals:
--   - INTEGER tenant_id NOT NULL on tenant-owned tables where applicable
--   - Property_Master: align status_id, indexes
--   - New: property_interactions, rent_agreements, property_expenses, property_images
-- =============================================================================

SET search_path TO "StreemLyne_MT", public;

-- -----------------------------------------------------------------------------
-- 1) Tenant_Master — ensure id type (PK usually tenant_id)
-- -----------------------------------------------------------------------------
ALTER TABLE "Tenant_Master"
  ALTER COLUMN "tenant_id" TYPE INTEGER USING ("tenant_id"::integer);

-- If tenant columns use mixed names from legacy routes, keep both; add NOT NULL where safe:
-- ALTER TABLE "Tenant_Master" ALTER COLUMN "tenant_id" SET NOT NULL;

-- -----------------------------------------------------------------------------
-- 2) Employee_Master — tenant_id INTEGER NOT NULL (backfill before SET NOT NULL)
-- -----------------------------------------------------------------------------
UPDATE "Employee_Master" SET "tenant_id" = (SELECT MIN("tenant_id") FROM "Tenant_Master")
WHERE "tenant_id" IS NULL;

ALTER TABLE "Employee_Master"
  ALTER COLUMN "tenant_id" TYPE INTEGER USING ("tenant_id"::integer);

DO $$
BEGIN
  ALTER TABLE "Employee_Master" ALTER COLUMN "tenant_id" SET NOT NULL;
EXCEPTION
  WHEN others THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_employee_master_tenant_id ON "Employee_Master" ("tenant_id");
CREATE INDEX IF NOT EXISTS idx_employee_master_tenant_email ON "Employee_Master" ("tenant_id", lower("email"));

-- -----------------------------------------------------------------------------
-- 3) Property_Master — tenant_id NOT NULL, status_id, indexes
-- -----------------------------------------------------------------------------
UPDATE "Property_Master" SET "tenant_id" = (SELECT MIN("tenant_id") FROM "Tenant_Master")
WHERE "tenant_id" IS NULL;

ALTER TABLE "Property_Master"
  ALTER COLUMN "tenant_id" TYPE INTEGER USING ("tenant_id"::integer);

DO $$
BEGIN
  ALTER TABLE "Property_Master" ALTER COLUMN "tenant_id" SET NOT NULL;
EXCEPTION
  WHEN others THEN NULL;
END $$;

-- Add status_id / created_by if missing
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'StreemLyne_MT' AND table_name = 'Property_Master' AND column_name = 'status_id'
  ) THEN
    ALTER TABLE "Property_Master" ADD COLUMN "status_id" smallint
      REFERENCES "StreemLyne_MT"."Stage_Master" ("stage_id");
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'StreemLyne_MT' AND table_name = 'Property_Master' AND column_name = 'created_by'
  ) THEN
    ALTER TABLE "Property_Master" ADD COLUMN "created_by" smallint;
  END IF;
END $$;

-- Optional: backfill status_id from legacy text column if present
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'StreemLyne_MT' AND table_name = 'Property_Master' AND column_name = 'property_status'
  ) THEN
    UPDATE "Property_Master" p
    SET "status_id" = s."stage_id"
    FROM "StreemLyne_MT"."Stage_Master" s
    WHERE p."status_id" IS NULL
      AND s."stage_type" = 3
      AND lower(trim(s."stage_name")) = lower(trim(p."property_status"::text));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_property_master_tenant_id ON "Property_Master" ("tenant_id");
CREATE INDEX IF NOT EXISTS idx_property_master_tenant_property ON "Property_Master" ("tenant_id", "property_id");
CREATE INDEX IF NOT EXISTS idx_property_master_assigned_agent ON "Property_Master" ("tenant_id", "assigned_agent_id");
CREATE INDEX IF NOT EXISTS idx_property_master_status ON "Property_Master" ("tenant_id", "status_id");

-- -----------------------------------------------------------------------------
-- 4) Notification_Master — tenant_id INTEGER NOT NULL
-- -----------------------------------------------------------------------------
ALTER TABLE "Notification_Master"
  ALTER COLUMN "tenant_id" TYPE INTEGER USING ("tenant_id"::integer);

-- -----------------------------------------------------------------------------
-- 5) New: property_interactions (do not overload Client_Interactions)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "StreemLyne_MT"."property_interactions" (
  "interaction_id"   BIGSERIAL PRIMARY KEY,
  "tenant_id"        INTEGER NOT NULL REFERENCES "StreemLyne_MT"."Tenant_Master" ("tenant_id"),
  "property_id"      SMALLINT NOT NULL REFERENCES "StreemLyne_MT"."Property_Master" ("property_id"),
  "employee_id"      SMALLINT,
  "interaction_type" VARCHAR(32) NOT NULL,
  "interaction_date" DATE,
  "reminder_date"    DATE,
  "notes"            TEXT,
  "next_steps"       TEXT,
  "contact_method"   SMALLINT DEFAULT 1,
  "created_at"       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
);

CREATE INDEX IF NOT EXISTS idx_property_interactions_tenant ON "StreemLyne_MT"."property_interactions" ("tenant_id");
CREATE INDEX IF NOT EXISTS idx_property_interactions_tenant_property ON "StreemLyne_MT"."property_interactions" ("tenant_id", "property_id");

-- -----------------------------------------------------------------------------
-- 6) New: rent_agreements, property_expenses, property_images
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS "StreemLyne_MT"."rent_agreements" (
  "agreement_id"   BIGSERIAL PRIMARY KEY,
  "tenant_id"      INTEGER NOT NULL REFERENCES "StreemLyne_MT"."Tenant_Master" ("tenant_id"),
  "property_id"    SMALLINT NOT NULL REFERENCES "StreemLyne_MT"."Property_Master" ("property_id"),
  "title"          VARCHAR(255),
  "start_date"     DATE,
  "end_date"       DATE,
  "rent_amount"    NUMERIC(12,2),
  "currency_id"    SMALLINT,
  "document_url"   TEXT,
  "created_at"     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  "updated_at"     TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_rent_agreements_tenant ON "StreemLyne_MT"."rent_agreements" ("tenant_id");
CREATE INDEX IF NOT EXISTS idx_rent_agreements_t_property ON "StreemLyne_MT"."rent_agreements" ("tenant_id", "property_id");

CREATE TABLE IF NOT EXISTS "StreemLyne_MT"."property_expenses" (
  "expense_id"     BIGSERIAL PRIMARY KEY,
  "tenant_id"      INTEGER NOT NULL REFERENCES "StreemLyne_MT"."Tenant_Master" ("tenant_id"),
  "property_id"    SMALLINT NOT NULL REFERENCES "StreemLyne_MT"."Property_Master" ("property_id"),
  "category"       VARCHAR(100),
  "amount"         NUMERIC(12,2) NOT NULL,
  "incurred_date"  DATE,
  "description"    TEXT,
  "created_at"     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_property_expenses_tenant ON "StreemLyne_MT"."property_expenses" ("tenant_id");
CREATE INDEX IF NOT EXISTS idx_property_expenses_t_property ON "StreemLyne_MT"."property_expenses" ("tenant_id", "property_id");

CREATE TABLE IF NOT EXISTS "StreemLyne_MT"."property_images" (
  "image_id"       BIGSERIAL PRIMARY KEY,
  "tenant_id"      INTEGER NOT NULL REFERENCES "StreemLyne_MT"."Tenant_Master" ("tenant_id"),
  "property_id"    SMALLINT NOT NULL REFERENCES "StreemLyne_MT"."Property_Master" ("property_id"),
  "url"            TEXT NOT NULL,
  "sort_order"     INTEGER DEFAULT 0,
  "is_primary"     BOOLEAN DEFAULT FALSE,
  "created_at"     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_property_images_tenant ON "StreemLyne_MT"."property_images" ("tenant_id");
CREATE INDEX IF NOT EXISTS idx_property_images_t_property ON "StreemLyne_MT"."property_images" ("tenant_id", "property_id");

-- -----------------------------------------------------------------------------
-- 7) Opportunity_Details — index (tenant_id, opportunity_id) if table exists
-- -----------------------------------------------------------------------------
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'StreemLyne_MT' AND table_name = 'Opportunity_Details'
  ) THEN
    ALTER TABLE "StreemLyne_MT"."Opportunity_Details"
      ALTER COLUMN "tenant_id" TYPE INTEGER USING (NULLIF("tenant_id"::text, '')::integer);
    CREATE INDEX IF NOT EXISTS idx_opportunity_tenant_opp
      ON "StreemLyne_MT"."Opportunity_Details" ("tenant_id", "opportunity_id");
  END IF;
END $$;
