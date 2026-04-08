-- Property CRM: expenses, invoices, mortgages, lease agreements (string tenant_id — matches Property_Master)
-- Run in Supabase SQL editor after backup.

CREATE TABLE IF NOT EXISTS "StreemLyne_MT"."property_expenses" (
  "expense_id" BIGSERIAL PRIMARY KEY,
  "tenant_id" VARCHAR(128) NOT NULL,
  "property_id" SMALLINT NOT NULL,
  "category" VARCHAR(100),
  "amount" NUMERIC(12,2) NOT NULL DEFAULT 0,
  "incurred_date" DATE,
  "description" TEXT,
  "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_property_expenses_tenant_prop
  ON "StreemLyne_MT"."property_expenses" ("tenant_id", "property_id");

CREATE TABLE IF NOT EXISTS "StreemLyne_MT"."property_invoices" (
  "invoice_id" BIGSERIAL PRIMARY KEY,
  "tenant_id" VARCHAR(128) NOT NULL,
  "property_id" SMALLINT NOT NULL,
  "invoice_number" VARCHAR(64),
  "amount" NUMERIC(12,2) NOT NULL DEFAULT 0,
  "due_date" DATE,
  "status" VARCHAR(32) DEFAULT 'draft',
  "document_url" TEXT,
  "notes" TEXT,
  "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_property_invoices_tenant_prop
  ON "StreemLyne_MT"."property_invoices" ("tenant_id", "property_id");

CREATE TABLE IF NOT EXISTS "StreemLyne_MT"."property_mortgages" (
  "mortgage_id" BIGSERIAL PRIMARY KEY,
  "tenant_id" VARCHAR(128) NOT NULL,
  "property_id" SMALLINT NOT NULL,
  "lender" VARCHAR(255),
  "principal" NUMERIC(14,2),
  "monthly_payment" NUMERIC(12,2),
  "rate_percent" NUMERIC(6,3),
  "start_date" DATE,
  "end_date" DATE,
  "document_url" TEXT,
  "notes" TEXT,
  "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_property_mortgages_tenant_prop
  ON "StreemLyne_MT"."property_mortgages" ("tenant_id", "property_id");

CREATE TABLE IF NOT EXISTS "StreemLyne_MT"."property_lease_agreements" (
  "agreement_id" BIGSERIAL PRIMARY KEY,
  "tenant_id" VARCHAR(128) NOT NULL,
  "property_id" SMALLINT NOT NULL,
  "tenant_display_name" VARCHAR(255),
  "title" VARCHAR(255),
  "start_date" DATE,
  "end_date" DATE,
  "rent_amount" NUMERIC(12,2),
  "document_url" TEXT,
  "notes" TEXT,
  "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  "updated_at" TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_property_lease_agreements_tenant_prop
  ON "StreemLyne_MT"."property_lease_agreements" ("tenant_id", "property_id");
