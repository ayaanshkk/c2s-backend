-- Tenant-scoped property rent payment rows (tenant_id = JWT slug; no file storage).
CREATE TABLE IF NOT EXISTS "StreemLyne_MT"."Property_Payments" (
  payment_id BIGSERIAL PRIMARY KEY,
  tenant_id VARCHAR(128) NOT NULL,
  property_id INTEGER NOT NULL,
  month VARCHAR(7) NOT NULL,
  amount NUMERIC(14, 2) NOT NULL DEFAULT 0,
  status VARCHAR(20) NOT NULL,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ,
  CONSTRAINT uq_Property_Payments_month UNIQUE (tenant_id, property_id, month),
  CONSTRAINT chk_Property_Payments_status CHECK (status IN ('PAID', 'NOT_PAID'))
);

CREATE INDEX IF NOT EXISTS idx_Property_Payments_tenant_property
  ON "StreemLyne_MT"."Property_Payments" (tenant_id, property_id);
