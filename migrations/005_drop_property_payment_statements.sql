-- Remove payment statements table if migration 003 was applied earlier.
DROP TABLE IF EXISTS "StreemLyne_MT"."property_payment_statements";
