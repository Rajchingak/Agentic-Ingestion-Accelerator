-- Business glossary table used by the Context Trust Score notebook.
-- Replace governance.trust with your catalog.schema, then set the notebook's
-- `glossary_table` widget to governance.trust.business_glossary.

CREATE TABLE IF NOT EXISTS governance.trust.business_glossary (
  term        STRING NOT NULL COMMENT 'Business term, e.g. Net Revenue. Matched to measure/column/table names ignoring case and punctuation.',
  definition  STRING          COMMENT 'Plain-language business definition (at least 20 characters to count as defined).',
  domain      STRING          COMMENT 'Business domain that owns the term, e.g. Sales, Finance. Should match your domain tag values.',
  owner       STRING          COMMENT 'Accountable business owner or data steward (person or group).',
  synonyms    STRING          COMMENT 'Comma-separated alternative names, e.g. "net_sales, revenue_net".',
  CONSTRAINT business_glossary_pk PRIMARY KEY (term)
)
COMMENT 'Business glossary: the agreed vocabulary of the business. Source of truth for metric names and definitions.';

-- Example rows; delete or replace with your own terms.
INSERT INTO governance.trust.business_glossary VALUES
  ('Net Revenue',
   'Gross revenue minus returns, discounts and allowances, in reporting currency.',
   'Finance', 'finance-data-stewards', 'net_sales, revenue_net'),
  ('Active Customer',
   'A customer with at least one completed order in the last 90 days.',
   'Sales', 'sales-data-stewards', 'active_customers, active_users'),
  ('Churn Rate',
   'Share of customers active at the start of a period who are not active at its end.',
   'Sales', 'sales-data-stewards', 'customer_churn');
