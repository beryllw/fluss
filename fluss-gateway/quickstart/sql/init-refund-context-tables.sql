-- ============================================================================
-- MCP-only quickstart tables for the fluss-gateway refund investigation story.
--
-- Story:
--   1. An order was cancelled.
--   2. The refund has not arrived yet.
--   3. Customer support uses an MCP-connected agent to inspect Fluss through
--      fluss-gateway and explain what happened.
--
-- Design goals:
--   - Keep the schema small and readable for a first quickstart.
--   - Prefer point lookups and prefix lookups over joins.
--   - Keep one lake-enabled history table for `$lake` verification.
-- ============================================================================

CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;
CREATE DATABASE IF NOT EXISTS refund_demo;
USE refund_demo;

CREATE TABLE IF NOT EXISTS customer_profiles (
    `customer_id` STRING NOT NULL COMMENT 'Primary customer key used when drilling from an order or support case',
    `customer_name` STRING COMMENT 'Display name of the customer',
    `customer_tier` STRING COMMENT 'Customer tier such as standard, returning, or vip',
    `contact_email` STRING COMMENT 'Customer contact email',
    `region_name` STRING COMMENT 'Human-readable home region of the customer',
    `updated_at` TIMESTAMP(3) COMMENT 'Last profile update time',
    PRIMARY KEY (`customer_id`) NOT ENFORCED
) COMMENT 'Current customer profile used during refund investigation.'
WITH (
    'bucket.num' = '1'
);

CREATE TABLE IF NOT EXISTS refund_orders (
    `order_id` STRING NOT NULL COMMENT 'Primary order key; the first lookup entry for the refund investigation',
    `customer_id` STRING COMMENT 'Customer who owns this order; next-hop key into customer_profiles',
    `order_status` STRING COMMENT 'Current order status such as PAID, CANCELLED, or FULFILLED',
    `refund_status` STRING COMMENT 'Current refund status such as NOT_REQUESTED, REQUESTED, or PROCESSING',
    `refund_amount` DECIMAL(18, 2) COMMENT 'Requested refund amount',
    `payment_amount` DECIMAL(18, 2) COMMENT 'Original paid amount',
    `cancel_reason` STRING COMMENT 'Human-readable cancellation reason',
    `cancelled_at` TIMESTAMP(3) COMMENT 'Cancellation time if the order was cancelled',
    `updated_at` TIMESTAMP(3) COMMENT 'Last order state update time',
    PRIMARY KEY (`order_id`) NOT ENFORCED
) COMMENT 'Current order state used to confirm whether a refund investigation starts from a cancelled order.'
WITH (
    'bucket.num' = '1'
);

CREATE TABLE IF NOT EXISTS support_cases (
    `order_id` STRING NOT NULL COMMENT 'Prefix lookup key used to find support cases for one order',
    `case_id` STRING NOT NULL COMMENT 'Support case identifier',
    `customer_id` STRING COMMENT 'Customer who opened the case',
    `issue_type` STRING COMMENT 'Issue type such as REFUND_DELAY',
    `case_status` STRING COMMENT 'Current support case status',
    `opened_at` TIMESTAMP(3) COMMENT 'When the support case was opened',
    `last_customer_message` STRING COMMENT 'Most recent customer question captured for the case',
    PRIMARY KEY (`order_id`, `case_id`) NOT ENFORCED
) COMMENT 'Support case context keyed by order for customer-service investigation.'
WITH (
    'bucket.num' = '1',
    'bucket.key' = 'order_id'
);

CREATE TABLE IF NOT EXISTS refund_events (
    `order_id` STRING NOT NULL COMMENT 'Prefix lookup key used to read the refund timeline for one order',
    `event_at` TIMESTAMP(3) NOT NULL COMMENT 'Event time used for recency ordering',
    `event_id` STRING NOT NULL COMMENT 'Unique event identifier within the order timeline',
    `event_type` STRING COMMENT 'Business event type such as ORDER_CANCELLED or REFUND_REQUESTED',
    `event_summary` STRING COMMENT 'Short human-readable summary of the event',
    `operator_note` STRING COMMENT 'Operational note attached to the event',
    PRIMARY KEY (`order_id`, `event_at`, `event_id`) NOT ENFORCED
) COMMENT 'Refund timeline keyed by order so an agent can explain what happened step by step.'
WITH (
    'bucket.num' = '1',
    'bucket.key' = 'order_id'
);

CREATE TABLE IF NOT EXISTS refund_audit_history (
    `order_id` STRING NOT NULL COMMENT 'Prefix lookup key used to inspect historical refund state for one order',
    `audit_at` TIMESTAMP(3) NOT NULL COMMENT 'Audit record time used for recency ordering',
    `audit_step` STRING NOT NULL COMMENT 'Short step name such as CASE_OPENED or PSP_PENDING',
    `status_summary` STRING COMMENT 'Human-readable historical state summary',
    `actor` STRING COMMENT 'System or team responsible for this state transition',
    PRIMARY KEY (`order_id`, `audit_at`, `audit_step`) NOT ENFORCED
) COMMENT 'Lake-enabled refund history table used to verify `$lake` reads through fluss-gateway.'
WITH (
    'bucket.num' = '1',
    'bucket.key' = 'order_id',
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);
