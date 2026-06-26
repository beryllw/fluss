-- ============================================================================
-- Seed deterministic refund context and start a small realtime refund stream.
--
-- This quickstart keeps the table set small, but it should still feel like a
-- realtime-context story:
--   1. customer_profiles is seeded once as stable dimension data
--   2. refund_orders and support_cases hold the current state for each order
--   3. refund_events continuously receives new refund-related events
--   4. refund_audit_history continuously accumulates a lake-enabled history trail
--
-- The live story is intentionally biased so that ORD-20260625-1001 keeps showing
-- refund-delay signals while ORD-20260625-1002 looks healthy and completed.
-- ============================================================================

CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;
USE refund_demo;

SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'sql-client.execution.result-mode' = 'tableau';

-- --------------------------------------------------------------------------
-- Stable customer context.
-- --------------------------------------------------------------------------

CREATE TEMPORARY VIEW source_customer_profiles (
  `customer_id`, `customer_name`, `customer_tier`, `contact_email`, `region_name`, `updated_at`
) AS
SELECT * FROM (
  VALUES
    ('CUS-1001', 'Lin Mei',   'vip',       'lin.mei@example.com',  'Yangtze River Delta',   TIMESTAMP '2026-06-25 10:02:00'),
    ('CUS-1002', 'Chen Hao',  'returning', 'chen.hao@example.com', 'Greater Bay Area',      TIMESTAMP '2026-06-25 10:02:00'),
    ('CUS-1003', 'Wang Yu',   'standard',  'wang.yu@example.com',  'Beijing-Tianjin-Hebei', TIMESTAMP '2026-06-25 10:02:00')
) AS t(`customer_id`, `customer_name`, `customer_tier`, `contact_email`, `region_name`, `updated_at`);

INSERT INTO customer_profiles SELECT * FROM source_customer_profiles;

-- --------------------------------------------------------------------------
-- Baseline current-state rows so the first MCP queries already have a stable
-- order/case story, even before the live stream has produced more events.
-- --------------------------------------------------------------------------

SET 'execution.runtime-mode' = 'batch';

INSERT INTO refund_orders (`order_id`, `customer_id`, `item_name`, `item_category`, `order_status`, `refund_status`, `refund_amount`, `payment_amount`, `cancel_reason`, `cancelled_at`, `updated_at`) VALUES
    ('ORD-20260625-1001', 'CUS-1001', 'PeakStore Air Fryer Pro', 'Home Appliance', 'CANCELLED', 'PROCESSING', CAST(299.00 AS DECIMAL(18, 2)), CAST(299.00 AS DECIMAL(18, 2)), 'Customer cancelled before shipment', TIMESTAMP '2026-06-25 10:06:00', TIMESTAMP '2026-06-25 10:16:00'),
    ('ORD-20260625-1002', 'CUS-1002', 'UrbanDaily Running Shoes', 'Fashion',        'CANCELLED', 'COMPLETED',  CAST(129.00 AS DECIMAL(18, 2)), CAST(129.00 AS DECIMAL(18, 2)), 'Customer changed size',            TIMESTAMP '2026-06-25 09:18:00', TIMESTAMP '2026-06-25 09:45:00'),
    ('ORD-20260625-1003', 'CUS-1003', 'NorthMart USB-C Charger',  'Electronics',    'FULFILLED', 'NOT_REQUESTED', CAST(0.00 AS DECIMAL(18, 2)), CAST(88.00 AS DECIMAL(18, 2)), CAST(NULL AS STRING),                  CAST(NULL AS TIMESTAMP(3)),     TIMESTAMP '2026-06-25 11:02:00');

INSERT INTO support_cases (`order_id`, `case_id`, `customer_id`, `issue_type`, `case_status`, `opened_at`, `last_customer_message`) VALUES
    ('ORD-20260625-1001', 'CASE-9001', 'CUS-1001', 'REFUND_DELAY',         'OPEN',     TIMESTAMP '2026-06-25 10:20:00', 'The order is already cancelled, why has the refund not arrived?'),
    ('ORD-20260625-1002', 'CASE-9002', 'CUS-1002', 'REFUND_CONFIRMATION',  'RESOLVED', TIMESTAMP '2026-06-25 09:40:00', 'Please confirm that the refund has been completed.');

INSERT INTO refund_events (`order_id`, `event_at`, `event_id`, `event_type`, `event_summary`, `operator_note`) VALUES
    ('ORD-20260625-1001', TIMESTAMP '2026-06-25 09:55:00', 'EVT-1001-1', 'ORDER_PAID',              'Customer payment captured successfully.',                         'Payment service acknowledged the transaction.'),
    ('ORD-20260625-1001', TIMESTAMP '2026-06-25 10:06:00', 'EVT-1001-2', 'ORDER_CANCELLED',         'Order was cancelled before shipment.',                            'Cancellation accepted by the order service.'),
    ('ORD-20260625-1001', TIMESTAMP '2026-06-25 10:07:00', 'EVT-1001-3', 'REFUND_REQUESTED',        'Refund request was submitted to the payment service provider.',   'Refund request id RF-1001 has been created.'),
    ('ORD-20260625-1001', TIMESTAMP '2026-06-25 10:16:00', 'EVT-1001-4', 'REFUND_PENDING_PROVIDER', 'Refund is still waiting for provider-side settlement.',           'No REFUND_COMPLETED event has been emitted yet.'),
    ('ORD-20260625-1002', TIMESTAMP '2026-06-25 09:10:00', 'EVT-1002-1', 'ORDER_PAID',              'Customer payment captured successfully.',                         'Payment service acknowledged the transaction.'),
    ('ORD-20260625-1002', TIMESTAMP '2026-06-25 09:18:00', 'EVT-1002-2', 'ORDER_CANCELLED',         'Order was cancelled after a size-change request.',                'Cancellation accepted by the order service.'),
    ('ORD-20260625-1002', TIMESTAMP '2026-06-25 09:19:00', 'EVT-1002-3', 'REFUND_REQUESTED',        'Refund request was submitted to the payment service provider.',   'Refund request id RF-1002 has been created.'),
    ('ORD-20260625-1002', TIMESTAMP '2026-06-25 09:45:00', 'EVT-1002-4', 'REFUND_COMPLETED',        'The payment service provider confirmed the refund.',              'Customer has already been refunded.');

INSERT INTO refund_audit_history (`order_id`, `audit_at`, `audit_step`, `status_summary`, `actor`) VALUES
    ('ORD-20260625-1001', TIMESTAMP '2026-06-25 10:06:00', 'CASE_OPENED',   'Support case opened after the customer reported the missing refund.', 'support-console'),
    ('ORD-20260625-1001', TIMESTAMP '2026-06-25 10:07:00', 'PSP_REQUESTED', 'Refund request forwarded to the payment service provider.',             'refund-service'),
    ('ORD-20260625-1001', TIMESTAMP '2026-06-25 10:16:00', 'PSP_PENDING',   'Provider settlement is still pending; refund is not yet completed.',   'payment-provider-sync'),
    ('ORD-20260625-1002', TIMESTAMP '2026-06-25 09:19:00', 'PSP_REQUESTED', 'Refund request forwarded to the payment service provider.',             'refund-service'),
    ('ORD-20260625-1002', TIMESTAMP '2026-06-25 09:45:00', 'PSP_COMPLETED', 'Provider confirmed the refund and the customer balance was updated.',   'payment-provider-sync');

-- --------------------------------------------------------------------------
-- Realtime refund signal stream.
--
-- The stream is intentionally biased:
--   - ORD-20260625-1001 frequently emits pending/retry/escalation signals
--   - ORD-20260625-1002 frequently emits completed/settled signals
--   - ORD-20260625-1003 emits healthy fulfillment noise
-- --------------------------------------------------------------------------

SET 'execution.runtime-mode' = 'streaming';

CREATE TEMPORARY TABLE source_refund_signal_seed (
    `event_no` BIGINT,
    `order_bucket` INT,
    `progress_bucket` INT,
    `proc_time` AS PROCTIME()
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '2',
  'fields.event_no.expression' = '#{number.numberBetween ''100000'',''999999''}',
  'fields.order_bucket.expression' = '#{number.numberBetween ''1'',''10''}',
  'fields.progress_bucket.expression' = '#{number.numberBetween ''1'',''100''}'
);

CREATE TEMPORARY VIEW source_refund_signal_core AS
SELECT
    CONCAT('EVT-LIVE-', CAST(`event_no` AS STRING)) AS `event_id`,
    CASE
      WHEN `order_bucket` <= 6 THEN 'ORD-20260625-1001'
      WHEN `order_bucket` <= 8 THEN 'ORD-20260625-1002'
      ELSE 'ORD-20260625-1003'
    END AS `order_id`,
    CASE
      WHEN `order_bucket` <= 6 THEN 'CUS-1001'
      WHEN `order_bucket` <= 8 THEN 'CUS-1002'
      ELSE 'CUS-1003'
    END AS `customer_id`,
    CASE
      WHEN `order_bucket` <= 6 THEN 'CASE-9001'
      WHEN `order_bucket` <= 8 THEN 'CASE-9002'
      ELSE CAST(NULL AS STRING)
    END AS `case_id`,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS `event_at`,
    CASE
      WHEN `order_bucket` <= 6 AND `progress_bucket` <= 55 THEN 'REFUND_PENDING_PROVIDER'
      WHEN `order_bucket` <= 6 AND `progress_bucket` <= 80 THEN 'REFUND_PROVIDER_RETRY'
      WHEN `order_bucket` <= 6 AND `progress_bucket` <= 92 THEN 'CUSTOMER_RECONTACTED'
      WHEN `order_bucket` <= 6 THEN 'SUPPORT_ESCALATED'
      WHEN `order_bucket` <= 8 AND `progress_bucket` <= 70 THEN 'REFUND_COMPLETED'
      WHEN `order_bucket` <= 8 THEN 'REFUND_SETTLEMENT_CONFIRMED'
      WHEN `progress_bucket` <= 50 THEN 'ORDER_FULFILLED'
      ELSE 'PACKAGE_DELIVERED'
    END AS `event_type`
FROM source_refund_signal_seed;

CREATE TEMPORARY VIEW source_refund_signal_events AS
SELECT
    `order_id`,
    `customer_id`,
    `case_id`,
    `event_at`,
    `event_id`,
    `event_type`,
    CASE `event_type`
      WHEN 'REFUND_PENDING_PROVIDER' THEN 'Refund is still pending on the payment provider side.'
      WHEN 'REFUND_PROVIDER_RETRY' THEN 'The provider settlement check is being retried.'
      WHEN 'CUSTOMER_RECONTACTED' THEN 'The customer contacted support again to ask about the refund.'
      WHEN 'SUPPORT_ESCALATED' THEN 'Support escalated the case because the refund is still delayed.'
      WHEN 'REFUND_COMPLETED' THEN 'The payment provider confirmed that the refund was completed.'
      WHEN 'REFUND_SETTLEMENT_CONFIRMED' THEN 'Settlement confirmation was refreshed and the refund remains healthy.'
      WHEN 'ORDER_FULFILLED' THEN 'The order was fulfilled normally and does not require a refund.'
      ELSE 'The delivery flow continues normally for this order.'
    END AS `event_summary`,
    CASE `event_type`
      WHEN 'REFUND_PENDING_PROVIDER' THEN 'Still waiting for REFUND_COMPLETED from the provider.'
      WHEN 'REFUND_PROVIDER_RETRY' THEN 'Background reconciliation is retrying against the provider.'
      WHEN 'CUSTOMER_RECONTACTED' THEN 'Case remains open because the customer has not seen the refund yet.'
      WHEN 'SUPPORT_ESCALATED' THEN 'Escalated to the payments operations queue.'
      WHEN 'REFUND_COMPLETED' THEN 'Funds should reach the customer shortly after provider confirmation.'
      WHEN 'REFUND_SETTLEMENT_CONFIRMED' THEN 'The completed refund remains healthy; no additional action needed.'
      WHEN 'ORDER_FULFILLED' THEN 'Healthy order noise for contrast in the realtime feed.'
      ELSE 'Healthy fulfillment noise for contrast in the realtime feed.'
    END AS `operator_note`
FROM source_refund_signal_core;

EXECUTE STATEMENT SET
BEGIN
    INSERT INTO refund_events
    SELECT
        `order_id`,
        `event_at`,
        `event_id`,
        `event_type`,
        `event_summary`,
        `operator_note`
    FROM source_refund_signal_events;

    INSERT INTO refund_audit_history
    SELECT
        `order_id`,
        `event_at` AS `audit_at`,
        CASE `event_type`
          WHEN 'REFUND_PENDING_PROVIDER' THEN 'PSP_PENDING'
          WHEN 'REFUND_PROVIDER_RETRY' THEN 'PSP_RETRY'
          WHEN 'CUSTOMER_RECONTACTED' THEN 'CUSTOMER_RECONTACTED'
          WHEN 'SUPPORT_ESCALATED' THEN 'SUPPORT_ESCALATED'
          WHEN 'REFUND_COMPLETED' THEN 'PSP_COMPLETED'
          WHEN 'REFUND_SETTLEMENT_CONFIRMED' THEN 'SETTLEMENT_CONFIRMED'
          WHEN 'ORDER_FULFILLED' THEN 'ORDER_FULFILLED'
          ELSE 'PACKAGE_DELIVERED'
        END AS `audit_step`,
        `event_summary` AS `status_summary`,
        CASE `event_type`
          WHEN 'SUPPORT_ESCALATED' THEN 'support-ops'
          WHEN 'CUSTOMER_RECONTACTED' THEN 'support-console'
          WHEN 'REFUND_COMPLETED' THEN 'payment-provider-sync'
          WHEN 'REFUND_SETTLEMENT_CONFIRMED' THEN 'payment-provider-sync'
          WHEN 'REFUND_PROVIDER_RETRY' THEN 'refund-reconciler'
          WHEN 'REFUND_PENDING_PROVIDER' THEN 'payment-provider-sync'
          ELSE 'order-service'
        END AS `actor`
    FROM source_refund_signal_events;

    INSERT INTO refund_orders
    SELECT
        `order_id`,
        `customer_id`,
        CASE
          WHEN `order_id` = 'ORD-20260625-1001' THEN 'PeakStore Air Fryer Pro'
          WHEN `order_id` = 'ORD-20260625-1002' THEN 'UrbanDaily Running Shoes'
          ELSE 'NorthMart USB-C Charger'
        END AS `item_name`,
        CASE
          WHEN `order_id` = 'ORD-20260625-1001' THEN 'Home Appliance'
          WHEN `order_id` = 'ORD-20260625-1002' THEN 'Fashion'
          ELSE 'Electronics'
        END AS `item_category`,
        CASE
          WHEN `order_id` = 'ORD-20260625-1003' THEN 'FULFILLED'
          ELSE 'CANCELLED'
        END AS `order_status`,
        CASE
          WHEN `order_id` = 'ORD-20260625-1001' THEN 'PROCESSING'
          WHEN `order_id` = 'ORD-20260625-1002' THEN 'COMPLETED'
          ELSE 'NOT_REQUESTED'
        END AS `refund_status`,
        CASE
          WHEN `order_id` = 'ORD-20260625-1001' THEN CAST(299.00 AS DECIMAL(18, 2))
          WHEN `order_id` = 'ORD-20260625-1002' THEN CAST(129.00 AS DECIMAL(18, 2))
          ELSE CAST(0.00 AS DECIMAL(18, 2))
        END AS `refund_amount`,
        CASE
          WHEN `order_id` = 'ORD-20260625-1001' THEN CAST(299.00 AS DECIMAL(18, 2))
          WHEN `order_id` = 'ORD-20260625-1002' THEN CAST(129.00 AS DECIMAL(18, 2))
          ELSE CAST(88.00 AS DECIMAL(18, 2))
        END AS `payment_amount`,
        CASE
          WHEN `order_id` = 'ORD-20260625-1001' THEN 'Customer cancelled before shipment'
          WHEN `order_id` = 'ORD-20260625-1002' THEN 'Customer changed size'
          ELSE CAST(NULL AS STRING)
        END AS `cancel_reason`,
        CASE
          WHEN `order_id` = 'ORD-20260625-1001' THEN TIMESTAMP '2026-06-25 10:06:00'
          WHEN `order_id` = 'ORD-20260625-1002' THEN TIMESTAMP '2026-06-25 09:18:00'
          ELSE CAST(NULL AS TIMESTAMP(3))
        END AS `cancelled_at`,
        `event_at` AS `updated_at`
    FROM source_refund_signal_events;

    INSERT INTO support_cases
    SELECT
        `order_id`,
        `case_id`,
        `customer_id`,
        CASE
          WHEN `order_id` = 'ORD-20260625-1001' THEN 'REFUND_DELAY'
          ELSE 'REFUND_CONFIRMATION'
        END AS `issue_type`,
        CASE
          WHEN `order_id` = 'ORD-20260625-1001' AND `event_type` = 'SUPPORT_ESCALATED' THEN 'ESCALATED'
          WHEN `order_id` = 'ORD-20260625-1001' THEN 'OPEN'
          ELSE 'RESOLVED'
        END AS `case_status`,
        CASE
          WHEN `order_id` = 'ORD-20260625-1001' THEN TIMESTAMP '2026-06-25 10:20:00'
          ELSE TIMESTAMP '2026-06-25 09:40:00'
        END AS `opened_at`,
        CASE `event_type`
          WHEN 'CUSTOMER_RECONTACTED' THEN 'The customer followed up again because the refund still has not arrived.'
          WHEN 'SUPPORT_ESCALATED' THEN 'Support escalated the refund case because provider settlement is still pending.'
          WHEN 'REFUND_PENDING_PROVIDER' THEN 'The order is cancelled, but the refund is still waiting for provider settlement.'
          WHEN 'REFUND_PROVIDER_RETRY' THEN 'We are retrying the provider-side reconciliation for this refund.'
          WHEN 'REFUND_COMPLETED' THEN 'Please confirm that the refund has been completed.'
          WHEN 'REFUND_SETTLEMENT_CONFIRMED' THEN 'The refund is complete and no further support action is required.'
          ELSE 'No refund issue is active for this order.'
        END AS `last_customer_message`
    FROM source_refund_signal_events
    WHERE `case_id` IS NOT NULL;
END;
