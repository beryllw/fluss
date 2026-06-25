-- ============================================================================
-- Seed deterministic refund-investigation data for the fluss-gateway quickstart.
--
-- This quickstart intentionally uses fixed data instead of a large streaming story:
-- users should be able to bring the cluster up, connect MCP, and reproduce the
-- same refund case every time.
-- ============================================================================

CREATE CATALOG IF NOT EXISTS fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'coordinator-server:9123'
);

USE CATALOG fluss_catalog;
USE refund_demo;

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

INSERT INTO customer_profiles (`customer_id`, `customer_name`, `customer_tier`, `contact_email`, `region_name`, `updated_at`) VALUES
    ('CUS-1001', 'Lin Mei', 'vip', 'lin.mei@example.com', 'Yangtze River Delta', TIMESTAMP '2026-06-25 10:02:00'),
    ('CUS-1002', 'Chen Hao', 'returning', 'chen.hao@example.com', 'Greater Bay Area', TIMESTAMP '2026-06-25 10:02:00'),
    ('CUS-1003', 'Wang Yu', 'standard', 'wang.yu@example.com', 'Beijing-Tianjin-Hebei', TIMESTAMP '2026-06-25 10:02:00');

INSERT INTO refund_orders (`order_id`, `customer_id`, `order_status`, `refund_status`, `refund_amount`, `payment_amount`, `cancel_reason`, `cancelled_at`, `updated_at`) VALUES
    ('ORD-20260625-1001', 'CUS-1001', 'CANCELLED', 'PROCESSING', CAST(299.00 AS DECIMAL(18, 2)), CAST(299.00 AS DECIMAL(18, 2)), 'Customer cancelled before shipment', TIMESTAMP '2026-06-25 10:06:00', TIMESTAMP '2026-06-25 10:16:00'),
    ('ORD-20260625-1002', 'CUS-1002', 'CANCELLED', 'COMPLETED', CAST(129.00 AS DECIMAL(18, 2)), CAST(129.00 AS DECIMAL(18, 2)), 'Customer changed size', TIMESTAMP '2026-06-25 09:18:00', TIMESTAMP '2026-06-25 09:45:00'),
    ('ORD-20260625-1003', 'CUS-1003', 'FULFILLED', 'NOT_REQUESTED', CAST(0.00 AS DECIMAL(18, 2)), CAST(88.00 AS DECIMAL(18, 2)), CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP(3)), TIMESTAMP '2026-06-25 11:02:00');

INSERT INTO support_cases (`order_id`, `case_id`, `customer_id`, `issue_type`, `case_status`, `opened_at`, `last_customer_message`) VALUES
    ('ORD-20260625-1001', 'CASE-9001', 'CUS-1001', 'REFUND_DELAY', 'OPEN', TIMESTAMP '2026-06-25 10:20:00', 'The order is already cancelled, why has the refund not arrived?'),
    ('ORD-20260625-1002', 'CASE-9002', 'CUS-1002', 'REFUND_CONFIRMATION', 'RESOLVED', TIMESTAMP '2026-06-25 09:40:00', 'Please confirm that the refund has been completed.');

INSERT INTO refund_events (`order_id`, `event_at`, `event_id`, `event_type`, `event_summary`, `operator_note`) VALUES
    ('ORD-20260625-1001', TIMESTAMP '2026-06-25 09:55:00', 'EVT-1001-1', 'ORDER_PAID', 'Customer payment captured successfully.', 'Payment service acknowledged the transaction.'),
    ('ORD-20260625-1001', TIMESTAMP '2026-06-25 10:06:00', 'EVT-1001-2', 'ORDER_CANCELLED', 'Order was cancelled before shipment.', 'Cancellation accepted by the order service.'),
    ('ORD-20260625-1001', TIMESTAMP '2026-06-25 10:07:00', 'EVT-1001-3', 'REFUND_REQUESTED', 'Refund request was submitted to the payment service provider.', 'Refund request id RF-1001 has been created.'),
    ('ORD-20260625-1001', TIMESTAMP '2026-06-25 10:16:00', 'EVT-1001-4', 'REFUND_PENDING_PROVIDER', 'Refund is still waiting for provider-side settlement.', 'No REFUND_COMPLETED event has been emitted yet.'),
    ('ORD-20260625-1002', TIMESTAMP '2026-06-25 09:10:00', 'EVT-1002-1', 'ORDER_PAID', 'Customer payment captured successfully.', 'Payment service acknowledged the transaction.'),
    ('ORD-20260625-1002', TIMESTAMP '2026-06-25 09:18:00', 'EVT-1002-2', 'ORDER_CANCELLED', 'Order was cancelled after a size-change request.', 'Cancellation accepted by the order service.'),
    ('ORD-20260625-1002', TIMESTAMP '2026-06-25 09:19:00', 'EVT-1002-3', 'REFUND_REQUESTED', 'Refund request was submitted to the payment service provider.', 'Refund request id RF-1002 has been created.'),
    ('ORD-20260625-1002', TIMESTAMP '2026-06-25 09:45:00', 'EVT-1002-4', 'REFUND_COMPLETED', 'The payment service provider confirmed the refund.', 'Customer has already been refunded.');

INSERT INTO refund_audit_history (`order_id`, `audit_at`, `audit_step`, `status_summary`, `actor`) VALUES
    ('ORD-20260625-1001', TIMESTAMP '2026-06-25 10:06:00', 'CASE_OPENED', 'Support case opened after the customer reported the missing refund.', 'support-console'),
    ('ORD-20260625-1001', TIMESTAMP '2026-06-25 10:07:00', 'PSP_REQUESTED', 'Refund request forwarded to the payment service provider.', 'refund-service'),
    ('ORD-20260625-1001', TIMESTAMP '2026-06-25 10:16:00', 'PSP_PENDING', 'Provider settlement is still pending; refund is not yet completed.', 'payment-provider-sync'),
    ('ORD-20260625-1002', TIMESTAMP '2026-06-25 09:19:00', 'PSP_REQUESTED', 'Refund request forwarded to the payment service provider.', 'refund-service'),
    ('ORD-20260625-1002', TIMESTAMP '2026-06-25 09:45:00', 'PSP_COMPLETED', 'Provider confirmed the refund and the customer balance was updated.', 'payment-provider-sync');
