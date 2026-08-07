package com.example.appian.mqtt.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Inbound message throttling and filtering layer.
 * <p>
 * Sits between the MQTT subscriber callback and the Appian process launcher to:
 * <ul>
 *   <li>Buffer messages using a bounded ArrayBlockingQueue</li>
 *   <li>Apply rate limiting (sliding window, messages per second)</li>
 *   <li>Apply JSON filter criteria before allowing messages through</li>
 *   <li>Never block the MQTT callback thread (non-blocking offer)</li>
 * </ul>
 */
public class MqttInboundThrottler {

    private static final Logger LOG = Logger.getLogger(MqttInboundThrottler.class.getName());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ArrayBlockingQueue<InboundMqttEvent> queue;
    private final int maxMessagesPerSecond;
    private final String jsonFilterExpression;
    private final AtomicLong droppedCount;
    private final AtomicLong acceptedCount;

    // Sliding window rate limiter state
    private final AtomicLong windowStart;
    private final AtomicLong windowCount;

    /**
     * Creates a new throttler.
     *
     * @param queueCapacity        Maximum number of buffered messages (default: 1000)
     * @param maxMessagesPerSecond  Rate limit — messages allowed per second (0 = unlimited)
     * @param jsonFilterExpression  JSON filter (e.g. "temperature > 80"), null = pass all
     */
    public MqttInboundThrottler(int queueCapacity, int maxMessagesPerSecond, String jsonFilterExpression) {
        this.queue = new ArrayBlockingQueue<>(queueCapacity > 0 ? queueCapacity : 1000);
        this.maxMessagesPerSecond = maxMessagesPerSecond;
        this.jsonFilterExpression = (jsonFilterExpression != null && !jsonFilterExpression.isBlank())
                ? jsonFilterExpression.trim() : null;
        this.droppedCount = new AtomicLong(0);
        this.acceptedCount = new AtomicLong(0);
        this.windowStart = new AtomicLong(System.currentTimeMillis());
        this.windowCount = new AtomicLong(0);
    }

    /**
     * Offers a message to the throttler. Never blocks.
     * Returns true if accepted, false if dropped (rate limit, filter, or queue full).
     */
    public boolean offer(InboundMqttEvent event) {
        if (event == null) {
            return false;
        }

        // Rate limiting check
        if (maxMessagesPerSecond > 0 && !checkRateLimit()) {
            droppedCount.incrementAndGet();
            LOG.fine("Message dropped: rate limit exceeded (" + maxMessagesPerSecond + " msg/s)");
            return false;
        }

        // JSON filter check
        if (jsonFilterExpression != null && !passesFilter(event)) {
            droppedCount.incrementAndGet();
            LOG.fine("Message dropped: JSON filter not matched");
            return false;
        }

        // Non-blocking offer to queue
        boolean accepted = queue.offer(event);
        if (!accepted) {
            droppedCount.incrementAndGet();
            LOG.warning("Message dropped: queue full (capacity=" + queue.remainingCapacity() + ")");
        } else {
            acceptedCount.incrementAndGet();
        }
        return accepted;
    }

    /**
     * Blocking poll for consumer thread. Returns null on timeout.
     */
    public InboundMqttEvent poll(long timeoutMs) throws InterruptedException {
        return queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Drains up to maxBatch events into a list for batch processing.
     */
    public List<InboundMqttEvent> drain(int maxBatch) {
        List<InboundMqttEvent> batch = new ArrayList<>(maxBatch);
        queue.drainTo(batch, maxBatch);
        return batch;
    }

    /**
     * Returns the current number of messages buffered in the queue.
     */
    public int getQueueSize() {
        return queue.size();
    }

    /**
     * Returns the total number of messages dropped since creation.
     */
    public long getDroppedCount() {
        return droppedCount.get();
    }

    /**
     * Returns the total number of messages accepted since creation.
     */
    public long getAcceptedCount() {
        return acceptedCount.get();
    }

    /**
     * Clears the queue and resets counters.
     */
    public void shutdown() {
        queue.clear();
        LOG.info("MqttInboundThrottler shut down. Total accepted: " + acceptedCount.get()
                + ", dropped: " + droppedCount.get());
    }

    // --- Rate limiter (fixed window per second) ---

    private boolean checkRateLimit() {
        long now = System.currentTimeMillis();
        long windowStartMs = windowStart.get();

        // If we've moved into a new 1-second window, reset
        if (now - windowStartMs >= 1000) {
            windowStart.set(now);
            windowCount.set(1);
            return true;
        }

        // Within current window — check count
        long currentCount = windowCount.incrementAndGet();
        return currentCount <= maxMessagesPerSecond;
    }

    // --- JSON filter evaluation ---

    /**
     * Evaluates a simple filter expression against the message payload.
     * Supports format: "fieldName operator value" where operator is >, <, >=, <=, ==, !=
     * Examples: "temperature > 80", "humidity <= 30", "status == 1"
     */
    boolean passesFilter(InboundMqttEvent event) {
        if (jsonFilterExpression == null) {
            return true;
        }

        try {
            String payloadStr = new String(event.getPayload(), StandardCharsets.UTF_8);
            JsonNode root = OBJECT_MAPPER.readTree(payloadStr);

            // Parse filter expression: "field operator value"
            String[] parts = jsonFilterExpression.split("\\s+", 3);
            if (parts.length != 3) {
                LOG.warning("Invalid filter expression format: " + jsonFilterExpression);
                return true; // Pass through on invalid filter
            }

            String fieldName = parts[0];
            String operator = parts[1];
            String valueStr = parts[2];

            JsonNode fieldNode = root.get(fieldName);
            if (fieldNode == null || !fieldNode.isNumber()) {
                return false; // Field not found or not numeric — doesn't match
            }

            double fieldValue = fieldNode.asDouble();
            double compareValue = Double.parseDouble(valueStr);

            return evaluateComparison(fieldValue, operator, compareValue);

        } catch (Exception e) {
            LOG.log(Level.FINE, "Filter evaluation error, passing through: " + e.getMessage(), e);
            return true; // On parse error, pass through
        }
    }

    private boolean evaluateComparison(double fieldValue, String operator, double compareValue) {
        return switch (operator) {
            case ">" -> fieldValue > compareValue;
            case ">=" -> fieldValue >= compareValue;
            case "<" -> fieldValue < compareValue;
            case "<=" -> fieldValue <= compareValue;
            case "==" -> fieldValue == compareValue;
            case "!=" -> fieldValue != compareValue;
            default -> {
                LOG.warning("Unknown filter operator: " + operator);
                yield true; // Unknown operator — pass through
            }
        };
    }

    // --- Inner class: Inbound MQTT Event ---

    /**
     * Represents a single inbound MQTT message captured by the subscriber callback.
     */
    public static class InboundMqttEvent {
        private final String topic;
        private final byte[] payload;
        private final int qos;
        private final long receivedTimestamp;

        public InboundMqttEvent(String topic, byte[] payload, int qos) {
            this.topic = topic;
            this.payload = payload != null ? payload : new byte[0];
            this.qos = qos;
            this.receivedTimestamp = System.currentTimeMillis();
        }

        public String getTopic() { return topic; }
        public byte[] getPayload() { return payload; }
        public int getQos() { return qos; }
        public long getReceivedTimestamp() { return receivedTimestamp; }

        public String getPayloadAsString() {
            return new String(payload, StandardCharsets.UTF_8);
        }
    }
}
