package com.example.appian.mqtt.core;

import com.example.appian.mqtt.core.MqttInboundThrottler.InboundMqttEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MqttInboundThrottlerTest {

    private MqttInboundThrottler throttler;

    @BeforeEach
    void setUp() {
        // Default: capacity 100, 50 msgs/sec, no filter
        throttler = new MqttInboundThrottler(100, 50, null);
    }

    // --- Basic offer/poll lifecycle ---

    @Test
    void offer_acceptsValidEvent() {
        InboundMqttEvent event = createEvent("test/topic", "{\"value\": 1}");
        assertTrue(throttler.offer(event));
        assertEquals(1, throttler.getQueueSize());
        assertEquals(1, throttler.getAcceptedCount());
        assertEquals(0, throttler.getDroppedCount());
    }

    @Test
    void offer_rejectsNullEvent() {
        assertFalse(throttler.offer(null));
        assertEquals(0, throttler.getQueueSize());
    }

    @Test
    void poll_returnsBufferedEvent() throws InterruptedException {
        InboundMqttEvent event = createEvent("poll/topic", "payload");
        throttler.offer(event);

        InboundMqttEvent polled = throttler.poll(100);
        assertNotNull(polled);
        assertEquals("poll/topic", polled.getTopic());
        assertEquals("payload", polled.getPayloadAsString());
    }

    @Test
    void poll_returnsNullOnTimeout() throws InterruptedException {
        InboundMqttEvent polled = throttler.poll(50);
        assertNull(polled);
    }

    @Test
    void drain_returnsUpToMaxBatch() {
        for (int i = 0; i < 10; i++) {
            throttler.offer(createEvent("drain/topic", "msg" + i));
        }

        List<InboundMqttEvent> batch = throttler.drain(5);
        assertEquals(5, batch.size());
        assertEquals(5, throttler.getQueueSize()); // 5 remaining
    }

    // --- Queue capacity enforcement ---

    @Test
    void offer_dropsWhenQueueFull() {
        MqttInboundThrottler smallThrottler = new MqttInboundThrottler(5, 0, null);

        for (int i = 0; i < 5; i++) {
            assertTrue(smallThrottler.offer(createEvent("topic", "msg" + i)));
        }

        // 6th should be dropped
        assertFalse(smallThrottler.offer(createEvent("topic", "overflow")));
        assertEquals(5, smallThrottler.getQueueSize());
        assertEquals(1, smallThrottler.getDroppedCount());
        assertEquals(5, smallThrottler.getAcceptedCount());
    }

    @Test
    void offer_dropsMultipleOverflowMessages() {
        MqttInboundThrottler smallThrottler = new MqttInboundThrottler(3, 0, null);

        for (int i = 0; i < 10; i++) {
            smallThrottler.offer(createEvent("topic", "msg" + i));
        }

        assertEquals(3, smallThrottler.getQueueSize());
        assertEquals(7, smallThrottler.getDroppedCount());
        assertEquals(3, smallThrottler.getAcceptedCount());
    }

    // --- Rate limiting ---

    @Test
    void offer_respectsRateLimit() {
        // Allow only 5 msgs/sec
        MqttInboundThrottler rateLimited = new MqttInboundThrottler(100, 5, null);

        int accepted = 0;
        for (int i = 0; i < 20; i++) {
            if (rateLimited.offer(createEvent("rate/topic", "msg" + i))) {
                accepted++;
            }
        }

        // Should have accepted at most 5 (the rate limit per second window)
        assertTrue(accepted <= 5, "Accepted " + accepted + " but rate limit is 5/sec");
        assertTrue(rateLimited.getDroppedCount() > 0);
    }

    @Test
    void offer_unlimitedRate_whenMaxIsZero() {
        MqttInboundThrottler unlimited = new MqttInboundThrottler(1000, 0, null);

        int accepted = 0;
        for (int i = 0; i < 100; i++) {
            if (unlimited.offer(createEvent("unlimited/topic", "msg" + i))) {
                accepted++;
            }
        }

        assertEquals(100, accepted);
        assertEquals(0, unlimited.getDroppedCount());
    }

    // --- JSON filter ---

    @Test
    void offer_passesWhenFilterMatches() {
        MqttInboundThrottler filtered = new MqttInboundThrottler(100, 0, "temperature > 80");

        InboundMqttEvent hot = createEvent("sensor", "{\"temperature\": 95}");
        assertTrue(filtered.offer(hot));
        assertEquals(1, filtered.getAcceptedCount());
    }

    @Test
    void offer_dropsWhenFilterDoesNotMatch() {
        MqttInboundThrottler filtered = new MqttInboundThrottler(100, 0, "temperature > 80");

        InboundMqttEvent cold = createEvent("sensor", "{\"temperature\": 25}");
        assertFalse(filtered.offer(cold));
        assertEquals(1, filtered.getDroppedCount());
    }

    @Test
    void offer_dropsWhenFieldMissing() {
        MqttInboundThrottler filtered = new MqttInboundThrottler(100, 0, "temperature > 80");

        InboundMqttEvent noField = createEvent("sensor", "{\"humidity\": 50}");
        assertFalse(filtered.offer(noField));
        assertEquals(1, filtered.getDroppedCount());
    }

    @Test
    void offer_passesAllWhenNoFilter() {
        MqttInboundThrottler noFilter = new MqttInboundThrottler(100, 0, null);

        assertTrue(noFilter.offer(createEvent("topic", "not json at all")));
        assertEquals(1, noFilter.getAcceptedCount());
    }

    @Test
    void offer_passesAllWhenEmptyFilter() {
        MqttInboundThrottler emptyFilter = new MqttInboundThrottler(100, 0, "  ");

        assertTrue(emptyFilter.offer(createEvent("topic", "anything")));
        assertEquals(1, emptyFilter.getAcceptedCount());
    }

    @Test
    void filter_supportsLessThanOperator() {
        MqttInboundThrottler filtered = new MqttInboundThrottler(100, 0, "value < 10");

        assertTrue(filtered.offer(createEvent("t", "{\"value\": 5}")));
        assertFalse(filtered.offer(createEvent("t", "{\"value\": 15}")));
    }

    @Test
    void filter_supportsEqualsOperator() {
        MqttInboundThrottler filtered = new MqttInboundThrottler(100, 0, "status == 1");

        assertTrue(filtered.offer(createEvent("t", "{\"status\": 1}")));
        assertFalse(filtered.offer(createEvent("t", "{\"status\": 0}")));
    }

    @Test
    void filter_supportsNotEqualsOperator() {
        MqttInboundThrottler filtered = new MqttInboundThrottler(100, 0, "status != 0");

        assertTrue(filtered.offer(createEvent("t", "{\"status\": 1}")));
        assertFalse(filtered.offer(createEvent("t", "{\"status\": 0}")));
    }

    @Test
    void filter_passesOnInvalidJson() {
        // Invalid JSON should pass through (fail-open)
        MqttInboundThrottler filtered = new MqttInboundThrottler(100, 0, "temperature > 80");

        InboundMqttEvent badJson = createEvent("sensor", "not json {{{");
        assertTrue(filtered.offer(badJson));
    }

    // --- Shutdown ---

    @Test
    void shutdown_clearsQueue() {
        for (int i = 0; i < 10; i++) {
            throttler.offer(createEvent("topic", "msg" + i));
        }
        assertEquals(10, throttler.getQueueSize());

        throttler.shutdown();
        assertEquals(0, throttler.getQueueSize());
    }

    // --- InboundMqttEvent ---

    @Test
    void inboundMqttEvent_storesAllFields() {
        InboundMqttEvent event = new InboundMqttEvent("my/topic", "hello".getBytes(StandardCharsets.UTF_8), 2);

        assertEquals("my/topic", event.getTopic());
        assertEquals("hello", event.getPayloadAsString());
        assertEquals(2, event.getQos());
        assertTrue(event.getReceivedTimestamp() > 0);
    }

    @Test
    void inboundMqttEvent_handlesNullPayload() {
        InboundMqttEvent event = new InboundMqttEvent("topic", null, 0);
        assertEquals(0, event.getPayload().length);
        assertEquals("", event.getPayloadAsString());
    }

    // --- Helper ---

    private InboundMqttEvent createEvent(String topic, String payload) {
        return new InboundMqttEvent(topic, payload.getBytes(StandardCharsets.UTF_8), 0);
    }
}
