package com.example.appian.mqtt.integration;

import com.example.appian.mqtt.core.CentralConnectionManager;
import com.example.appian.mqtt.core.MqttInboundThrottler;
import com.example.appian.mqtt.core.MqttInboundThrottler.InboundMqttEvent;
import com.example.appian.mqtt.core.SocketHolder;
import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests using an embedded Moquette MQTT broker.
 * Tests the full publish/subscribe flow through the plugin's core classes.
 */
@Timeout(30) // Fail any test that takes longer than 30 seconds
class MqttEndToEndTest {

    private static Server mqttBroker;
    private static final String BROKER_URL = "tcp://localhost:21883"; // Non-standard port to avoid conflicts
    private static final int BROKER_PORT = 21883;

    @BeforeAll
    static void startBroker() throws Exception {
        mqttBroker = new Server();
        Properties props = new Properties();
        props.setProperty("port", String.valueOf(BROKER_PORT));
        props.setProperty("host", "0.0.0.0");
        props.setProperty("allow_anonymous", "true");
        mqttBroker.startServer(new MemoryConfig(props));

        // Give broker a moment to start
        Thread.sleep(500);
    }

    @AfterAll
    static void stopBroker() {
        if (mqttBroker != null) {
            mqttBroker.stopServer();
        }
        // Clean up singleton state
        CentralConnectionManager.getInstance().getConnectionRegistry().forEach((key, holder) -> {
            try { holder.close(); } catch (Exception ignored) {}
        });
        CentralConnectionManager.getInstance().getConnectionRegistry().clear();
    }

    @AfterEach
    void cleanupConnections() {
        CentralConnectionManager.getInstance().getConnectionRegistry().forEach((key, holder) -> {
            try { holder.close(); } catch (Exception ignored) {}
        });
        CentralConnectionManager.getInstance().getConnectionRegistry().clear();
    }

    // --- Test 1: Publish flow ---

    @Test
    void publish_messageReceivedBySubscriber() throws Exception {
        String topic = "integration/publish/test";
        String payload = "{\"sensor\":\"temp\",\"value\":42}";

        // Set up a raw subscriber to verify the message arrives
        MqttAsyncClient subscriber = new MqttAsyncClient(BROKER_URL, "sub-verify", new MemoryPersistence());
        MqttConnectOptions subOpts = new MqttConnectOptions();
        subOpts.setCleanSession(true);
        subscriber.connect(subOpts).waitForCompletion(5000);

        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<String> receivedPayload = new AtomicReference<>();

        subscriber.subscribe(topic, 1, (t, msg) -> {
            receivedPayload.set(new String(msg.getPayload(), StandardCharsets.UTF_8));
            received.countDown();
        }).waitForCompletion(3000);

        // Publish via CentralConnectionManager (the plugin's path)
        SocketHolder holder = CentralConnectionManager.getInstance().getOrConnect(
                BROKER_URL, "pub-client-1", null, null, 10, true);

        MqttMessage message = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
        message.setQos(1);
        holder.getClient().publish(topic, message).waitForCompletion(5000);

        // Verify message arrived
        assertTrue(received.await(5, TimeUnit.SECONDS), "Message should be received within 5 seconds");
        assertEquals(payload, receivedPayload.get());

        // Cleanup
        subscriber.disconnect().waitForCompletion(2000);
        subscriber.close();
    }

    // --- Test 2: One-shot subscribe via connection manager ---

    @Test
    void subscribe_collectsPublishedMessage() throws Exception {
        String topic = "integration/subscribe/test";

        // Connect a subscriber via connection manager
        SocketHolder subHolder = CentralConnectionManager.getInstance().getOrConnect(
                BROKER_URL, "sub-client-1", null, null, 10, true);

        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<String> receivedPayload = new AtomicReference<>();

        subHolder.getClient().subscribe(topic, 1, (t, msg) -> {
            receivedPayload.set(new String(msg.getPayload(), StandardCharsets.UTF_8));
            received.countDown();
        }).waitForCompletion(3000);

        // Publish from a separate client
        MqttAsyncClient publisher = new MqttAsyncClient(BROKER_URL, "pub-for-sub", new MemoryPersistence());
        MqttConnectOptions pubOpts = new MqttConnectOptions();
        pubOpts.setCleanSession(true);
        publisher.connect(pubOpts).waitForCompletion(5000);

        MqttMessage message = new MqttMessage("subscribe-test-payload".getBytes(StandardCharsets.UTF_8));
        message.setQos(1);
        publisher.publish(topic, message).waitForCompletion(5000);

        // Verify
        assertTrue(received.await(5, TimeUnit.SECONDS));
        assertEquals("subscribe-test-payload", receivedPayload.get());

        // Cleanup
        subHolder.getClient().unsubscribe(topic).waitForCompletion(2000);
        publisher.disconnect().waitForCompletion(2000);
        publisher.close();
    }

    // --- Test 3: Connection pooling ---

    @Test
    void connectionPooling_reusesSameHolder() throws Exception {
        SocketHolder holder1 = CentralConnectionManager.getInstance().getOrConnect(
                BROKER_URL, "pool-client-1", null, null, 10, true);
        SocketHolder holder2 = CentralConnectionManager.getInstance().getOrConnect(
                BROKER_URL, "pool-client-1", null, null, 10, true);

        // Same key should return same holder
        assertSame(holder1, holder2);

        // Different client ID should return different holder
        SocketHolder holder3 = CentralConnectionManager.getInstance().getOrConnect(
                BROKER_URL, "pool-client-2", null, null, 10, true);
        assertNotSame(holder1, holder3);

        assertEquals(2, CentralConnectionManager.getInstance().getActiveConnectionCount());
    }

    // --- Test 4: Idle eviction ---

    @Test
    void idleEviction_removesStaleConnection() throws Exception {
        SocketHolder holder = CentralConnectionManager.getInstance().getOrConnect(
                BROKER_URL, "evict-client", null, null, 10, true);

        String key = CentralConnectionManager.buildConnectionKey(BROKER_URL, "evict-client");
        assertTrue(CentralConnectionManager.getInstance().hasConnection(key));

        // Manually set timestamp to past (simulate idle > 60s)
        var timestampField = SocketHolder.class.getDeclaredField("lastAccessedTimestamp");
        timestampField.setAccessible(true);
        var atomicLong = (java.util.concurrent.atomic.AtomicLong) timestampField.get(holder);
        atomicLong.set(System.currentTimeMillis() - CentralConnectionManager.IDLE_TIMEOUT_MS - 5000);

        // Trigger eviction manually
        CentralConnectionManager.getInstance().evictIdleSockets();

        // Connection should be removed
        assertFalse(CentralConnectionManager.getInstance().hasConnection(key));
        assertEquals(0, CentralConnectionManager.getInstance().getActiveConnectionCount());
    }

    // --- Test 5: Throttler integration ---

    @Test
    void throttler_handlesOverflow() {
        int capacity = 100;
        MqttInboundThrottler throttler = new MqttInboundThrottler(capacity, 0, null);

        // Feed more messages than capacity
        int totalMessages = 200;
        int accepted = 0;
        for (int i = 0; i < totalMessages; i++) {
            InboundMqttEvent event = new InboundMqttEvent(
                    "overflow/topic",
                    ("{\"seq\":" + i + "}").getBytes(StandardCharsets.UTF_8),
                    0);
            if (throttler.offer(event)) {
                accepted++;
            }
        }

        // Exactly capacity should be buffered
        assertEquals(capacity, throttler.getQueueSize());
        assertEquals(capacity, accepted);
        assertEquals(totalMessages - capacity, throttler.getDroppedCount());

        throttler.shutdown();
    }
}
