package com.example.appian.mqtt.core;

import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CentralConnectionManagerTest {

    private CentralConnectionManager manager;

    @BeforeEach
    void setUp() {
        manager = CentralConnectionManager.getInstance();
        // Clear any leftover connections from previous tests
        manager.getConnectionRegistry().clear();
    }

    @AfterEach
    void tearDown() {
        // Clean up connections after each test
        manager.getConnectionRegistry().forEach((key, holder) -> {
            try {
                holder.close();
            } catch (Exception ignored) {}
        });
        manager.getConnectionRegistry().clear();
    }

    @Test
    void getInstance_returnsSameInstance() {
        CentralConnectionManager instance1 = CentralConnectionManager.getInstance();
        CentralConnectionManager instance2 = CentralConnectionManager.getInstance();
        assertSame(instance1, instance2);
    }

    @Test
    void buildConnectionKey_combinesBrokerUrlAndClientId() {
        String key = CentralConnectionManager.buildConnectionKey("tcp://broker:1883", "client1");
        assertEquals("tcp://broker:1883::client1", key);
    }

    @Test
    void getActiveConnectionCount_initiallyZero() {
        assertEquals(0, manager.getActiveConnectionCount());
    }

    @Test
    void getWorkerPool_returnsNonNull() {
        assertNotNull(manager.getWorkerPool());
        assertFalse(manager.getWorkerPool().isShutdown());
    }

    @Test
    void hasConnection_returnsFalseForNonExistentKey() {
        assertFalse(manager.hasConnection("tcp://nonexistent:1883::client"));
    }

    @Test
    void releaseConnection_removesAndClosesHolder() {
        // Manually inject a mock holder
        String key = "tcp://test:1883::client1";
        MqttAsyncClient mockClient = mock(MqttAsyncClient.class);
        when(mockClient.isConnected()).thenReturn(true);
        SocketHolder holder = new SocketHolder(key, "tcp://test:1883", "client1", mockClient);

        manager.getConnectionRegistry().put(key, holder);
        assertTrue(manager.hasConnection(key));
        assertEquals(1, manager.getActiveConnectionCount());

        manager.releaseConnection(key);

        assertFalse(manager.hasConnection(key));
        assertEquals(0, manager.getActiveConnectionCount());
    }

    @Test
    void releaseConnection_noOpForNonExistentKey() {
        // Should not throw
        assertDoesNotThrow(() -> manager.releaseConnection("nonexistent::key"));
    }

    @Test
    void getOrConnect_reusesExistingConnectedSocket() throws Exception {
        // Inject a mock connected holder
        String brokerUrl = "tcp://reuse-test:1883";
        String clientId = "reuseClient";
        String key = CentralConnectionManager.buildConnectionKey(brokerUrl, clientId);

        MqttAsyncClient mockClient = mock(MqttAsyncClient.class);
        when(mockClient.isConnected()).thenReturn(true);
        SocketHolder existingHolder = new SocketHolder(key, brokerUrl, clientId, mockClient);

        manager.getConnectionRegistry().put(key, existingHolder);

        // Should return the same holder without creating a new connection
        SocketHolder result = manager.getOrConnect(brokerUrl, clientId, null, null, 10, true);
        assertSame(existingHolder, result);
    }

    @Test
    void getOrConnect_removesStaleDisconnectedEntry() throws Exception {
        // Inject a disconnected mock holder
        String brokerUrl = "tcp://stale-test:1883";
        String clientId = "staleClient";
        String key = CentralConnectionManager.buildConnectionKey(brokerUrl, clientId);

        MqttAsyncClient mockClient = mock(MqttAsyncClient.class);
        when(mockClient.isConnected()).thenReturn(false);
        SocketHolder staleHolder = new SocketHolder(key, brokerUrl, clientId, mockClient);

        manager.getConnectionRegistry().put(key, staleHolder);

        // Attempting to connect will fail because there's no real broker,
        // but it should first remove the stale entry
        assertThrows(MqttException.class, () ->
                manager.getOrConnect(brokerUrl, clientId, null, null, 1, true));

        // Stale entry should have been removed before the new connection attempt
        assertFalse(manager.getConnectionRegistry().containsKey(key));
    }

    @Test
    void getOrConnect_throwsWhenPoolAtCapacity() {
        // Fill the registry to MAX_CONNECTIONS with mock holders
        for (int i = 0; i < CentralConnectionManager.MAX_CONNECTIONS; i++) {
            String key = "tcp://cap-test:1883::client" + i;
            MqttAsyncClient mockClient = mock(MqttAsyncClient.class);
            when(mockClient.isConnected()).thenReturn(true);
            SocketHolder holder = new SocketHolder(key, "tcp://cap-test:1883", "client" + i, mockClient);
            manager.getConnectionRegistry().put(key, holder);
        }

        assertEquals(CentralConnectionManager.MAX_CONNECTIONS, manager.getActiveConnectionCount());

        // Attempting to create one more should throw
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                manager.getOrConnect("tcp://cap-test:1883", "clientOverflow", null, null, 1, true));

        assertTrue(ex.getMessage().contains("Connection pool at capacity"));
    }

    @Test
    void evictIdleSockets_removesSocketsIdleBeyondTimeout() {
        // Create a holder with timestamp set far in the past
        String key = "tcp://evict-test:1883::evictMe";
        MqttAsyncClient mockClient = mock(MqttAsyncClient.class);
        when(mockClient.isConnected()).thenReturn(true);
        SocketHolder holder = new SocketHolder(key, "tcp://evict-test:1883", "evictMe", mockClient);

        // Manually set timestamp to past (idle > 60s)
        try {
            var timestampField = SocketHolder.class.getDeclaredField("lastAccessedTimestamp");
            timestampField.setAccessible(true);
            var atomicLong = (java.util.concurrent.atomic.AtomicLong) timestampField.get(holder);
            atomicLong.set(System.currentTimeMillis() - CentralConnectionManager.IDLE_TIMEOUT_MS - 5000);
        } catch (Exception e) {
            fail("Failed to set timestamp via reflection: " + e.getMessage());
        }

        manager.getConnectionRegistry().put(key, holder);
        assertEquals(1, manager.getActiveConnectionCount());

        // Run eviction
        manager.evictIdleSockets();

        assertEquals(0, manager.getActiveConnectionCount());
        assertFalse(manager.hasConnection(key));
    }

    @Test
    void evictIdleSockets_keepsRecentlyAccessedSockets() {
        // Create a holder with fresh timestamp (just accessed)
        String key = "tcp://fresh-test:1883::keepMe";
        MqttAsyncClient mockClient = mock(MqttAsyncClient.class);
        when(mockClient.isConnected()).thenReturn(true);
        SocketHolder holder = new SocketHolder(key, "tcp://fresh-test:1883", "keepMe", mockClient);
        holder.touch(); // Ensure fresh timestamp

        manager.getConnectionRegistry().put(key, holder);

        // Run eviction
        manager.evictIdleSockets();

        // Should still be there
        assertEquals(1, manager.getActiveConnectionCount());
        assertTrue(manager.hasConnection(key));
    }

    @Test
    void maxConnections_constantIsReasonable() {
        assertEquals(20, CentralConnectionManager.MAX_CONNECTIONS);
    }

    @Test
    void maxWorkerThreads_constantIsReasonable() {
        assertEquals(5, CentralConnectionManager.MAX_WORKER_THREADS);
    }
}
