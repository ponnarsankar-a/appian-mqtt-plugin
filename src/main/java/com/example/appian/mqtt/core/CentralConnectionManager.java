package com.example.appian.mqtt.core;

import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * JVM-wide singleton managing pooled MQTT connections.
 * <p>
 * Features:
 * <ul>
 *   <li>Thread-safe connection registry (ConcurrentHashMap)</li>
 *   <li>Bounded worker thread pool (capped at MAX_WORKER_THREADS)</li>
 *   <li>Automatic idle socket eviction (configurable interval and timeout)</li>
 *   <li>Connection pool cap to prevent resource exhaustion</li>
 * </ul>
 */
public class CentralConnectionManager {

    private static final Logger LOG = Logger.getLogger(CentralConnectionManager.class.getName());

    // --- Configurable constants ---
    public static final int MAX_WORKER_THREADS = 5;
    public static final long EVICTION_INTERVAL_MS = 30_000L;
    public static final long IDLE_TIMEOUT_MS = 60_000L;
    public static final int MAX_CONNECTIONS = 20;

    // --- Singleton holder pattern (lazy, thread-safe) ---
    private static class InstanceHolder {
        private static final CentralConnectionManager INSTANCE = new CentralConnectionManager();
    }

    public static CentralConnectionManager getInstance() {
        return InstanceHolder.INSTANCE;
    }

    // --- Internal state ---
    private final ConcurrentHashMap<String, SocketHolder> connectionRegistry;
    private final ScheduledExecutorService workerPool;
    private final ScheduledFuture<?> evictionTask;

    private CentralConnectionManager() {
        this.connectionRegistry = new ConcurrentHashMap<>();
        this.workerPool = Executors.newScheduledThreadPool(MAX_WORKER_THREADS, r -> {
            Thread t = new Thread(r, "mqtt-worker-" + System.nanoTime());
            t.setDaemon(true);
            return t;
        });
        this.evictionTask = workerPool.scheduleAtFixedRate(
                this::evictIdleSockets,
                EVICTION_INTERVAL_MS,
                EVICTION_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
        LOG.info("CentralConnectionManager initialized with pool cap=" + MAX_CONNECTIONS
                + ", workers=" + MAX_WORKER_THREADS
                + ", eviction interval=" + EVICTION_INTERVAL_MS + "ms"
                + ", idle timeout=" + IDLE_TIMEOUT_MS + "ms");
    }

    /**
     * Retrieves an existing connection or creates a new one.
     *
     * @param brokerUrl         MQTT broker URL (e.g. tcp://broker.example.com:1883)
     * @param clientId          Unique client identifier
     * @param username          Username for authentication (nullable)
     * @param password          Password for authentication (nullable)
     * @param connectionTimeout Connection timeout in seconds
     * @param cleanSession      Whether to use a clean MQTT session
     * @return SocketHolder wrapping the connected MqttAsyncClient
     * @throws MqttException       if connection fails
     * @throws IllegalStateException if the connection pool is at capacity
     */
    public synchronized SocketHolder getOrConnect(
            String brokerUrl,
            String clientId,
            String username,
            String password,
            int connectionTimeout,
            boolean cleanSession) throws MqttException {

        // Normalize protocol scheme for Paho compatibility
        brokerUrl = normalizeScheme(brokerUrl);

        String connectionKey = buildConnectionKey(brokerUrl, clientId);

        // Check for existing connected socket
        SocketHolder existing = connectionRegistry.get(connectionKey);
        if (existing != null && existing.isConnected()) {
            existing.touch();
            LOG.fine("Reusing existing MQTT connection for key: " + connectionKey);
            return existing;
        }

        // Remove stale entry if present but disconnected
        if (existing != null) {
            LOG.info("Removing stale disconnected connection for key: " + connectionKey);
            connectionRegistry.remove(connectionKey);
            existing.close();
        }

        // Enforce pool cap
        if (connectionRegistry.size() >= MAX_CONNECTIONS) {
            throw new IllegalStateException(
                    "Connection pool at capacity (" + MAX_CONNECTIONS + "). "
                            + "Cannot create new connection for key: " + connectionKey);
        }

        // Create new connection
        LOG.info("Creating new MQTT connection for key: " + connectionKey);
        MqttAsyncClient client = new MqttAsyncClient(brokerUrl, clientId, new MemoryPersistence());

        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(cleanSession);
        options.setConnectionTimeout(connectionTimeout);
        options.setMaxInflight(10);

        if (username != null && !username.isBlank()) {
            options.setUserName(username);
        }
        if (password != null && !password.isBlank()) {
            options.setPassword(password.toCharArray());
        }

        client.connect(options).waitForCompletion(connectionTimeout * 1000L);

        SocketHolder holder = new SocketHolder(connectionKey, brokerUrl, clientId, client);
        connectionRegistry.put(connectionKey, holder);

        LOG.info("MQTT connection established for key: " + connectionKey);
        return holder;
    }

    /**
     * Releases and closes a specific connection.
     *
     * @param connectionKey The key identifying the connection to release
     */
    public void releaseConnection(String connectionKey) {
        SocketHolder holder = connectionRegistry.remove(connectionKey);
        if (holder != null) {
            LOG.info("Releasing MQTT connection for key: " + connectionKey);
            holder.close();
        }
    }

    /**
     * Returns the shared bounded worker thread pool for background tasks.
     */
    public ScheduledExecutorService getWorkerPool() {
        return workerPool;
    }

    /**
     * Returns the current number of active connections in the pool.
     */
    public int getActiveConnectionCount() {
        return connectionRegistry.size();
    }

    /**
     * Checks if a connection exists for the given key.
     */
    public boolean hasConnection(String connectionKey) {
        return connectionRegistry.containsKey(connectionKey);
    }

    /**
     * Shuts down all connections, clears the registry, and terminates the worker pool.
     * Intended for JVM shutdown or plugin unload scenarios.
     */
    public void shutdown() {
        LOG.info("Shutting down CentralConnectionManager...");

        evictionTask.cancel(false);

        for (Map.Entry<String, SocketHolder> entry : connectionRegistry.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Error closing connection: " + entry.getKey(), e);
            }
        }
        connectionRegistry.clear();

        workerPool.shutdown();
        try {
            if (!workerPool.awaitTermination(5, TimeUnit.SECONDS)) {
                workerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            workerPool.shutdownNow();
            Thread.currentThread().interrupt();
        }

        LOG.info("CentralConnectionManager shutdown complete.");
    }

    /**
     * Evicts sockets that have been idle longer than IDLE_TIMEOUT_MS.
     * Called periodically by the scheduled executor.
     */
    public void evictIdleSockets() {
        long now = System.currentTimeMillis();
        int evictedCount = 0;

        for (Map.Entry<String, SocketHolder> entry : connectionRegistry.entrySet()) {
            SocketHolder holder = entry.getValue();
            long idleTime = now - holder.getLastAccessedTimestamp();

            if (idleTime > IDLE_TIMEOUT_MS) {
                LOG.info("Evicting idle MQTT connection (idle " + idleTime + "ms): " + entry.getKey());
                connectionRegistry.remove(entry.getKey());
                holder.close();
                evictedCount++;
            }
        }

        if (evictedCount > 0) {
            LOG.info("Idle socket eviction complete. Evicted: " + evictedCount
                    + ", remaining: " + connectionRegistry.size());
        }
    }

    /**
     * Normalizes the broker URL scheme for Paho compatibility.
     * mqtt:// → tcp://, mqtts:// → ssl://
     */
    static String normalizeScheme(String brokerUrl) {
        if (brokerUrl == null) return brokerUrl;
        if (brokerUrl.startsWith("mqtt://")) {
            return "tcp://" + brokerUrl.substring("mqtt://".length());
        }
        if (brokerUrl.startsWith("mqtts://")) {
            return "ssl://" + brokerUrl.substring("mqtts://".length());
        }
        return brokerUrl;
    }

    /**
     * Builds the connection key from broker URL and client ID.
     */
    public static String buildConnectionKey(String brokerUrl, String clientId) {
        return brokerUrl + "::" + clientId;
    }

    // --- Visible for testing ---

    public ConcurrentHashMap<String, SocketHolder> getConnectionRegistry() {
        return connectionRegistry;
    }
}
