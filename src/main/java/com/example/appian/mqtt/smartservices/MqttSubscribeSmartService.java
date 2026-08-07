package com.example.appian.mqtt.smartservices;

import com.appiancorp.suiteapi.process.framework.AppianSmartService;
import com.appiancorp.suiteapi.process.framework.Input;
import com.appiancorp.suiteapi.process.framework.Order;
import com.appiancorp.suiteapi.process.framework.Output;
import com.appiancorp.suiteapi.process.framework.Required;
import com.appiancorp.suiteapi.process.palette.PaletteInfo;
import com.example.appian.mqtt.core.AppianProcessLauncher;
import com.example.appian.mqtt.core.CentralConnectionManager;
import com.example.appian.mqtt.core.MqttInboundThrottler;
import com.example.appian.mqtt.core.MqttInboundThrottler.InboundMqttEvent;
import com.example.appian.mqtt.core.SocketHolder;
import com.appiancorp.suiteapi.process.ProcessExecutionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MQTT Subscribe Smart Service for Appian Process Models.
 * <p>
 * Supports two modes:
 * <ul>
 *   <li><b>ONE_SHOT</b>: Subscribes, collects up to maxMessages or until timeout,
 *       then unsubscribes and returns collected messages as JSON.</li>
 *   <li><b>PERSISTENT</b>: Starts a long-lived background listener that triggers
 *       Appian process instances for each inbound message via ProcessExecutionService.
 *       Messages are buffered through MqttInboundThrottler for rate limiting and filtering.</li>
 * </ul>
 * <p>
 * Lifecycle management:
 * <ul>
 *   <li>Persistent listeners are registered with a unique listenerId</li>
 *   <li>Call {@link #stopListener(String)} to gracefully stop a persistent listener</li>
 *   <li>ListenerHandle.stop() is idempotent and cleans up all resources</li>
 * </ul>
 */
@PaletteInfo(paletteCategory = "Integration Services", palette = "MQTT")
public class MqttSubscribeSmartService extends AppianSmartService {

    private static final Logger LOG = Logger.getLogger(MqttSubscribeSmartService.class.getName());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int DEFAULT_CONNECTION_TIMEOUT = 10;

    /** Registry of active persistent listeners for lifecycle management */
    private static final ConcurrentHashMap<String, ListenerHandle> LISTENER_REGISTRY = new ConcurrentHashMap<>();

    // --- Input fields ---
    private String brokerUrl;
    private String clientId;
    private String username;
    private String password;
    private String topic;
    private Integer qos;
    private String mode; // "ONE_SHOT" or "PERSISTENT"

    // One-shot inputs
    private Integer maxMessages;
    private Long timeoutMs;

    // Persistent inputs
    private Long processModelId;
    private String serviceAccountUsername;
    private Integer maxMessagesPerSecond;
    private Integer queueCapacity;
    private String jsonFilterExpression;

    // For persistent mode: injected ProcessExecutionService
    private ProcessExecutionService processExecutionService;

    // --- Output fields ---
    private boolean success;
    private String errorMessage;
    private String collectedMessages; // JSON array for ONE_SHOT
    private String listenerId; // For PERSISTENT mode management

    // --- Input setters ---

    @Input(required = Required.ALWAYS, order = 1)
    @Order(1)
    public void setBrokerUrl(String brokerUrl) { this.brokerUrl = brokerUrl; }

    @Input(required = Required.ALWAYS, order = 2)
    @Order(2)
    public void setClientId(String clientId) { this.clientId = clientId; }

    @Input(required = Required.OPTIONAL, order = 3)
    @Order(3)
    public void setUsername(String username) { this.username = username; }

    @Input(required = Required.OPTIONAL, order = 4)
    @Order(4)
    public void setPassword(String password) { this.password = password; }

    @Input(required = Required.ALWAYS, order = 5)
    @Order(5)
    public void setTopic(String topic) { this.topic = topic; }

    @Input(required = Required.OPTIONAL, order = 6)
    @Order(6)
    public void setQos(Integer qos) { this.qos = qos; }

    @Input(required = Required.ALWAYS, order = 7)
    @Order(7)
    public void setMode(String mode) { this.mode = mode; }

    @Input(required = Required.OPTIONAL, order = 8)
    @Order(8)
    public void setMaxMessages(Integer maxMessages) { this.maxMessages = maxMessages; }

    @Input(required = Required.OPTIONAL, order = 9)
    @Order(9)
    public void setTimeoutMs(Long timeoutMs) { this.timeoutMs = timeoutMs; }

    @Input(required = Required.OPTIONAL, order = 10)
    @Order(10)
    public void setProcessModelId(Long processModelId) { this.processModelId = processModelId; }

    @Input(required = Required.OPTIONAL, order = 11)
    @Order(11)
    public void setServiceAccountUsername(String serviceAccountUsername) { this.serviceAccountUsername = serviceAccountUsername; }

    @Input(required = Required.OPTIONAL, order = 12)
    @Order(12)
    public void setMaxMessagesPerSecond(Integer maxMessagesPerSecond) { this.maxMessagesPerSecond = maxMessagesPerSecond; }

    @Input(required = Required.OPTIONAL, order = 13)
    @Order(13)
    public void setQueueCapacity(Integer queueCapacity) { this.queueCapacity = queueCapacity; }

    @Input(required = Required.OPTIONAL, order = 14)
    @Order(14)
    public void setJsonFilterExpression(String jsonFilterExpression) { this.jsonFilterExpression = jsonFilterExpression; }

    // --- Non-annotation setter for dependency injection (used in persistent mode) ---
    // NOTE: ProcessExecutionService must NOT be exposed as a public setter because
    // Appian's smart service framework will attempt to resolve it as an input type.
    void setProcessExecutionService(ProcessExecutionService processExecutionService) {
        this.processExecutionService = processExecutionService;
    }

    // --- Output getters ---

    @Output(order = 1)
    public boolean isSuccess() { return success; }

    @Output(order = 2)
    public String getErrorMessage() { return errorMessage; }

    @Output(order = 3)
    public String getCollectedMessages() { return collectedMessages; }

    @Output(order = 4)
    public String getListenerId() { return listenerId; }

    // --- Core execution ---

    @Override
    public void run() throws Exception {
        super.run();

        try {
            if ("PERSISTENT".equalsIgnoreCase(mode)) {
                executePersistentMode();
            } else {
                executeOneShotMode();
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "MQTT Subscribe failed: mode=" + mode + ", topic=" + topic, e);
            this.success = false;
            this.errorMessage = e.getMessage();
        }
    }

    // --- ONE_SHOT mode ---

    private void executeOneShotMode() throws Exception {
        int msgLimit = (maxMessages != null && maxMessages > 0) ? maxMessages : 10;
        long timeout = (timeoutMs != null && timeoutMs > 0) ? timeoutMs : 5000L;
        int qosLevel = (qos != null) ? qos : 0;

        SocketHolder socketHolder = CentralConnectionManager.getInstance().getOrConnect(
                brokerUrl, clientId, username, password, DEFAULT_CONNECTION_TIMEOUT, true);

        MqttAsyncClient client = socketHolder.getClient();
        List<Map<String, Object>> collected = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(msgLimit);

        // Subscribe with callback that collects messages
        IMqttMessageListener listener = (receivedTopic, message) -> {
            if (latch.getCount() > 0) {
                Map<String, Object> msgMap = new HashMap<>();
                msgMap.put("topic", receivedTopic);
                msgMap.put("payload", new String(message.getPayload(), StandardCharsets.UTF_8));
                msgMap.put("qos", message.getQos());
                msgMap.put("retained", message.isRetained());
                msgMap.put("timestamp", System.currentTimeMillis());

                synchronized (collected) {
                    collected.add(msgMap);
                }
                latch.countDown();
            }
        };

        client.subscribe(topic, qosLevel, listener).waitForCompletion(5000);

        try {
            // Wait for messages or timeout
            latch.await(timeout, TimeUnit.MILLISECONDS);
        } finally {
            // Unsubscribe
            try {
                client.unsubscribe(topic).waitForCompletion(3000);
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Failed to unsubscribe from topic: " + topic, e);
            }
        }

        // Serialize collected messages to JSON
        this.collectedMessages = OBJECT_MAPPER.writeValueAsString(collected);
        this.success = true;
        this.errorMessage = null;

        LOG.info("ONE_SHOT subscribe complete: topic=" + topic + ", collected=" + collected.size());
    }

    // --- PERSISTENT mode ---

    private void executePersistentMode() throws Exception {
        int qosLevel = (qos != null) ? qos : 0;
        int capacity = (queueCapacity != null && queueCapacity > 0) ? queueCapacity : 1000;
        int rateLimit = (maxMessagesPerSecond != null && maxMessagesPerSecond > 0) ? maxMessagesPerSecond : 50;

        if (processModelId == null) {
            throw new IllegalArgumentException("processModelId is required for PERSISTENT mode");
        }
        if (serviceAccountUsername == null || serviceAccountUsername.isBlank()) {
            throw new IllegalArgumentException("serviceAccountUsername is required for PERSISTENT mode");
        }

        SocketHolder socketHolder = CentralConnectionManager.getInstance().getOrConnect(
                brokerUrl, clientId, username, password, DEFAULT_CONNECTION_TIMEOUT, true);

        MqttAsyncClient client = socketHolder.getClient();

        // Create throttler
        MqttInboundThrottler throttler = new MqttInboundThrottler(capacity, rateLimit, jsonFilterExpression);

        // Subscribe with callback that feeds into the throttler
        IMqttMessageListener mqttListener = (receivedTopic, message) -> {
            InboundMqttEvent event = new InboundMqttEvent(
                    receivedTopic, message.getPayload(), message.getQos());
            throttler.offer(event);
        };

        client.subscribe(topic, qosLevel, mqttListener).waitForCompletion(5000);

        // Create process launcher
        AppianProcessLauncher processLauncher = createProcessLauncher();

        // Start consumer loop on the worker pool
        String id = UUID.randomUUID().toString();
        Future<?> consumerFuture = CentralConnectionManager.getInstance().getWorkerPool().submit(() -> {
            LOG.info("Persistent listener consumer started: id=" + id + ", topic=" + topic);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    InboundMqttEvent event = throttler.poll(1000);
                    if (event != null) {
                        Map<String, Object> processParams = new HashMap<>();
                        processParams.put("mqttTopic", event.getTopic());
                        processParams.put("mqttPayload", event.getPayloadAsString());
                        processParams.put("mqttQos", (long) event.getQos());
                        processParams.put("mqttTimestamp", event.getReceivedTimestamp());

                        processLauncher.triggerProcess(processModelId, processParams);
                    }

                    // Check if client is still connected
                    if (!client.isConnected()) {
                        LOG.warning("MQTT client disconnected, stopping listener: " + id);
                        break;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    LOG.log(Level.WARNING, "Error in persistent listener consumer: " + id, e);
                }
            }
            LOG.info("Persistent listener consumer stopped: id=" + id);
            // Self-remove from registry on exit
            LISTENER_REGISTRY.remove(id);
        });

        // Register handle
        ListenerHandle handle = new ListenerHandle(id, topic, client, throttler, consumerFuture);
        LISTENER_REGISTRY.put(id, handle);

        this.listenerId = id;
        this.success = true;
        this.errorMessage = null;

        LOG.info("PERSISTENT listener started: id=" + id + ", topic=" + topic
                + ", rateLimit=" + rateLimit + "/s, capacity=" + capacity);
    }

    /**
     * Creates the AppianProcessLauncher. Extracted for testability.
     */
    AppianProcessLauncher createProcessLauncher() {
        if (processExecutionService == null) {
            throw new IllegalStateException("ProcessExecutionService not available. "
                    + "Ensure the service is injected before running in PERSISTENT mode.");
        }
        return new AppianProcessLauncher(processExecutionService, serviceAccountUsername);
    }

    // --- Static listener management ---

    /**
     * Stops a persistent listener by its ID.
     *
     * @param listenerId The unique ID returned when the listener was started
     * @return true if the listener was found and stopped, false if not found
     */
    public static boolean stopListener(String listenerId) {
        ListenerHandle handle = LISTENER_REGISTRY.remove(listenerId);
        if (handle != null) {
            handle.stop();
            return true;
        }
        return false;
    }

    /**
     * Returns the number of active persistent listeners.
     */
    public static int getActiveListenerCount() {
        return LISTENER_REGISTRY.size();
    }

    /**
     * Returns the listener registry (for testing).
     */
    public static ConcurrentHashMap<String, ListenerHandle> getListenerRegistry() {
        return LISTENER_REGISTRY;
    }

    // --- ListenerHandle: lifecycle wrapper for persistent listeners ---

    /**
     * Holds references to all resources associated with a persistent listener.
     * Provides an idempotent stop() method for clean shutdown.
     */
    public static class ListenerHandle {
        private final String id;
        private final String topic;
        private final MqttAsyncClient client;
        private final MqttInboundThrottler throttler;
        private final Future<?> consumerFuture;
        private volatile boolean stopped = false;

        ListenerHandle(String id, String topic, MqttAsyncClient client,
                       MqttInboundThrottler throttler, Future<?> consumerFuture) {
            this.id = id;
            this.topic = topic;
            this.client = client;
            this.throttler = throttler;
            this.consumerFuture = consumerFuture;
        }

        /**
         * Idempotent shutdown: unsubscribes, cancels consumer, shuts down throttler.
         */
        public synchronized void stop() {
            if (stopped) return;
            stopped = true;

            LOG.info("Stopping persistent listener: id=" + id + ", topic=" + topic);

            // Cancel consumer thread
            consumerFuture.cancel(true);

            // Unsubscribe from topic
            try {
                if (client.isConnected()) {
                    client.unsubscribe(topic).waitForCompletion(3000);
                }
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Error unsubscribing during listener stop: " + id, e);
            }

            // Shutdown throttler
            throttler.shutdown();

            LOG.info("Persistent listener stopped: id=" + id);
        }

        public String getId() { return id; }
        public String getTopic() { return topic; }
        public boolean isStopped() { return stopped; }

        public long getDroppedCount() { return throttler.getDroppedCount(); }
        public long getAcceptedCount() { return throttler.getAcceptedCount(); }
        public int getQueueSize() { return throttler.getQueueSize(); }
    }
}
