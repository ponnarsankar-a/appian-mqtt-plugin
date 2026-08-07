package com.example.appian.mqtt.core;

import org.eclipse.paho.client.mqttv3.MqttAsyncClient;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Encapsulates an active MQTT async client socket along with activity tracking
 * for automated idle eviction.
 */
public class SocketHolder implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(SocketHolder.class.getName());

    private final String connectionKey;
    private final String brokerUrl;
    private final String clientId;
    private final MqttAsyncClient client;
    private final AtomicLong lastAccessedTimestamp;

    public SocketHolder(String connectionKey, String brokerUrl, String clientId, MqttAsyncClient client) {
        this.connectionKey = connectionKey;
        this.brokerUrl = brokerUrl;
        this.clientId = clientId;
        this.client = client;
        this.lastAccessedTimestamp = new AtomicLong(System.currentTimeMillis());
    }

    public String getConnectionKey() {
        return connectionKey;
    }

    public String getBrokerUrl() {
        return brokerUrl;
    }

    public String getClientId() {
        return clientId;
    }

    public MqttAsyncClient getClient() {
        touch();
        return client;
    }

    public long getLastAccessedTimestamp() {
        return lastAccessedTimestamp.get();
    }

    public void touch() {
        this.lastAccessedTimestamp.set(System.currentTimeMillis());
    }

    public boolean isConnected() {
        return client != null && client.isConnected();
    }

    @Override
    public void close() {
        if (client != null) {
            try {
                if (client.isConnected()) {
                    LOG.info("Disconnecting MQTT client for key: " + connectionKey);
                    client.disconnectForcibly(1000, 2000);
                }
                LOG.info("Closing MQTT client resources for key: " + connectionKey);
                client.close();
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Error closing MQTT client socket for key: " + connectionKey, e);
            }
        }
    }
}
