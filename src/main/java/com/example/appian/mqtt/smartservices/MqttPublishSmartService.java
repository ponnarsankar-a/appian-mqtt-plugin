package com.example.appian.mqtt.smartservices;

import com.appiancorp.suiteapi.process.framework.AppianSmartService;
import com.appiancorp.suiteapi.process.framework.Input;
import com.appiancorp.suiteapi.process.framework.Order;
import com.appiancorp.suiteapi.process.framework.Output;
import com.appiancorp.suiteapi.process.framework.Required;
import com.appiancorp.suiteapi.process.palette.PaletteInfo;
import com.example.appian.mqtt.core.CentralConnectionManager;
import com.example.appian.mqtt.core.SocketHolder;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MQTT Publish Smart Service for Appian Process Models.
 * <p>
 * Appears as a draggable node in the Process Modeler palette.
 * Publishes a message to an MQTT topic when the process node activates.
 * <p>
 * Lifecycle:
 * <ol>
 *   <li>Appian sets all @Input properties from process variables</li>
 *   <li>Appian calls run() — publishes the MQTT message</li>
 *   <li>Appian reads all @Output properties back into process variables</li>
 * </ol>
 */
@PaletteInfo(paletteCategory = "Integration Services", palette = "MQTT")
public class MqttPublishSmartService extends AppianSmartService {

    private static final Logger LOG = Logger.getLogger(MqttPublishSmartService.class.getName());
    private static final int DEFAULT_CONNECTION_TIMEOUT = 10;
    private static final long PUBLISH_TIMEOUT_MS = 10_000L;

    // --- Input fields ---
    private String brokerUrl;
    private String clientId;
    private String username;
    private String password;
    private String topic;
    private String payload;
    private Integer qos;
    private Boolean retained;

    // --- Output fields ---
    private boolean success;
    private String errorMessage;
    private String messageId;

    // --- Input setters ---

    @Input(required = Required.ALWAYS, order = 1)
    @Order(1)
    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    @Input(required = Required.ALWAYS, order = 2)
    @Order(2)
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Input(required = Required.OPTIONAL, order = 3)
    @Order(3)
    public void setUsername(String username) {
        this.username = username;
    }

    @Input(required = Required.OPTIONAL, order = 4)
    @Order(4)
    public void setPassword(String password) {
        this.password = password;
    }

    @Input(required = Required.ALWAYS, order = 5)
    @Order(5)
    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Input(required = Required.ALWAYS, order = 6)
    @Order(6)
    public void setPayload(String payload) {
        this.payload = payload;
    }

    @Input(required = Required.OPTIONAL, order = 7)
    @Order(7)
    public void setQos(Integer qos) {
        this.qos = qos;
    }

    @Input(required = Required.OPTIONAL, order = 8)
    @Order(8)
    public void setRetained(Boolean retained) {
        this.retained = retained;
    }

    // --- Output getters ---

    @Output(order = 1)
    public boolean isSuccess() {
        return success;
    }

    @Output(order = 2)
    public String getErrorMessage() {
        return errorMessage;
    }

    @Output(order = 3)
    public String getMessageId() {
        return messageId;
    }

    // --- Core execution ---

    @Override
    public void run() throws Exception {
        super.run();

        int qosLevel = (qos != null) ? qos : 0;
        boolean retainFlag = (retained != null) ? retained : false;

        try {
            SocketHolder socketHolder = CentralConnectionManager.getInstance().getOrConnect(
                    brokerUrl, clientId, username, password,
                    DEFAULT_CONNECTION_TIMEOUT, true);

            MqttMessage message = new MqttMessage(
                    payload != null ? payload.getBytes(StandardCharsets.UTF_8) : new byte[0]);
            message.setQos(qosLevel);
            message.setRetained(retainFlag);

            socketHolder.getClient().publish(topic, message).waitForCompletion(PUBLISH_TIMEOUT_MS);

            this.success = true;
            this.messageId = CentralConnectionManager.buildConnectionKey(brokerUrl, clientId)
                    + "::" + topic + "::" + System.currentTimeMillis();
            this.errorMessage = null;

            LOG.info("MQTT Smart Service publish successful: topic=" + topic);

        } catch (Exception e) {
            LOG.log(Level.SEVERE, "MQTT Smart Service publish failed: topic=" + topic, e);
            this.success = false;
            this.errorMessage = e.getMessage();
            this.messageId = null;
            // Do NOT re-throw — allow the process to continue with error outputs
        }
    }
}
