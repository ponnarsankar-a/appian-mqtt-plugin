package com.example.appian.mqtt.templates;

import com.appian.connectedsystems.simplified.sdk.SimpleIntegrationTemplate;
import com.appian.connectedsystems.simplified.sdk.configuration.SimpleConfiguration;
import com.appian.connectedsystems.templateframework.sdk.ExecutionContext;
import com.appian.connectedsystems.templateframework.sdk.IntegrationError;
import com.appian.connectedsystems.templateframework.sdk.IntegrationResponse;
import com.appian.connectedsystems.templateframework.sdk.TemplateId;
import com.appian.connectedsystems.templateframework.sdk.configuration.PropertyPath;
import com.appian.connectedsystems.templateframework.sdk.diagnostics.IntegrationDesignerDiagnostic;
import com.example.appian.mqtt.core.CentralConnectionManager;
import com.example.appian.mqtt.core.SocketHolder;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MQTT Publish Integration Template for Appian Designer.
 * <p>
 * Allows low-code developers to publish messages to MQTT topics
 * via point-and-click configuration in the Integration Designer.
 * <p>
 * Uses the Connected System configuration for broker connection parameters
 * and exposes topic, payload, QoS, and retained flag as integration properties.
 */
@TemplateId(name = "MqttPublishIntegrationTemplate")
public class MqttPublishIntegrationTemplate extends SimpleIntegrationTemplate {

    private static final Logger LOG = Logger.getLogger(MqttPublishIntegrationTemplate.class.getName());

    public static final String PROP_TOPIC = "topic";
    public static final String PROP_PAYLOAD = "payload";
    public static final String PROP_QOS = "qos";
    public static final String PROP_RETAINED = "retained";

    private static final int DEFAULT_CONNECTION_TIMEOUT = 10;

    @Override
    protected SimpleConfiguration getConfiguration(
            SimpleConfiguration integrationConfiguration,
            SimpleConfiguration connectedSystemConfiguration,
            PropertyPath propertyPath,
            ExecutionContext executionContext) {

        return integrationConfiguration.setProperties(
                textProperty(PROP_TOPIC)
                        .label("MQTT Topic")
                        .isRequired(true)
                        .build(),
                textProperty(PROP_PAYLOAD)
                        .label("Payload (JSON/Text)")
                        .isRequired(true)
                        .build(),
                integerProperty(PROP_QOS)
                        .label("QoS Level (0, 1, or 2)")
                        .isRequired(true)
                        .build(),
                booleanProperty(PROP_RETAINED)
                        .label("Retain Message")
                        .isRequired(false)
                        .build()
        );
    }

    @Override
    protected IntegrationResponse execute(
            SimpleConfiguration integrationConfiguration,
            SimpleConfiguration connectedSystemConfiguration,
            ExecutionContext executionContext) {

        // Read Connected System properties
        String brokerUrl = connectedSystemConfiguration.getValue(MqttConnectedSystemTemplate.PROP_BROKER_URL);
        String clientId = connectedSystemConfiguration.getValue(MqttConnectedSystemTemplate.PROP_CLIENT_ID);
        String username = connectedSystemConfiguration.getValue(MqttConnectedSystemTemplate.PROP_USERNAME);
        String password = connectedSystemConfiguration.getValue(MqttConnectedSystemTemplate.PROP_PASSWORD);

        // Read Integration properties
        String topic = integrationConfiguration.getValue(PROP_TOPIC);
        String payload = integrationConfiguration.getValue(PROP_PAYLOAD);
        Integer qos = integrationConfiguration.getValue(PROP_QOS);
        Boolean retained = integrationConfiguration.getValue(PROP_RETAINED);

        // Apply defaults
        int qosLevel = (qos != null) ? qos : 0;
        boolean retainFlag = (retained != null) ? retained : false;

        Map<String, Object> diagnosticOutput = new HashMap<>();
        diagnosticOutput.put("topic", topic);
        diagnosticOutput.put("qos", qosLevel);
        diagnosticOutput.put("retained", retainFlag);

        try {
            // Get or create connection via singleton manager
            SocketHolder socketHolder = CentralConnectionManager.getInstance().getOrConnect(
                    brokerUrl, clientId, username, password,
                    DEFAULT_CONNECTION_TIMEOUT, true);

            // Build and publish MQTT message
            MqttMessage message = new MqttMessage(
                    payload != null ? payload.getBytes(StandardCharsets.UTF_8) : new byte[0]);
            message.setQos(qosLevel);
            message.setRetained(retainFlag);

            socketHolder.getClient().publish(topic, message).waitForCompletion(10_000L);

            diagnosticOutput.put("status", "SUCCESS");
            diagnosticOutput.put("publishedTopic", topic);
            diagnosticOutput.put("payloadSize", payload != null ? payload.length() : 0);

            LOG.info("MQTT publish successful: topic=" + topic + ", qos=" + qosLevel);
            return IntegrationResponse.forSuccess(diagnosticOutput).build();

        } catch (Exception e) {
            LOG.log(Level.SEVERE, "MQTT publish failed: topic=" + topic, e);
            diagnosticOutput.put("status", "ERROR");
            diagnosticOutput.put("errorMessage", e.getMessage());

            IntegrationError error = IntegrationError.builder()
                    .title("MQTT Publish Failed")
                    .message(e.getMessage())
                    .build();
            return IntegrationResponse.forError(error).build();
        }
    }
}
