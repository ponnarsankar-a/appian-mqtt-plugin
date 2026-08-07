package com.example.appian.mqtt.templates;

import com.appian.connectedsystems.simplified.sdk.SimpleIntegrationTemplate.ExecutionResult;
import com.appian.connectedsystems.simplified.sdk.configuration.SimpleConfiguration;
import com.appian.connectedsystems.templateframework.sdk.ExecutionContext;
import com.appian.connectedsystems.templateframework.sdk.configuration.PropertyDescriptor;
import com.example.appian.mqtt.core.CentralConnectionManager;
import com.example.appian.mqtt.core.SocketHolder;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MqttPublishIntegrationTemplateTest {

    @Mock
    private ExecutionContext executionContext;

    @Mock
    private MqttAsyncClient mockClient;

    @Mock
    private IMqttDeliveryToken mockDeliveryToken;

    private MqttPublishIntegrationTemplate template;

    @BeforeEach
    void setUp() {
        template = new MqttPublishIntegrationTemplate();
    }

    @AfterEach
    void tearDown() {
        CentralConnectionManager.getInstance().getConnectionRegistry().clear();
    }

    @Test
    void getConfiguration_registersAllExpectedProperties() {
        SimpleConfiguration integrationConfig = new SimpleConfiguration();
        SimpleConfiguration connectedSystemConfig = new SimpleConfiguration();

        SimpleConfiguration result = template.getConfiguration(
                integrationConfig, connectedSystemConfig, executionContext);

        Map<String, PropertyDescriptor> props = result.getProperties();
        assertEquals(4, props.size());
        assertTrue(props.containsKey(MqttPublishIntegrationTemplate.PROP_TOPIC));
        assertTrue(props.containsKey(MqttPublishIntegrationTemplate.PROP_PAYLOAD));
        assertTrue(props.containsKey(MqttPublishIntegrationTemplate.PROP_QOS));
        assertTrue(props.containsKey(MqttPublishIntegrationTemplate.PROP_RETAINED));
    }

    @Test
    void getConfiguration_topic_isRequired() {
        SimpleConfiguration integrationConfig = new SimpleConfiguration();
        SimpleConfiguration connectedSystemConfig = new SimpleConfiguration();
        template.getConfiguration(integrationConfig, connectedSystemConfig, executionContext);

        PropertyDescriptor topic = integrationConfig.getProperties().get(MqttPublishIntegrationTemplate.PROP_TOPIC);
        assertTrue(topic.isRequired());
        assertEquals(PropertyDescriptor.PropertyType.TEXT, topic.getType());
    }

    @Test
    void getConfiguration_payload_isMultiline() {
        SimpleConfiguration integrationConfig = new SimpleConfiguration();
        SimpleConfiguration connectedSystemConfig = new SimpleConfiguration();
        template.getConfiguration(integrationConfig, connectedSystemConfig, executionContext);

        PropertyDescriptor payload = integrationConfig.getProperties().get(MqttPublishIntegrationTemplate.PROP_PAYLOAD);
        assertTrue(payload.isRequired());
        assertTrue(payload.isMultiline());
    }

    @Test
    void getConfiguration_qos_hasDefaultZero() {
        SimpleConfiguration integrationConfig = new SimpleConfiguration();
        SimpleConfiguration connectedSystemConfig = new SimpleConfiguration();
        template.getConfiguration(integrationConfig, connectedSystemConfig, executionContext);

        PropertyDescriptor qos = integrationConfig.getProperties().get(MqttPublishIntegrationTemplate.PROP_QOS);
        assertTrue(qos.isRequired());
        assertEquals(0, qos.getDefaultValue());
    }

    @Test
    void execute_publishesSuccessfully() throws Exception {
        // Set up a pre-connected mock in the connection manager
        String brokerUrl = "tcp://test-broker:1883";
        String clientId = "testClient";
        String key = CentralConnectionManager.buildConnectionKey(brokerUrl, clientId);

        when(mockClient.isConnected()).thenReturn(true);
        when(mockClient.publish(anyString(), any(MqttMessage.class))).thenReturn(mockDeliveryToken);

        SocketHolder holder = new SocketHolder(key, brokerUrl, clientId, mockClient);
        CentralConnectionManager.getInstance().getConnectionRegistry().put(key, holder);

        // Set up configurations
        SimpleConfiguration connSystemConfig = new SimpleConfiguration();
        connSystemConfig.setValue(MqttConnectedSystemTemplate.PROP_BROKER_URL, brokerUrl);
        connSystemConfig.setValue(MqttConnectedSystemTemplate.PROP_CLIENT_ID, clientId);
        connSystemConfig.setValue(MqttConnectedSystemTemplate.PROP_USERNAME, null);
        connSystemConfig.setValue(MqttConnectedSystemTemplate.PROP_PASSWORD, null);

        SimpleConfiguration integrationConfig = new SimpleConfiguration();
        integrationConfig.setValue(MqttPublishIntegrationTemplate.PROP_TOPIC, "sensor/temperature");
        integrationConfig.setValue(MqttPublishIntegrationTemplate.PROP_PAYLOAD, "{\"temp\": 25.5}");
        integrationConfig.setValue(MqttPublishIntegrationTemplate.PROP_QOS, 1);
        integrationConfig.setValue(MqttPublishIntegrationTemplate.PROP_RETAINED, false);

        // Execute
        ExecutionResult result = template.execute(integrationConfig, connSystemConfig, executionContext);

        // Verify
        assertTrue(result.isSuccess());
        assertEquals("SUCCESS", result.getResult().get("status"));
        assertEquals("sensor/temperature", result.getResult().get("publishedTopic"));

        // Verify publish was called with correct parameters
        ArgumentCaptor<MqttMessage> messageCaptor = ArgumentCaptor.forClass(MqttMessage.class);
        verify(mockClient).publish(eq("sensor/temperature"), messageCaptor.capture());

        MqttMessage capturedMessage = messageCaptor.getValue();
        assertEquals(1, capturedMessage.getQos());
        assertFalse(capturedMessage.isRetained());
        assertEquals("{\"temp\": 25.5}", new String(capturedMessage.getPayload()));
    }

    @Test
    void execute_returnsErrorOnConnectionFailure() {
        // Use a broker URL that has no pre-connected socket and will fail
        SimpleConfiguration connSystemConfig = new SimpleConfiguration();
        connSystemConfig.setValue(MqttConnectedSystemTemplate.PROP_BROKER_URL, "tcp://nonexistent:1883");
        connSystemConfig.setValue(MqttConnectedSystemTemplate.PROP_CLIENT_ID, "failClient");
        connSystemConfig.setValue(MqttConnectedSystemTemplate.PROP_USERNAME, null);
        connSystemConfig.setValue(MqttConnectedSystemTemplate.PROP_PASSWORD, null);

        SimpleConfiguration integrationConfig = new SimpleConfiguration();
        integrationConfig.setValue(MqttPublishIntegrationTemplate.PROP_TOPIC, "test/topic");
        integrationConfig.setValue(MqttPublishIntegrationTemplate.PROP_PAYLOAD, "hello");
        integrationConfig.setValue(MqttPublishIntegrationTemplate.PROP_QOS, 0);
        integrationConfig.setValue(MqttPublishIntegrationTemplate.PROP_RETAINED, false);

        // Execute — should fail since there's no real broker
        ExecutionResult result = template.execute(integrationConfig, connSystemConfig, executionContext);

        assertFalse(result.isSuccess());
        assertEquals("MQTT_PUBLISH_FAILED", result.getErrorCode());
        assertNotNull(result.getErrorMessage());
        assertEquals("ERROR", result.getResult().get("status"));
    }

    @Test
    void execute_handlesRetainedFlag() throws Exception {
        String brokerUrl = "tcp://retained-test:1883";
        String clientId = "retainedClient";
        String key = CentralConnectionManager.buildConnectionKey(brokerUrl, clientId);

        when(mockClient.isConnected()).thenReturn(true);
        when(mockClient.publish(anyString(), any(MqttMessage.class))).thenReturn(mockDeliveryToken);

        SocketHolder holder = new SocketHolder(key, brokerUrl, clientId, mockClient);
        CentralConnectionManager.getInstance().getConnectionRegistry().put(key, holder);

        SimpleConfiguration connSystemConfig = new SimpleConfiguration();
        connSystemConfig.setValue(MqttConnectedSystemTemplate.PROP_BROKER_URL, brokerUrl);
        connSystemConfig.setValue(MqttConnectedSystemTemplate.PROP_CLIENT_ID, clientId);
        connSystemConfig.setValue(MqttConnectedSystemTemplate.PROP_USERNAME, null);
        connSystemConfig.setValue(MqttConnectedSystemTemplate.PROP_PASSWORD, null);

        SimpleConfiguration integrationConfig = new SimpleConfiguration();
        integrationConfig.setValue(MqttPublishIntegrationTemplate.PROP_TOPIC, "retained/topic");
        integrationConfig.setValue(MqttPublishIntegrationTemplate.PROP_PAYLOAD, "retained msg");
        integrationConfig.setValue(MqttPublishIntegrationTemplate.PROP_QOS, 2);
        integrationConfig.setValue(MqttPublishIntegrationTemplate.PROP_RETAINED, true);

        ExecutionResult result = template.execute(integrationConfig, connSystemConfig, executionContext);

        assertTrue(result.isSuccess());

        ArgumentCaptor<MqttMessage> messageCaptor = ArgumentCaptor.forClass(MqttMessage.class);
        verify(mockClient).publish(eq("retained/topic"), messageCaptor.capture());

        MqttMessage capturedMessage = messageCaptor.getValue();
        assertEquals(2, capturedMessage.getQos());
        assertTrue(capturedMessage.isRetained());
    }

    @Test
    void templateId_annotation_isPresent() {
        var annotation = MqttPublishIntegrationTemplate.class.getAnnotation(
                com.appian.connectedsystems.template.annotations.TemplateId.class);
        assertNotNull(annotation);
        assertEquals("MqttPublishIntegrationTemplate", annotation.name());
    }
}
