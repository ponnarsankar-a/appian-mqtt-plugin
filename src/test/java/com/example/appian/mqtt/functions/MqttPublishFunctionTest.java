package com.example.appian.mqtt.functions;

import com.example.appian.mqtt.core.CentralConnectionManager;
import com.example.appian.mqtt.core.SocketHolder;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MqttPublishFunctionTest {

    private static final String BROKER_URL = "tcp://func-test:1883";
    private static final String CLIENT_ID = "funcClient";

    @Mock
    private MqttAsyncClient mockClient;

    @Mock
    private IMqttDeliveryToken mockDeliveryToken;

    private MqttPublishFunction function;

    @BeforeEach
    void setUp() {
        function = new MqttPublishFunction();
    }

    @AfterEach
    void tearDown() {
        CentralConnectionManager.getInstance().getConnectionRegistry().clear();
    }

    private void injectMockConnection() {
        String key = CentralConnectionManager.buildConnectionKey(BROKER_URL, CLIENT_ID);
        when(mockClient.isConnected()).thenReturn(true);
        SocketHolder holder = new SocketHolder(key, BROKER_URL, CLIENT_ID, mockClient);
        CentralConnectionManager.getInstance().getConnectionRegistry().put(key, holder);
    }

    @Test
    void mqttPublish_returnsSuccessJson() throws Exception {
        injectMockConnection();
        when(mockClient.publish(anyString(), any(MqttMessage.class))).thenReturn(mockDeliveryToken);

        String result = function.mqttPublish(BROKER_URL, CLIENT_ID, "test/topic", "hello", 1L);

        assertTrue(result.contains("\"status\":\"SUCCESS\""));
        assertTrue(result.contains("\"topic\":\"test/topic\""));
        assertTrue(result.contains("\"messageId\":\""));
    }

    @Test
    void mqttPublish_returnsErrorJson_onConnectionFailure() {
        // No mock connection — will fail to connect to nonexistent broker
        String result = function.mqttPublish("tcp://nonexistent:1883", "failClient",
                "fail/topic", "fail", 0L);

        assertTrue(result.contains("\"status\":\"ERROR\""));
        assertTrue(result.contains("\"errorMessage\":\""));
    }

    @Test
    void mqttPublish_returnsErrorJson_onInvalidQos() {
        String result = function.mqttPublish(BROKER_URL, CLIENT_ID, "test/topic", "hello", 5L);

        assertTrue(result.contains("\"status\":\"ERROR\""));
        assertTrue(result.contains("Invalid QoS level: 5"));
    }

    @Test
    void mqttPublish_returnsErrorJson_onNegativeQos() {
        String result = function.mqttPublish(BROKER_URL, CLIENT_ID, "test/topic", "hello", -1L);

        assertTrue(result.contains("\"status\":\"ERROR\""));
        assertTrue(result.contains("Invalid QoS level: -1"));
    }

    @Test
    void mqttPublish_defaultsQosToZero_whenNull() throws Exception {
        injectMockConnection();
        when(mockClient.publish(anyString(), any(MqttMessage.class))).thenReturn(mockDeliveryToken);

        String result = function.mqttPublish(BROKER_URL, CLIENT_ID, "test/topic", "hello", null);

        assertTrue(result.contains("\"status\":\"SUCCESS\""));

        var msgCaptor = org.mockito.ArgumentCaptor.forClass(MqttMessage.class);
        verify(mockClient).publish(eq("test/topic"), msgCaptor.capture());
        assertEquals(0, msgCaptor.getValue().getQos());
    }

    @Test
    void mqttPublish_handlesNullPayload() throws Exception {
        injectMockConnection();
        when(mockClient.publish(anyString(), any(MqttMessage.class))).thenReturn(mockDeliveryToken);

        String result = function.mqttPublish(BROKER_URL, CLIENT_ID, "test/topic", null, 0L);

        assertTrue(result.contains("\"status\":\"SUCCESS\""));

        var msgCaptor = org.mockito.ArgumentCaptor.forClass(MqttMessage.class);
        verify(mockClient).publish(eq("test/topic"), msgCaptor.capture());
        assertEquals(0, msgCaptor.getValue().getPayload().length);
    }

    @Test
    void mqttPublish_neverThrows() {
        // Even with completely invalid inputs, should return error JSON, not throw
        assertDoesNotThrow(() -> function.mqttPublish(null, null, null, null, null));
    }

    @Test
    void functionAnnotation_isPresent() {
        var methods = MqttPublishFunction.class.getDeclaredMethods();
        boolean found = false;
        for (var method : methods) {
            if (method.getName().equals("mqttPublish") &&
                    method.isAnnotationPresent(com.appiancorp.suiteapi.expression.annotations.Function.class)) {
                found = true;
                break;
            }
        }
        assertTrue(found, "@Function annotation should be present on mqttPublish method");
    }

    @Test
    void categoryAnnotation_isPresent() {
        var annotation = MqttPublishFunction.class.getAnnotation(
                com.appiancorp.suiteapi.expression.annotations.Category.class);
        assertNotNull(annotation);
        assertEquals("mqttFunctions", annotation.value());
    }
}
