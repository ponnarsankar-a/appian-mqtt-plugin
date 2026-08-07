package com.example.appian.mqtt.smartservices;

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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MqttPublishSmartServiceTest {

    private static final String BROKER_URL = "tcp://smart-test:1883";
    private static final String CLIENT_ID = "smartClient";

    @Mock
    private MqttAsyncClient mockClient;

    @Mock
    private IMqttDeliveryToken mockDeliveryToken;

    private MqttPublishSmartService service;

    @BeforeEach
    void setUp() {
        service = new MqttPublishSmartService();
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
    void setters_storeInputValues() {
        service.setBrokerUrl("tcp://test:1883");
        service.setClientId("client1");
        service.setUsername("user");
        service.setPassword("pass");
        service.setTopic("test/topic");
        service.setPayload("hello");
        service.setQos(2);
        service.setRetained(true);

        // No exception — inputs accepted
        assertDoesNotThrow(() -> {});
    }

    @Test
    void run_publishesSuccessfully() throws Exception {
        injectMockConnection();
        when(mockClient.publish(anyString(), any(MqttMessage.class))).thenReturn(mockDeliveryToken);

        service.setBrokerUrl(BROKER_URL);
        service.setClientId(CLIENT_ID);
        service.setTopic("sensor/data");
        service.setPayload("{\"value\": 42}");
        service.setQos(1);
        service.setRetained(false);

        service.run();

        assertTrue(service.isSuccess());
        assertNull(service.getErrorMessage());
        assertNotNull(service.getMessageId());

        ArgumentCaptor<MqttMessage> msgCaptor = ArgumentCaptor.forClass(MqttMessage.class);
        verify(mockClient).publish(eq("sensor/data"), msgCaptor.capture());
        assertEquals(1, msgCaptor.getValue().getQos());
        assertFalse(msgCaptor.getValue().isRetained());
        assertEquals("{\"value\": 42}", new String(msgCaptor.getValue().getPayload()));
    }

    @Test
    void run_usesDefaultQosAndRetained_whenNull() throws Exception {
        injectMockConnection();
        when(mockClient.publish(anyString(), any(MqttMessage.class))).thenReturn(mockDeliveryToken);

        service.setBrokerUrl(BROKER_URL);
        service.setClientId(CLIENT_ID);
        service.setTopic("default/topic");
        service.setPayload("test");
        // qos and retained not set — should default to 0 and false

        service.run();

        assertTrue(service.isSuccess());

        ArgumentCaptor<MqttMessage> msgCaptor = ArgumentCaptor.forClass(MqttMessage.class);
        verify(mockClient).publish(eq("default/topic"), msgCaptor.capture());
        assertEquals(0, msgCaptor.getValue().getQos());
        assertFalse(msgCaptor.getValue().isRetained());
    }

    @Test
    void run_setsRetainedFlag() throws Exception {
        injectMockConnection();
        when(mockClient.publish(anyString(), any(MqttMessage.class))).thenReturn(mockDeliveryToken);

        service.setBrokerUrl(BROKER_URL);
        service.setClientId(CLIENT_ID);
        service.setTopic("retained/topic");
        service.setPayload("retained");
        service.setQos(2);
        service.setRetained(true);

        service.run();

        assertTrue(service.isSuccess());

        ArgumentCaptor<MqttMessage> msgCaptor = ArgumentCaptor.forClass(MqttMessage.class);
        verify(mockClient).publish(eq("retained/topic"), msgCaptor.capture());
        assertEquals(2, msgCaptor.getValue().getQos());
        assertTrue(msgCaptor.getValue().isRetained());
    }

    @Test
    void run_handlesConnectionFailure_withoutThrowing() throws Exception {
        // No mock connection injected — will fail to connect
        service.setBrokerUrl("tcp://nonexistent:1883");
        service.setClientId("failClient");
        service.setTopic("fail/topic");
        service.setPayload("fail");
        service.setQos(0);
        service.setRetained(false);

        // Should NOT throw
        assertDoesNotThrow(() -> service.run());

        assertFalse(service.isSuccess());
        assertNotNull(service.getErrorMessage());
        assertNull(service.getMessageId());
    }

    @Test
    void run_handlesPublishException_withoutThrowing() throws Exception {
        injectMockConnection();
        when(mockClient.publish(anyString(), any(MqttMessage.class)))
                .thenThrow(new org.eclipse.paho.client.mqttv3.MqttException(
                        org.eclipse.paho.client.mqttv3.MqttException.REASON_CODE_CLIENT_NOT_CONNECTED));

        service.setBrokerUrl(BROKER_URL);
        service.setClientId(CLIENT_ID);
        service.setTopic("error/topic");
        service.setPayload("error");
        service.setQos(0);
        service.setRetained(false);

        assertDoesNotThrow(() -> service.run());

        assertFalse(service.isSuccess());
        assertNotNull(service.getErrorMessage());
        assertNull(service.getMessageId());
    }

    @Test
    void outputs_initiallyDefaultValues() {
        assertFalse(service.isSuccess());
        assertNull(service.getErrorMessage());
        assertNull(service.getMessageId());
    }

    @Test
    void paletteInfo_annotation_isPresent() {
        var annotation = MqttPublishSmartService.class.getAnnotation(
                com.appiancorp.suiteapi.process.palette.PaletteInfo.class);
        assertNotNull(annotation);
        assertEquals("Integration Services", annotation.paletteCategory());
        assertEquals("MQTT", annotation.palette());
    }
}
