package com.example.appian.mqtt.smartservices;

import com.appiancorp.suiteapi.process.ProcessExecutionService;
import com.example.appian.mqtt.core.CentralConnectionManager;
import com.example.appian.mqtt.core.SocketHolder;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class MqttSubscribeSmartServiceTest {

    private static final String BROKER_URL = "tcp://sub-test:1883";
    private static final String CLIENT_ID = "subClient";

    @Mock
    private MqttAsyncClient mockClient;

    @Mock
    private IMqttToken mockToken;

    @Mock
    private ProcessExecutionService mockProcessService;

    private MqttSubscribeSmartService service;

    @BeforeEach
    void setUp() {
        service = new MqttSubscribeSmartService();
        MqttSubscribeSmartService.getListenerRegistry().clear();
    }

    @AfterEach
    void tearDown() {
        // Stop any active listeners
        MqttSubscribeSmartService.getListenerRegistry().forEach((id, handle) -> handle.stop());
        MqttSubscribeSmartService.getListenerRegistry().clear();
        CentralConnectionManager.getInstance().getConnectionRegistry().clear();
    }

    private void injectMockConnection() throws Exception {
        String key = CentralConnectionManager.buildConnectionKey(BROKER_URL, CLIENT_ID);
        when(mockClient.isConnected()).thenReturn(true);
        when(mockClient.subscribe(anyString(), anyInt(), any(IMqttMessageListener.class))).thenReturn(mockToken);
        when(mockClient.unsubscribe(anyString())).thenReturn(mockToken);
        SocketHolder holder = new SocketHolder(key, BROKER_URL, CLIENT_ID, mockClient);
        CentralConnectionManager.getInstance().getConnectionRegistry().put(key, holder);
    }

    // --- ONE_SHOT mode tests ---

    @Test
    void oneShot_collectsMessagesAndReturnsJson() throws Exception {
        injectMockConnection();

        // Capture the listener so we can simulate inbound messages
        ArgumentCaptor<IMqttMessageListener> listenerCaptor = ArgumentCaptor.forClass(IMqttMessageListener.class);
        when(mockClient.subscribe(eq("test/topic"), eq(0), listenerCaptor.capture())).thenReturn(mockToken);

        service.setBrokerUrl(BROKER_URL);
        service.setClientId(CLIENT_ID);
        service.setTopic("test/topic");
        service.setQos(0);
        service.setMode("ONE_SHOT");
        service.setMaxMessages(2);
        service.setTimeoutMs(2000L);

        // Run in a separate thread since we need to inject messages
        Thread runner = new Thread(() -> {
            try { service.run(); } catch (Exception e) { throw new RuntimeException(e); }
        });
        runner.start();

        // Wait for subscription to be set up
        Thread.sleep(100);

        // Simulate inbound messages via the captured listener
        IMqttMessageListener capturedListener = listenerCaptor.getValue();
        MqttMessage msg1 = new MqttMessage("payload1".getBytes(StandardCharsets.UTF_8));
        msg1.setQos(0);
        capturedListener.messageArrived("test/topic", msg1);

        MqttMessage msg2 = new MqttMessage("payload2".getBytes(StandardCharsets.UTF_8));
        msg2.setQos(1);
        capturedListener.messageArrived("test/topic", msg2);

        runner.join(3000);

        assertTrue(service.isSuccess());
        assertNull(service.getErrorMessage());
        assertNotNull(service.getCollectedMessages());
        assertTrue(service.getCollectedMessages().contains("payload1"));
        assertTrue(service.getCollectedMessages().contains("payload2"));
    }

    @Test
    void oneShot_returnsPartialResultsOnTimeout() throws Exception {
        injectMockConnection();

        service.setBrokerUrl(BROKER_URL);
        service.setClientId(CLIENT_ID);
        service.setTopic("timeout/topic");
        service.setQos(0);
        service.setMode("ONE_SHOT");
        service.setMaxMessages(100); // Asking for many
        service.setTimeoutMs(200L); // Very short timeout

        service.run();

        // Should succeed even with 0 messages (timeout reached)
        assertTrue(service.isSuccess());
        assertNotNull(service.getCollectedMessages());
        assertEquals("[]", service.getCollectedMessages()); // Empty array
    }

    @Test
    void oneShot_usesDefaultsWhenOptionalInputsNull() throws Exception {
        injectMockConnection();

        service.setBrokerUrl(BROKER_URL);
        service.setClientId(CLIENT_ID);
        service.setTopic("default/topic");
        service.setMode("ONE_SHOT");
        // maxMessages and timeoutMs left null — should use defaults (10 and 5000ms)

        // Run with short thread to avoid blocking on the 5s default timeout
        Thread runner = new Thread(() -> {
            try { service.run(); } catch (Exception e) { throw new RuntimeException(e); }
        });
        runner.start();
        runner.join(6000); // Wait slightly longer than default timeout

        assertTrue(service.isSuccess());
    }

    // --- PERSISTENT mode tests ---

    @Test
    void persistent_registersListenerInRegistry() throws Exception {
        injectMockConnection();
        when(mockProcessService.startProcess(any(), anyLong(), any())).thenReturn(1L);

        service.setBrokerUrl(BROKER_URL);
        service.setClientId(CLIENT_ID);
        service.setTopic("persistent/topic");
        service.setQos(1);
        service.setMode("PERSISTENT");
        service.setProcessModelId(100L);
        service.setServiceAccountUsername("mqtt-svc");
        service.setMaxMessagesPerSecond(50);
        service.setQueueCapacity(500);
        service.setProcessExecutionService(mockProcessService);

        service.run();

        assertTrue(service.isSuccess());
        assertNotNull(service.getListenerId());
        assertEquals(1, MqttSubscribeSmartService.getActiveListenerCount());
        assertTrue(MqttSubscribeSmartService.getListenerRegistry().containsKey(service.getListenerId()));
    }

    @Test
    void persistent_failsWithoutProcessModelId() throws Exception {
        injectMockConnection();

        service.setBrokerUrl(BROKER_URL);
        service.setClientId(CLIENT_ID);
        service.setTopic("persistent/topic");
        service.setMode("PERSISTENT");
        service.setProcessModelId(null); // Missing!
        service.setServiceAccountUsername("mqtt-svc");
        service.setProcessExecutionService(mockProcessService);

        service.run();

        assertFalse(service.isSuccess());
        assertNotNull(service.getErrorMessage());
        assertTrue(service.getErrorMessage().contains("processModelId"));
    }

    @Test
    void persistent_failsWithoutServiceAccountUsername() throws Exception {
        injectMockConnection();

        service.setBrokerUrl(BROKER_URL);
        service.setClientId(CLIENT_ID);
        service.setTopic("persistent/topic");
        service.setMode("PERSISTENT");
        service.setProcessModelId(100L);
        service.setServiceAccountUsername(null); // Missing!
        service.setProcessExecutionService(mockProcessService);

        service.run();

        assertFalse(service.isSuccess());
        assertNotNull(service.getErrorMessage());
        assertTrue(service.getErrorMessage().contains("serviceAccountUsername"));
    }

    // --- stopListener tests ---

    @Test
    void stopListener_stopsAndRemovesHandle() throws Exception {
        injectMockConnection();
        when(mockProcessService.startProcess(any(), anyLong(), any())).thenReturn(1L);

        service.setBrokerUrl(BROKER_URL);
        service.setClientId(CLIENT_ID);
        service.setTopic("stop/topic");
        service.setMode("PERSISTENT");
        service.setProcessModelId(100L);
        service.setServiceAccountUsername("mqtt-svc");
        service.setProcessExecutionService(mockProcessService);

        service.run();

        String id = service.getListenerId();
        assertNotNull(id);
        assertEquals(1, MqttSubscribeSmartService.getActiveListenerCount());

        // Stop the listener
        boolean stopped = MqttSubscribeSmartService.stopListener(id);

        assertTrue(stopped);
        assertEquals(0, MqttSubscribeSmartService.getActiveListenerCount());
    }

    @Test
    void stopListener_returnsFalseForNonExistentId() {
        boolean stopped = MqttSubscribeSmartService.stopListener("nonexistent-id");
        assertFalse(stopped);
    }

    @Test
    void stopListener_isIdempotent() throws Exception {
        injectMockConnection();
        when(mockProcessService.startProcess(any(), anyLong(), any())).thenReturn(1L);

        service.setBrokerUrl(BROKER_URL);
        service.setClientId(CLIENT_ID);
        service.setTopic("idempotent/topic");
        service.setMode("PERSISTENT");
        service.setProcessModelId(100L);
        service.setServiceAccountUsername("mqtt-svc");
        service.setProcessExecutionService(mockProcessService);

        service.run();

        String id = service.getListenerId();
        MqttSubscribeSmartService.ListenerHandle handle = MqttSubscribeSmartService.getListenerRegistry().get(id);

        // Stop via handle directly (twice)
        handle.stop();
        assertDoesNotThrow(() -> handle.stop()); // Second call should not throw
        assertTrue(handle.isStopped());
    }

    // --- Connection failure ---

    @Test
    void run_handlesConnectionFailureGracefully() throws Exception {
        // No mock connection — will fail
        service.setBrokerUrl("tcp://nonexistent:1883");
        service.setClientId("failClient");
        service.setTopic("fail/topic");
        service.setMode("ONE_SHOT");
        service.setTimeoutMs(100L);

        assertDoesNotThrow(() -> service.run());

        assertFalse(service.isSuccess());
        assertNotNull(service.getErrorMessage());
    }

    // --- Annotation checks ---

    @Test
    void paletteInfo_annotation_isPresent() {
        var annotation = MqttSubscribeSmartService.class.getAnnotation(
                com.appiancorp.suiteapi.process.palette.PaletteInfo.class);
        assertNotNull(annotation);
        assertEquals("Integration Services", annotation.paletteCategory());
        assertEquals("MQTT", annotation.palette());
    }
}
