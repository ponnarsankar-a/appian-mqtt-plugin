package com.example.appian.mqtt.core;

import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SocketHolderTest {

    private static final String CONNECTION_KEY = "tcp://broker.test:1883::testClient";
    private static final String BROKER_URL = "tcp://broker.test:1883";
    private static final String CLIENT_ID = "testClient";

    @Mock
    private MqttAsyncClient mockClient;

    private SocketHolder holder;

    @BeforeEach
    void setUp() {
        holder = new SocketHolder(CONNECTION_KEY, BROKER_URL, CLIENT_ID, mockClient);
    }

    @Test
    void constructor_setsFieldsCorrectly() {
        assertEquals(CONNECTION_KEY, holder.getConnectionKey());
        assertEquals(BROKER_URL, holder.getBrokerUrl());
        assertEquals(CLIENT_ID, holder.getClientId());
    }

    @Test
    void constructor_initializesTimestampToCurrentTime() {
        long before = System.currentTimeMillis();
        SocketHolder newHolder = new SocketHolder(CONNECTION_KEY, BROKER_URL, CLIENT_ID, mockClient);
        long after = System.currentTimeMillis();

        assertTrue(newHolder.getLastAccessedTimestamp() >= before);
        assertTrue(newHolder.getLastAccessedTimestamp() <= after);
    }

    @Test
    void touch_updatesLastAccessedTimestamp() throws InterruptedException {
        long initialTimestamp = holder.getLastAccessedTimestamp();
        Thread.sleep(10); // Small delay to ensure timestamp changes
        holder.touch();

        assertTrue(holder.getLastAccessedTimestamp() > initialTimestamp);
    }

    @Test
    void getClient_touchesTimestamp() throws InterruptedException {
        long initialTimestamp = holder.getLastAccessedTimestamp();
        Thread.sleep(10);
        MqttAsyncClient client = holder.getClient();

        assertSame(mockClient, client);
        assertTrue(holder.getLastAccessedTimestamp() > initialTimestamp);
    }

    @Test
    void isConnected_delegatesToClient_whenConnected() {
        when(mockClient.isConnected()).thenReturn(true);
        assertTrue(holder.isConnected());
    }

    @Test
    void isConnected_delegatesToClient_whenDisconnected() {
        when(mockClient.isConnected()).thenReturn(false);
        assertFalse(holder.isConnected());
    }

    @Test
    void close_disconnectsAndClosesClient_whenConnected() throws Exception {
        when(mockClient.isConnected()).thenReturn(true);

        holder.close();

        verify(mockClient).disconnectForcibly(1000, 2000);
        verify(mockClient).close();
    }

    @Test
    void close_onlyClosesClient_whenNotConnected() throws Exception {
        when(mockClient.isConnected()).thenReturn(false);

        holder.close();

        verify(mockClient, never()).disconnectForcibly(anyLong(), anyLong());
        verify(mockClient).close();
    }

    @Test
    void close_handlesExceptionGracefully() throws Exception {
        when(mockClient.isConnected()).thenReturn(true);
        doThrow(new RuntimeException("disconnect error")).when(mockClient).disconnectForcibly(anyLong(), anyLong());

        // Should not throw
        assertDoesNotThrow(() -> holder.close());
    }
}
