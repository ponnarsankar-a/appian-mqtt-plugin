package com.example.appian.mqtt.core;

import com.appiancorp.services.ServiceContext;
import com.appiancorp.suiteapi.process.ProcessExecutionService;
import com.appiancorp.suiteapi.process.ProcessVariable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AppianProcessLauncherTest {

    @Mock
    private ProcessExecutionService mockProcessService;

    private AppianProcessLauncher launcher;

    @BeforeEach
    void setUp() {
        launcher = new AppianProcessLauncher(mockProcessService, "mqtt-service-account");
    }

    @Test
    void triggerProcess_callsStartProcessWithCorrectParameters() throws Exception {
        when(mockProcessService.startProcess(any(ServiceContext.class), eq(100L), any(ProcessVariable[].class)))
                .thenReturn(999L);

        Map<String, Object> params = new HashMap<>();
        params.put("temperature", 85.5);
        params.put("sensorId", "sensor-001");

        Long result = launcher.triggerProcess(100L, params);

        assertEquals(999L, result);
        verify(mockProcessService).startProcess(any(ServiceContext.class), eq(100L), any(ProcessVariable[].class));
    }

    @Test
    void triggerProcess_passesServiceContextWithCorrectUsername() throws Exception {
        when(mockProcessService.startProcess(any(ServiceContext.class), anyLong(), any(ProcessVariable[].class)))
                .thenReturn(1L);

        launcher.triggerProcess(1L, Map.of("key", "value"));

        ArgumentCaptor<ServiceContext> ctxCaptor = ArgumentCaptor.forClass(ServiceContext.class);
        verify(mockProcessService).startProcess(ctxCaptor.capture(), anyLong(), any(ProcessVariable[].class));

        assertEquals("mqtt-service-account", ctxCaptor.getValue().getUsername());
    }

    @Test
    void triggerProcess_returnsNullOnException() throws Exception {
        when(mockProcessService.startProcess(any(ServiceContext.class), anyLong(), any(ProcessVariable[].class)))
                .thenThrow(new RuntimeException("Process engine unavailable"));

        Long result = launcher.triggerProcess(100L, Map.of("key", "value"));

        assertNull(result);
    }

    @Test
    void triggerProcess_returnsNullOnError() throws Exception {
        when(mockProcessService.startProcess(any(ServiceContext.class), anyLong(), any(ProcessVariable[].class)))
                .thenThrow(new OutOfMemoryError("simulated OOM"));

        Long result = launcher.triggerProcess(100L, Map.of("key", "value"));

        assertNull(result);
    }

    @Test
    void triggerProcess_handlesNullParameters() throws Exception {
        when(mockProcessService.startProcess(any(ServiceContext.class), eq(50L), any(ProcessVariable[].class)))
                .thenReturn(42L);

        Long result = launcher.triggerProcess(50L, null);

        assertEquals(42L, result);

        ArgumentCaptor<ProcessVariable[]> varsCaptor = ArgumentCaptor.forClass(ProcessVariable[].class);
        verify(mockProcessService).startProcess(any(), eq(50L), varsCaptor.capture());
        assertEquals(0, varsCaptor.getValue().length);
    }

    @Test
    void triggerProcess_handlesEmptyParameters() throws Exception {
        when(mockProcessService.startProcess(any(ServiceContext.class), eq(50L), any(ProcessVariable[].class)))
                .thenReturn(43L);

        Long result = launcher.triggerProcess(50L, Map.of());

        assertEquals(43L, result);

        ArgumentCaptor<ProcessVariable[]> varsCaptor = ArgumentCaptor.forClass(ProcessVariable[].class);
        verify(mockProcessService).startProcess(any(), eq(50L), varsCaptor.capture());
        assertEquals(0, varsCaptor.getValue().length);
    }

    // --- buildVariables tests ---

    @Test
    void buildVariables_mapsStringValue() {
        Map<String, Object> params = Map.of("name", "sensor-alpha");

        ProcessVariable[] vars = launcher.buildVariables(params);

        assertEquals(1, vars.length);
        assertEquals("name", vars[0].getName());
        assertEquals("sensor-alpha", vars[0].getValue().getValue());
    }

    @Test
    void buildVariables_mapsLongValue() {
        Map<String, Object> params = Map.of("count", 42L);

        ProcessVariable[] vars = launcher.buildVariables(params);

        assertEquals(1, vars.length);
        assertEquals("count", vars[0].getName());
        assertEquals(42L, vars[0].getValue().getValue());
    }

    @Test
    void buildVariables_mapsDoubleValue() {
        Map<String, Object> params = Map.of("temperature", 98.6);

        ProcessVariable[] vars = launcher.buildVariables(params);

        assertEquals(1, vars.length);
        assertEquals("temperature", vars[0].getName());
        assertEquals(98.6, vars[0].getValue().getValue());
    }

    @Test
    void buildVariables_mapsBooleanValue() {
        Map<String, Object> params = Map.of("isAlert", true);

        ProcessVariable[] vars = launcher.buildVariables(params);

        assertEquals(1, vars.length);
        assertEquals("isAlert", vars[0].getName());
        assertEquals(true, vars[0].getValue().getValue());
    }

    @Test
    void buildVariables_mapsMultipleEntries() {
        Map<String, Object> params = new HashMap<>();
        params.put("field1", "value1");
        params.put("field2", 100L);
        params.put("field3", true);

        ProcessVariable[] vars = launcher.buildVariables(params);

        assertEquals(3, vars.length);
    }

    @Test
    void buildVariables_returnsEmptyArrayForNull() {
        ProcessVariable[] vars = launcher.buildVariables(null);
        assertEquals(0, vars.length);
    }

    @Test
    void buildVariables_returnsEmptyArrayForEmptyMap() {
        ProcessVariable[] vars = launcher.buildVariables(Map.of());
        assertEquals(0, vars.length);
    }
}
