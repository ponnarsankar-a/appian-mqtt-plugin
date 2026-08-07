package com.example.appian.mqtt.templates;

import com.appian.connectedsystems.simplified.sdk.configuration.SimpleConfiguration;
import com.appian.connectedsystems.templateframework.sdk.ExecutionContext;
import com.appian.connectedsystems.templateframework.sdk.configuration.PropertyDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class MqttConnectedSystemTemplateTest {

    @Mock
    private ExecutionContext executionContext;

    private MqttConnectedSystemTemplate template;

    @BeforeEach
    void setUp() {
        template = new MqttConnectedSystemTemplate();
    }

    @Test
    void getConfiguration_registersAllExpectedProperties() {
        SimpleConfiguration config = new SimpleConfiguration();

        SimpleConfiguration result = template.getConfiguration(config, executionContext);

        Map<String, PropertyDescriptor> props = result.getProperties();
        assertEquals(6, props.size());
        assertTrue(props.containsKey(MqttConnectedSystemTemplate.PROP_BROKER_URL));
        assertTrue(props.containsKey(MqttConnectedSystemTemplate.PROP_CLIENT_ID));
        assertTrue(props.containsKey(MqttConnectedSystemTemplate.PROP_USERNAME));
        assertTrue(props.containsKey(MqttConnectedSystemTemplate.PROP_PASSWORD));
        assertTrue(props.containsKey(MqttConnectedSystemTemplate.PROP_KEEP_ALIVE));
        assertTrue(props.containsKey(MqttConnectedSystemTemplate.PROP_CLEAN_SESSION));
    }

    @Test
    void getConfiguration_brokerUrl_isRequired() {
        SimpleConfiguration config = new SimpleConfiguration();
        template.getConfiguration(config, executionContext);

        PropertyDescriptor brokerUrl = config.getProperties().get(MqttConnectedSystemTemplate.PROP_BROKER_URL);
        assertNotNull(brokerUrl);
        assertTrue(brokerUrl.isRequired());
        assertEquals(PropertyDescriptor.PropertyType.TEXT, brokerUrl.getType());
    }

    @Test
    void getConfiguration_clientId_isRequired() {
        SimpleConfiguration config = new SimpleConfiguration();
        template.getConfiguration(config, executionContext);

        PropertyDescriptor clientId = config.getProperties().get(MqttConnectedSystemTemplate.PROP_CLIENT_ID);
        assertNotNull(clientId);
        assertTrue(clientId.isRequired());
        assertEquals(PropertyDescriptor.PropertyType.TEXT, clientId.getType());
    }

    @Test
    void getConfiguration_username_isOptional() {
        SimpleConfiguration config = new SimpleConfiguration();
        template.getConfiguration(config, executionContext);

        PropertyDescriptor username = config.getProperties().get(MqttConnectedSystemTemplate.PROP_USERNAME);
        assertNotNull(username);
        assertFalse(username.isRequired());
        assertEquals(PropertyDescriptor.PropertyType.TEXT, username.getType());
    }

    @Test
    void getConfiguration_password_isEncrypted() {
        SimpleConfiguration config = new SimpleConfiguration();
        template.getConfiguration(config, executionContext);

        PropertyDescriptor password = config.getProperties().get(MqttConnectedSystemTemplate.PROP_PASSWORD);
        assertNotNull(password);
        assertFalse(password.isRequired());
        assertTrue(password.isEncrypted());
        assertEquals(PropertyDescriptor.PropertyType.ENCRYPTED_TEXT, password.getType());
    }

    @Test
    void getConfiguration_keepAlive_hasDefaultValue() {
        SimpleConfiguration config = new SimpleConfiguration();
        template.getConfiguration(config, executionContext);

        PropertyDescriptor keepAlive = config.getProperties().get(MqttConnectedSystemTemplate.PROP_KEEP_ALIVE);
        assertNotNull(keepAlive);
        assertFalse(keepAlive.isRequired());
        assertEquals(PropertyDescriptor.PropertyType.INTEGER, keepAlive.getType());
        assertEquals(60, keepAlive.getDefaultValue());
    }

    @Test
    void getConfiguration_cleanSession_hasDefaultValue() {
        SimpleConfiguration config = new SimpleConfiguration();
        template.getConfiguration(config, executionContext);

        PropertyDescriptor cleanSession = config.getProperties().get(MqttConnectedSystemTemplate.PROP_CLEAN_SESSION);
        assertNotNull(cleanSession);
        assertFalse(cleanSession.isRequired());
        assertEquals(PropertyDescriptor.PropertyType.BOOLEAN, cleanSession.getType());
        assertEquals(true, cleanSession.getDefaultValue());
    }

    @Test
    void templateId_annotation_isPresent() {
        var annotation = MqttConnectedSystemTemplate.class.getAnnotation(
                com.appian.connectedsystems.template.annotations.TemplateId.class);
        assertNotNull(annotation);
        assertEquals("MqttConnectedSystemTemplate", annotation.name());
    }
}
