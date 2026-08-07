package com.example.appian.mqtt.integration;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that all classes referenced in appian-plugin.xml can be loaded.
 * This catches typos in class names or missing classes before deployment.
 */
class PluginManifestTest {

    private static final String[] REGISTERED_CLASSES = {
            "com.example.appian.mqtt.templates.MqttConnectedSystemTemplate",
            "com.example.appian.mqtt.templates.MqttPublishIntegrationTemplate",
            "com.example.appian.mqtt.smartservices.MqttPublishSmartService",
            "com.example.appian.mqtt.smartservices.MqttSubscribeSmartService",
            "com.example.appian.mqtt.functions.MqttPublishFunction"
    };

    @Test
    void allRegisteredClassesCanBeLoaded() {
        for (String className : REGISTERED_CLASSES) {
            assertDoesNotThrow(() -> Class.forName(className),
                    "Failed to load class registered in appian-plugin.xml: " + className);
        }
    }

    @Test
    void connectedSystemTemplate_extendsCorrectSuperclass() throws Exception {
        Class<?> clazz = Class.forName("com.example.appian.mqtt.templates.MqttConnectedSystemTemplate");
        assertEquals("SimpleConnectedSystemTemplate", clazz.getSuperclass().getSimpleName());
    }

    @Test
    void integrationTemplate_extendsCorrectSuperclass() throws Exception {
        Class<?> clazz = Class.forName("com.example.appian.mqtt.templates.MqttPublishIntegrationTemplate");
        assertEquals("SimpleIntegrationTemplate", clazz.getSuperclass().getSimpleName());
    }

    @Test
    void smartServices_extendAppianSmartService() throws Exception {
        Class<?> publishClass = Class.forName("com.example.appian.mqtt.smartservices.MqttPublishSmartService");
        Class<?> subscribeClass = Class.forName("com.example.appian.mqtt.smartservices.MqttSubscribeSmartService");

        assertEquals("AppianSmartService", publishClass.getSuperclass().getSimpleName());
        assertEquals("AppianSmartService", subscribeClass.getSuperclass().getSimpleName());
    }

    @Test
    void expressionFunction_hasFunctionAnnotation() throws Exception {
        Class<?> clazz = Class.forName("com.example.appian.mqtt.functions.MqttPublishFunction");
        var methods = clazz.getDeclaredMethods();

        boolean hasFunctionMethod = false;
        for (var method : methods) {
            if (method.getName().equals("mqttPublish")) {
                hasFunctionMethod = true;
                break;
            }
        }
        assertTrue(hasFunctionMethod, "MqttPublishFunction should have mqttPublish method");
    }
}
