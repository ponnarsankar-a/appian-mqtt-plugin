package com.example.appian.mqtt.templates;

import com.appian.connectedsystems.simplified.sdk.SimpleConnectedSystemTemplate;
import com.appian.connectedsystems.simplified.sdk.configuration.SimpleConfiguration;
import com.appian.connectedsystems.templateframework.sdk.ExecutionContext;
import com.appian.connectedsystems.templateframework.sdk.TemplateId;

/**
 * MQTT Connected System Template for Appian Designer.
 * <p>
 * Exposes broker connection configuration as a reusable Connected System
 * that can be shared across multiple Integration templates and Smart Services.
 * <p>
 * Designer properties:
 * <ul>
 *   <li>brokerUrl — MQTT broker URL (e.g. tcp://broker.example.com:1883)</li>
 *   <li>clientId — Unique MQTT client identifier</li>
 *   <li>username — Authentication username (optional)</li>
 *   <li>password — Authentication password (encrypted, optional)</li>
 *   <li>keepAlive — Keep-alive interval in seconds (default 60)</li>
 *   <li>cleanSession — Whether to use a clean MQTT session (default true)</li>
 * </ul>
 */
@TemplateId(name = "MqttConnectedSystemTemplate")
public class MqttConnectedSystemTemplate extends SimpleConnectedSystemTemplate {

    public static final String PROP_BROKER_URL = "brokerUrl";
    public static final String PROP_CLIENT_ID = "clientId";
    public static final String PROP_USERNAME = "username";
    public static final String PROP_PASSWORD = "password";
    public static final String PROP_KEEP_ALIVE = "keepAlive";
    public static final String PROP_CLEAN_SESSION = "cleanSession";

    @Override
    protected SimpleConfiguration getConfiguration(
            SimpleConfiguration configuration,
            ExecutionContext executionContext) {

        return configuration.setProperties(
                textProperty(PROP_BROKER_URL)
                        .label("Broker URL (e.g. tcp://broker.example.com:1883)")
                        .isRequired(true)
                        .build(),
                textProperty(PROP_CLIENT_ID)
                        .label("Client ID")
                        .isRequired(true)
                        .build(),
                textProperty(PROP_USERNAME)
                        .label("Username")
                        .isRequired(false)
                        .build(),
                encryptedTextProperty(PROP_PASSWORD)
                        .label("Password")
                        .isRequired(false)
                        .build(),
                integerProperty(PROP_KEEP_ALIVE)
                        .label("Keep Alive (seconds)")
                        .isRequired(false)
                        .build(),
                booleanProperty(PROP_CLEAN_SESSION)
                        .label("Clean Session")
                        .isRequired(false)
                        .build()
        );
    }
}
