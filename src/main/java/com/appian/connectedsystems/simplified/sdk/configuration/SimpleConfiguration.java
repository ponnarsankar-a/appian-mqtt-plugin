package com.appian.connectedsystems.simplified.sdk.configuration;

import com.appian.connectedsystems.templateframework.sdk.configuration.PropertyDescriptor;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Stub for Appian Connected System SDK SimpleConfiguration.
 * The real class is provided by Appian at runtime.
 */
public class SimpleConfiguration {

    private final Map<String, Object> values = new LinkedHashMap<>();
    private final Map<String, PropertyDescriptor> properties = new LinkedHashMap<>();

    public SimpleConfiguration setProperties(PropertyDescriptor... descriptors) {
        for (PropertyDescriptor d : descriptors) {
            properties.put(d.getKey(), d);
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    public <T> T getValue(String key) {
        return (T) values.get(key);
    }

    public void setValue(String key, Object value) {
        values.put(key, value);
    }

    public Map<String, PropertyDescriptor> getProperties() {
        return properties;
    }

    public boolean hasProperty(String key) {
        return properties.containsKey(key);
    }
}
