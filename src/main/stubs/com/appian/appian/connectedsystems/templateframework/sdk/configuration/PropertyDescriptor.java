package com.appian.connectedsystems.templateframework.sdk.configuration;

/**
 * Stub for Appian Connected System SDK PropertyDescriptor.
 * The real class is provided by Appian at runtime.
 */
public class PropertyDescriptor {

    private final String key;
    private final String label;
    private final PropertyType type;
    private final boolean required;
    private final boolean encrypted;
    private final boolean multiline;
    private final Object defaultValue;

    private PropertyDescriptor(Builder builder) {
        this.key = builder.key;
        this.label = builder.label;
        this.type = builder.type;
        this.required = builder.required;
        this.encrypted = builder.encrypted;
        this.multiline = builder.multiline;
        this.defaultValue = builder.defaultValue;
    }

    public String getKey() { return key; }
    public String getLabel() { return label; }
    public PropertyType getType() { return type; }
    public boolean isRequired() { return required; }
    public boolean isEncrypted() { return encrypted; }
    public boolean isMultiline() { return multiline; }
    public Object getDefaultValue() { return defaultValue; }

    public enum PropertyType {
        TEXT, INTEGER, BOOLEAN, ENCRYPTED_TEXT
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String key;
        private String label;
        private PropertyType type = PropertyType.TEXT;
        private boolean required = false;
        private boolean encrypted = false;
        private boolean multiline = false;
        private Object defaultValue;

        public Builder key(String key) { this.key = key; return this; }
        public Builder label(String label) { this.label = label; return this; }
        public Builder type(PropertyType type) { this.type = type; return this; }
        public Builder isRequired(boolean required) { this.required = required; return this; }
        public Builder isEncrypted(boolean encrypted) { this.encrypted = encrypted; return this; }
        public Builder isMultiline(boolean multiline) { this.multiline = multiline; return this; }
        public Builder defaultValue(Object defaultValue) { this.defaultValue = defaultValue; return this; }

        public PropertyDescriptor build() {
            return new PropertyDescriptor(this);
        }
    }
}
