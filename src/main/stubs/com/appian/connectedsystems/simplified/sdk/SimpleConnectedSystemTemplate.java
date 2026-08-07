package com.appian.connectedsystems.simplified.sdk;

import com.appian.connectedsystems.simplified.sdk.configuration.SimpleConfiguration;
import com.appian.connectedsystems.templateframework.sdk.ExecutionContext;
import com.appian.connectedsystems.templateframework.sdk.configuration.PropertyDescriptor;

/**
 * Stub for Appian Connected System SDK SimpleConnectedSystemTemplate.
 * The real class is provided by Appian at runtime.
 */
public abstract class SimpleConnectedSystemTemplate {

    protected abstract SimpleConfiguration getConfiguration(
            SimpleConfiguration configuration,
            ExecutionContext executionContext);

    // --- Property builder helper methods (mirrors real SDK fluent API) ---

    protected static TextPropertyBuilder textProperty(String key) {
        return new TextPropertyBuilder(key);
    }

    protected static IntegerPropertyBuilder integerProperty(String key) {
        return new IntegerPropertyBuilder(key);
    }

    protected static BooleanPropertyBuilder booleanProperty(String key) {
        return new BooleanPropertyBuilder(key);
    }

    protected static EncryptedTextPropertyBuilder encryptedTextProperty(String key) {
        return new EncryptedTextPropertyBuilder(key);
    }

    // --- Builder classes ---

    public static class TextPropertyBuilder {
        private final String key;
        private String label;
        private boolean required;
        private boolean multiline;

        TextPropertyBuilder(String key) { this.key = key; }
        public TextPropertyBuilder label(String label) { this.label = label; return this; }
        public TextPropertyBuilder isRequired(boolean required) { this.required = required; return this; }
        public TextPropertyBuilder isMultiline(boolean multiline) { this.multiline = multiline; return this; }

        public PropertyDescriptor build() {
            return PropertyDescriptor.builder()
                    .key(key).label(label).type(PropertyDescriptor.PropertyType.TEXT)
                    .isRequired(required).isMultiline(multiline).build();
        }
    }

    public static class IntegerPropertyBuilder {
        private final String key;
        private String label;
        private boolean required;
        private Object defaultValue;

        IntegerPropertyBuilder(String key) { this.key = key; }
        public IntegerPropertyBuilder label(String label) { this.label = label; return this; }
        public IntegerPropertyBuilder isRequired(boolean required) { this.required = required; return this; }
        public IntegerPropertyBuilder defaultValue(Object defaultValue) { this.defaultValue = defaultValue; return this; }

        public PropertyDescriptor build() {
            return PropertyDescriptor.builder()
                    .key(key).label(label).type(PropertyDescriptor.PropertyType.INTEGER)
                    .isRequired(required).defaultValue(defaultValue).build();
        }
    }

    public static class BooleanPropertyBuilder {
        private final String key;
        private String label;
        private boolean required;
        private Object defaultValue;

        BooleanPropertyBuilder(String key) { this.key = key; }
        public BooleanPropertyBuilder label(String label) { this.label = label; return this; }
        public BooleanPropertyBuilder isRequired(boolean required) { this.required = required; return this; }
        public BooleanPropertyBuilder defaultValue(Object defaultValue) { this.defaultValue = defaultValue; return this; }

        public PropertyDescriptor build() {
            return PropertyDescriptor.builder()
                    .key(key).label(label).type(PropertyDescriptor.PropertyType.BOOLEAN)
                    .isRequired(required).defaultValue(defaultValue).build();
        }
    }

    public static class EncryptedTextPropertyBuilder {
        private final String key;
        private String label;
        private boolean required;

        EncryptedTextPropertyBuilder(String key) { this.key = key; }
        public EncryptedTextPropertyBuilder label(String label) { this.label = label; return this; }
        public EncryptedTextPropertyBuilder isRequired(boolean required) { this.required = required; return this; }

        public PropertyDescriptor build() {
            return PropertyDescriptor.builder()
                    .key(key).label(label).type(PropertyDescriptor.PropertyType.ENCRYPTED_TEXT)
                    .isRequired(required).isEncrypted(true).build();
        }
    }
}
