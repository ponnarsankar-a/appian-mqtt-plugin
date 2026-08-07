package com.appian.connectedsystems.simplified.sdk;

import com.appian.connectedsystems.simplified.sdk.configuration.SimpleConfiguration;
import com.appian.connectedsystems.templateframework.sdk.ExecutionContext;
import com.appian.connectedsystems.templateframework.sdk.configuration.PropertyDescriptor;

import java.util.Map;

/**
 * Stub for Appian Connected System SDK SimpleIntegrationTemplate.
 * The real class is provided by Appian at runtime.
 */
public abstract class SimpleIntegrationTemplate {

    protected abstract SimpleConfiguration getConfiguration(
            SimpleConfiguration integrationConfiguration,
            SimpleConfiguration connectedSystemConfiguration,
            ExecutionContext executionContext);

    protected abstract ExecutionResult execute(
            SimpleConfiguration integrationConfiguration,
            SimpleConfiguration connectedSystemConfiguration,
            ExecutionContext executionContext);

    // --- Property builder helper methods (mirrors real SDK fluent API) ---

    protected static SimpleConnectedSystemTemplate.TextPropertyBuilder textProperty(String key) {
        return new SimpleConnectedSystemTemplate.TextPropertyBuilder(key);
    }

    protected static SimpleConnectedSystemTemplate.IntegerPropertyBuilder integerProperty(String key) {
        return new SimpleConnectedSystemTemplate.IntegerPropertyBuilder(key);
    }

    protected static SimpleConnectedSystemTemplate.BooleanPropertyBuilder booleanProperty(String key) {
        return new SimpleConnectedSystemTemplate.BooleanPropertyBuilder(key);
    }

    // --- ExecutionResult (nested for simplicity, mirrors real SDK) ---

    public static class ExecutionResult {
        private final boolean success;
        private final String errorCode;
        private final String errorMessage;
        private final Map<String, Object> result;

        private ExecutionResult(boolean success, String errorCode, String errorMessage, Map<String, Object> result) {
            this.success = success;
            this.errorCode = errorCode;
            this.errorMessage = errorMessage;
            this.result = result;
        }

        public static ExecutionResult success(Map<String, Object> result) {
            return new ExecutionResult(true, null, null, result);
        }

        public static ExecutionResult error(String errorCode, String errorMessage, Map<String, Object> diagnostics) {
            return new ExecutionResult(false, errorCode, errorMessage, diagnostics);
        }

        public boolean isSuccess() { return success; }
        public String getErrorCode() { return errorCode; }
        public String getErrorMessage() { return errorMessage; }
        public Map<String, Object> getResult() { return result; }
    }
}
