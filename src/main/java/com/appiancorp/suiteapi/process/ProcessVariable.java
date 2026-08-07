package com.appiancorp.suiteapi.process;

/**
 * Stub for Appian Suite API ProcessVariable.
 * The real class is provided by Appian at runtime.
 */
public class ProcessVariable {

    private String name;
    private TypedValue value;

    public ProcessVariable() {}

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public TypedValue getValue() { return value; }
    public void setValue(TypedValue value) { this.value = value; }
}
