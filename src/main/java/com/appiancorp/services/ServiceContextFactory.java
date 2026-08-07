package com.appiancorp.services;

/**
 * Stub for Appian Suite API ServiceContextFactory.
 * The real class is provided by Appian at runtime.
 */
public class ServiceContextFactory {

    /**
     * Creates a ServiceContext for background thread execution.
     *
     * @param username The service account username
     * @return A ServiceContext for the given user
     */
    public static ServiceContext getServiceContext(String username) {
        return new ServiceContext() {
            @Override
            public String getUsername() {
                return username;
            }
        };
    }
}
