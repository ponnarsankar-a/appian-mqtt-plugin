package com.appiancorp.suiteapi.process;

import com.appiancorp.services.ServiceContext;

/**
 * Stub for Appian Suite API ProcessExecutionService.
 * The real class is provided by Appian at runtime.
 */
public interface ProcessExecutionService {

    /**
     * Starts a new process instance.
     *
     * @param context          The service context for authentication
     * @param processModelId   The ID of the process model to start
     * @param processVariables Initial process variables
     * @return The process instance ID
     * @throws Exception if the process cannot be started
     */
    Long startProcess(ServiceContext context, Long processModelId, ProcessVariable[] processVariables)
            throws Exception;
}
