package com.example.appian.mqtt.core;

import com.appiancorp.services.ServiceContext;
import com.appiancorp.services.ServiceContextFactory;
import com.appiancorp.suiteapi.process.ProcessExecutionService;
import com.appiancorp.suiteapi.process.ProcessVariable;
import com.appiancorp.suiteapi.process.TypedValue;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Safe execution wrapper over Appian's ProcessExecutionService.
 * <p>
 * Converts Java payload maps into ProcessVariable arrays and launches
 * Appian process instances from background threads.
 * <p>
 * Thread safety: this class is effectively immutable after construction
 * (all fields are final), making it safe for concurrent use by multiple threads.
 * <p>
 * Exception handling: all exceptions are caught and logged — this class
 * NEVER propagates exceptions to prevent background thread crashes.
 */
public class AppianProcessLauncher {

    private static final Logger LOG = Logger.getLogger(AppianProcessLauncher.class.getName());

    private final ProcessExecutionService processService;
    private final ServiceContext serviceContext;

    /**
     * Creates a new process launcher.
     *
     * @param processService         Appian's ProcessExecutionService instance
     * @param serviceAccountUsername Username of the service account for background execution
     */
    public AppianProcessLauncher(ProcessExecutionService processService, String serviceAccountUsername) {
        this.processService = processService;
        this.serviceContext = ServiceContextFactory.getServiceContext(serviceAccountUsername);
    }

    /**
     * Triggers an Appian process instance with the given input parameters.
     *
     * @param processModelId  The ID of the process model to start
     * @param inputParameters Map of process variable names to values (String, Long, Double, Boolean)
     * @return The process instance ID on success, or null on failure
     */
    public Long triggerProcess(Long processModelId, Map<String, Object> inputParameters) {
        try {
            ProcessVariable[] processVariables = buildVariables(inputParameters);

            Long processInstanceId = processService.startProcess(
                    serviceContext,
                    processModelId,
                    processVariables
            );

            LOG.info("Triggered Appian Process Instance ID: " + processInstanceId
                    + " (model=" + processModelId + ")");
            return processInstanceId;

        } catch (Throwable t) {
            // Catch Throwable (not just Exception) to prevent background thread crashes
            LOG.log(Level.SEVERE, "Failed to start process model ID: " + processModelId, t);
            return null;
        }
    }

    /**
     * Converts a map of parameters into Appian ProcessVariable array.
     */
    ProcessVariable[] buildVariables(Map<String, Object> params) {
        if (params == null || params.isEmpty()) {
            return new ProcessVariable[0];
        }

        ProcessVariable[] vars = new ProcessVariable[params.size()];
        int i = 0;

        for (Map.Entry<String, Object> entry : params.entrySet()) {
            ProcessVariable pv = new ProcessVariable();
            pv.setName(entry.getKey());

            TypedValue tv = new TypedValue();
            tv.setValue(entry.getValue());
            pv.setValue(tv);

            vars[i++] = pv;
        }

        return vars;
    }
}
