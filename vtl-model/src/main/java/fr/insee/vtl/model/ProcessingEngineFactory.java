package fr.insee.vtl.model;

import javax.script.ScriptEngine;

/**
 * A service used by the vtl engine to retrieve processing engines.
 *
 * <code>
 * ServiceLoader.load(ProcessingEngineFactory.class)
 * </code>
 */
public interface ProcessingEngineFactory {

    /**
     * The name of the processing engine.
     */
    String getName();

    /**
     * Instantiate a new Processing engine for the script engine.
     */
    ProcessingEngine getProcessingEngine(ScriptEngine engine);

}
