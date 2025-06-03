package fr.insee.vtl.model;

import javax.script.ScriptEngine;

/**
 * A service used by the VTL engine to retrieve processing engines. <code>
 * ServiceLoader.load(ProcessingEngineFactory.class)
 * </code>
 */
public interface ProcessingEngineFactory {

  /** Returns the name of the processing engine. */
  String getName();

  /** Instantiates a new processing engine for the script engine. */
  ProcessingEngine getProcessingEngine(ScriptEngine engine);
}
