package fr.insee.vtl.model;

import javax.script.ScriptEngine;

public interface ProcessingEngineFactory {

    String getName();

    ProcessingEngine getProcessingEngine(ScriptEngine engine);

}
