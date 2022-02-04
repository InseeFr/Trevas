import fr.insee.vtl.engine.VtlScriptEngineFactory;
import fr.insee.vtl.engine.processors.InMemoryProcessingEngine;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ProcessingEngineFactory;

import javax.script.ScriptEngineFactory;

/**
 * This module contains the actual VTL engine.
 */
module fr.insee.vtl.engine {
    exports fr.insee.vtl.engine.exceptions;

    requires transitive java.scripting;
    requires transitive fr.insee.vtl.parser;
    requires transitive fr.insee.vtl.model;

    uses ProcessingEngine;
    uses ProcessingEngineFactory;

    exports fr.insee.vtl.engine.processors;
    provides ProcessingEngineFactory with InMemoryProcessingEngine.Factory;

    requires org.antlr.antlr4.runtime;

    // TODO: Consider removing these.
    requires org.apache.commons.lang3;
    requires org.apache.commons.text;

    provides ScriptEngineFactory with VtlScriptEngineFactory;

}
