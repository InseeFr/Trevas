import fr.insee.vtl.engine.VtlScriptEngineFactory;
import fr.insee.vtl.model.ProcessingEngine;

import javax.script.ScriptEngineFactory;

/**
 * This module contains the actual VTL engine.
 */
module fr.insee.vtl.engine {
    exports fr.insee.vtl.engine.exceptions;

    requires transitive java.scripting;
    requires transitive fr.insee.vtl.parser;
    requires transitive fr.insee.vtl.model;

    requires org.antlr.antlr4.runtime;
    requires org.apache.commons.lang3;

    provides ScriptEngineFactory with VtlScriptEngineFactory;
    provides ProcessingEngine with fr.insee.vtl.engine.processors.InMemoryProcessingEngine;
}