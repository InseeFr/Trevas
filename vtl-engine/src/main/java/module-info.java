import fr.insee.vtl.engine.VtlScriptEngineFactory;

import javax.script.ScriptEngineFactory;

/**
 * This module contains the actual VTL engine.
 */
module fr.insee.vtl.engine {
    requires java.scripting;
    requires transitive fr.insee.vtl.parser;
    requires transitive fr.insee.vtl.model;
    requires org.antlr.antlr4.runtime;
    requires org.apache.commons.lang3;
    provides ScriptEngineFactory with VtlScriptEngineFactory;
}