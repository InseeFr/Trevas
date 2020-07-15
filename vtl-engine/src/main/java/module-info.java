import fr.insee.vtl.engine.VtlScriptEngineFactory;

import javax.script.ScriptEngineFactory;

module fr.insee.vtl.engine {
    requires java.scripting;
    requires transitive fr.insee.vtl.parser;
    requires transitive fr.insee.vtl.model;
    requires org.antlr.antlr4.runtime;
    provides ScriptEngineFactory with VtlScriptEngineFactory;
}