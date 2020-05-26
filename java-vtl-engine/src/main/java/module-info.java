import fr.insee.vtl.engine.VtlScriptEngineFactory;

import javax.script.ScriptEngineFactory;

module fr.insee.vtl.engine {
    requires java.scripting;
    provides ScriptEngineFactory with VtlScriptEngineFactory;
}