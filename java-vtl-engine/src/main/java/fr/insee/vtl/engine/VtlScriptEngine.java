package fr.insee.vtl.engine;

import javax.script.AbstractScriptEngine;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngineFactory;
import java.io.Reader;

public class VtlScriptEngine extends AbstractScriptEngine {

    private final ScriptEngineFactory factory;

    public VtlScriptEngine(ScriptEngineFactory factory) {
        this.factory = factory;
    }

    @Override
    public Object eval(String script, ScriptContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object eval(Reader reader, ScriptContext context) {
        throw new UnsupportedOperationException();

    }

    @Override
    public Bindings createBindings() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScriptEngineFactory getFactory() {
        return factory;
    }
}
