package fr.insee.vtl.engine;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import java.util.List;

public class VtlScriptEngineFactory implements ScriptEngineFactory {
    @Override
    public String getEngineName() {
        return "Java VTL engine";
    }

    @Override
    public String getEngineVersion() {
        return "0.0.1";
    }

    @Override
    public List<String> getExtensions() {
        return List.of("vtl");
    }

    @Override
    public List<String> getMimeTypes() {
        return List.of();
    }

    @Override
    public List<String> getNames() {
        return List.of(getLanguageName());
    }

    @Override
    public String getLanguageName() {
        return "vtl";
    }

    @Override
    public String getLanguageVersion() {
        return "2.1";
    }

    @Override
    public Object getParameter(String key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getMethodCallSyntax(String obj, String m, String... args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getOutputStatement(String toDisplay) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getProgram(String... statements) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScriptEngine getScriptEngine() {
        return new VtlScriptEngine(this);
    }
}
