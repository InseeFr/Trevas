package fr.insee.vtl.engine.functions;

import fr.insee.vtl.model.FunctionProvider;

import javax.script.ScriptEngine;
import java.lang.reflect.Method;
import java.util.Map;

public class LevenshteinProvider implements FunctionProvider {

    @Override
    public Map<String, Method> getFunctions(ScriptEngine vtlEngine) {
        return Map.of();
    }

}
