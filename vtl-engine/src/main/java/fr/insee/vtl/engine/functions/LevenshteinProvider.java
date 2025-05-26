package fr.insee.vtl.engine.functions;

import fr.insee.vtl.model.FunctionProvider;
import java.lang.reflect.Method;
import java.util.Map;
import javax.script.ScriptEngine;

public class LevenshteinProvider implements FunctionProvider {

  @Override
  public Map<String, Method> getFunctions(ScriptEngine vtlEngine) {
    return Map.of();
  }
}
