package fr.insee.vtl.model;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import javax.script.ScriptEngine;

/** Registers extension functions to be made available in the VTL engine. */
public interface FunctionProvider {

  /**
   * Returns functions to add to the VTL engine.
   *
   * @param vtlEngine the VTL implementation of the {@link ScriptEngine}.
   * @return VTL function name to reflective {@link Method} bindings (supports overloads).
   */
  Map<String, List<Method>> getFunctions(ScriptEngine vtlEngine);
}
