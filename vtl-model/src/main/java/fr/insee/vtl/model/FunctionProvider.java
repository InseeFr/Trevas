package fr.insee.vtl.model;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.script.ScriptEngine;

/** Registers extension functions to be made available in the VTL engine. */
public interface FunctionProvider {

  /**
   * Returns functions to add to the VTL engine.
   *
   * <p>Default implementation wraps {@link #getFunctionBindings(ScriptEngine)} so SPI modules that
   * expose one {@link Method} per VTL name keep working. Override this method directly to register
   * overloads.
   *
   * @param vtlEngine the VTL implementation of the {@link ScriptEngine}.
   * @return VTL function name to reflective {@link Method} bindings (supports overloads).
   */
  default Map<String, List<Method>> getFunctions(ScriptEngine vtlEngine) {
    Map<String, Method> bindings = getFunctionBindings(vtlEngine);
    if (bindings.isEmpty()) {
      return Map.of();
    }
    Map<String, List<Method>> result = new LinkedHashMap<>();
    bindings.forEach((name, method) -> result.put(name, List.of(method)));
    return Map.copyOf(result);
  }

  /**
   * Legacy SPI hook: one reflective binding per VTL function name.
   *
   * <p>Override this when migrating modules that previously implemented {@code Map<String, Method>
   * getFunctions(ScriptEngine)}. New modules should override {@link #getFunctions(ScriptEngine)}
   * instead.
   *
   * @param vtlEngine the VTL implementation of the {@link ScriptEngine}.
   * @return VTL function name to reflective {@link Method} binding.
   */
  default Map<String, Method> getFunctionBindings(ScriptEngine vtlEngine) {
    return Map.of();
  }
}
