package fr.insee.vtl.engine.functions;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.VtlMethod;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/** VTL-name keyed catalogue of native {@link Method} bindings with overload resolution. */
public final class NativeFunctionRegistry {

  private final Map<String, List<Method>> byVtlName = new LinkedHashMap<>();

  public static NativeFunctionRegistry empty() {
    return new NativeFunctionRegistry();
  }

  public void registerAll(Map<String, List<Method>> functions) {
    functions.forEach((vtlName, methods) -> methods.forEach(method -> register(vtlName, method)));
  }

  public void register(String vtlName, Method method) {
    Objects.requireNonNull(vtlName);
    Objects.requireNonNull(method);
    byVtlName.compute(
        vtlName,
        (name, methods) -> {
          List<Method> updated = methods == null ? new ArrayList<>() : new ArrayList<>(methods);
          if (updated.stream().anyMatch(existing -> signaturesEqual(existing, method))) {
            throw new IllegalStateException(
                "duplicate native function binding for " + vtlName + ": " + method);
          }
          updated.add(method);
          return List.copyOf(updated);
        });
  }

  public Method putAndReturnPrevious(String vtlName, Method method) {
    Objects.requireNonNull(vtlName);
    Objects.requireNonNull(method);
    List<Method> previous = byVtlName.put(vtlName, List.of(method));
    return previous == null || previous.isEmpty() ? null : previous.get(0);
  }

  public VtlMethod resolve(String vtlName, Collection<Class> types) throws NoSuchMethodException {
    List<Method> candidates = byVtlName.get(vtlName);
    if (candidates == null || candidates.isEmpty()) {
      throw new NoSuchMethodException(methodToString(vtlName, types));
    }

    List<Method> matches =
        candidates.stream()
            .filter(method -> VtlScriptEngine.matchParameters(method, types.toArray(Class[]::new)))
            .collect(Collectors.toList());
    if (matches.size() == 1) {
      return new VtlMethod(matches.get(0));
    }

    for (Method method : candidates) {
      if (types.equals(Arrays.asList(method.getParameterTypes()))) {
        return new VtlMethod(method);
      }
    }
    throw new NoSuchMethodException(methodToString(vtlName, types));
  }

  public VtlMethod resolveOrNull(String vtlName, Collection<Class> types) {
    try {
      return resolve(vtlName, types);
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  private static boolean signaturesEqual(Method left, Method right) {
    return left.getName().equals(right.getName())
        && Arrays.equals(left.getParameterTypes(), right.getParameterTypes());
  }

  private static String methodToString(String name, Collection<Class> argTypes) {
    StringJoiner sj = new StringJoiner(", ", name + "(", ")");
    if (argTypes != null) {
      for (Class<?> c : argTypes) {
        sj.add(c == null ? "null" : c.getSimpleName());
      }
    }
    return sj.toString();
  }
}
