package fr.insee.vtl.engine.functions;

import fr.insee.vtl.model.VtlMethod;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Catalogue of native reflective {@link Method} bindings keyed by VTL function name, with overload
 * resolution.
 *
 * <h2>Registration</h2>
 *
 * <ul>
 *   <li>{@link #register(String, Method)} adds a binding. Parameter-type duplicates under the same
 *       VTL name are rejected.
 *   <li>{@link #putAndReturnPrevious(String, Method)} replaces the binding that has the same
 *       parameter types (if any), otherwise appends. Other overloads for that VTL name are kept.
 * </ul>
 *
 * <h2>Resolution ({@link #resolve})</h2>
 *
 * Given a VTL name and the runtime argument types:
 *
 * <ol>
 *   <li>Look up candidates registered under that VTL name.
 *   <li>Keep candidates for which {@link #matchParameters} succeeds (assignability, including
 *       shared type variables such as {@code <T extends Comparable<T>>}).
 *   <li>If exactly one candidate matches, return it.
 *   <li>Otherwise, if several (or zero) soft-matches remain, fall back to an <em>exact</em>
 *       parameter-type identity check ({@code
 *       types.equals(Arrays.asList(method.getParameterTypes()))}).
 *   <li>If still unresolved, throw {@link NoSuchMethodException}.
 * </ol>
 *
 * The soft-match step allows polymorphic natives (e.g. {@code between(T, T, T)}); the exact-match
 * fallback disambiguates when several overloads are assignable.
 */
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
          if (updated.stream().anyMatch(existing -> parameterTypesEqual(existing, method))) {
            throw new IllegalStateException(
                "duplicate native function binding for " + vtlName + ": " + method);
          }
          updated.add(method);
          return List.copyOf(updated);
        });
  }

  /**
   * Replaces the overload with the same parameter types under {@code vtlName}, or appends if none
   * exists. Does not drop unrelated overloads.
   *
   * @return the previous method with the same parameter types, or {@code null}
   */
  public Method putAndReturnPrevious(String vtlName, Method method) {
    Objects.requireNonNull(vtlName);
    Objects.requireNonNull(method);
    List<Method> previous = byVtlName.getOrDefault(vtlName, List.of());
    Method replaced = null;
    List<Method> updated = new ArrayList<>(previous.size() + 1);
    for (Method existing : previous) {
      if (parameterTypesEqual(existing, method)) {
        replaced = existing;
        updated.add(method);
      } else {
        updated.add(existing);
      }
    }
    if (replaced == null) {
      updated.add(method);
    }
    byVtlName.put(vtlName, List.copyOf(updated));
    return replaced;
  }

  public VtlMethod resolve(String vtlName, Collection<Class> types) throws NoSuchMethodException {
    List<Method> candidates = candidatesFor(vtlName);
    if (candidates.isEmpty()) {
      throw new NoSuchMethodException(methodToString(vtlName, types));
    }

    Class<?>[] argumentTypes = types.toArray(Class[]::new);
    List<Method> softMatches = softMatches(candidates, argumentTypes);
    if (softMatches.size() == 1) {
      return new VtlMethod(softMatches.get(0));
    }

    Method exact = exactMatch(candidates, types);
    if (exact != null) {
      return new VtlMethod(exact);
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

  private List<Method> candidatesFor(String vtlName) {
    List<Method> candidates = byVtlName.get(vtlName);
    return candidates == null ? List.of() : candidates;
  }

  /** Soft matches: argument types are assignable to the method parameters. */
  private static List<Method> softMatches(List<Method> candidates, Class<?>[] argumentTypes) {
    List<Method> matches = new ArrayList<>();
    for (Method method : candidates) {
      if (matchParameters(method, argumentTypes)) {
        matches.add(method);
      }
    }
    return matches;
  }

  /** Exact match: argument types equal the declared parameter types (order-sensitive). */
  private static Method exactMatch(List<Method> candidates, Collection<Class> types) {
    for (Method method : candidates) {
      if (types.equals(Arrays.asList(method.getParameterTypes()))) {
        return method;
      }
    }
    return null;
  }

  /**
   * Whether {@code classes} can be passed to {@code method}, including consistency of shared type
   * variables across parameters.
   */
  static boolean matchParameters(Method method, Class<?>... classes) {
    Type[] genericParameterTypes = method.getGenericParameterTypes();
    Class<?>[] parameterTypes = method.getParameterTypes();

    if (classes.length != parameterTypes.length) {
      return false;
    }

    Map<TypeVariable<?>, Class<?>> typeArguments = new HashMap<>();

    for (int i = 0; i < parameterTypes.length; i++) {
      if (!isAssignableTo(classes[i], parameterTypes[i], genericParameterTypes[i], typeArguments)) {
        return false;
      }
    }

    return true;
  }

  static boolean isAssignableTo(
      Class<?> clazz,
      Class<?> target,
      Type genericTarget,
      Map<TypeVariable<?>, Class<?>> typeArguments) {
    if (target.isAssignableFrom(clazz)) {
      if (genericTarget instanceof TypeVariable<?> typeVariable) {
        Class<?> existingTypeArgument = typeArguments.get(typeVariable);
        if (existingTypeArgument == null) {
          typeArguments.put(typeVariable, clazz);
        } else {
          return existingTypeArgument.equals(clazz);
        }
      }
      return true;
    }

    if (genericTarget instanceof ParameterizedType parameterizedType) {
      Type[] typeArgumentsArray = parameterizedType.getActualTypeArguments();

      if (typeArgumentsArray.length != 1) {
        return false;
      }

      Type typeArgument = typeArgumentsArray[0];

      if (typeArgument instanceof TypeVariable<?> typeVariable) {
        Class<?> existingTypeArgument = typeArguments.get(typeVariable);
        if (existingTypeArgument == null) {
          typeArguments.put(typeVariable, clazz);
        } else {
          return existingTypeArgument.equals(clazz);
        }
        return true;
      } else if (typeArgument instanceof Class<?> classArgument) {
        return classArgument.isAssignableFrom(clazz);
      }
    }

    return false;
  }

  /** Duplicate check is by parameter types only: methods are already keyed by VTL name. */
  private static boolean parameterTypesEqual(Method left, Method right) {
    return Arrays.equals(left.getParameterTypes(), right.getParameterTypes());
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
