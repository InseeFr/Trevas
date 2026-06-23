package fr.insee.vtl.engine.functions.providers;

import com.github.hervian.reflection.Fun;
import fr.insee.vtl.engine.functions.BuiltinFunctionProvider;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public final class UnaryFunctionsProvider implements BuiltinFunctionProvider {

  public static Long plus(Long right) {
    return right;
  }

  public static Double plus(Double right) {
    return right;
  }

  public static Long minus(Long right) {
    if (right == null) {
      return null;
    }
    return -right;
  }

  public static Double minus(Double right) {
    if (right == null) {
      return null;
    }
    return -right;
  }

  public static Boolean not(Boolean right) {
    if (right == null) {
      return null;
    }
    return !right;
  }

  @Override
  public Map<String, List<Method>> getFunctions() {
    return Map.of(
        "plus",
            List.of(
                Fun.<Long>toMethod(UnaryFunctionsProvider::plus),
                Fun.<Double>toMethod(UnaryFunctionsProvider::plus)),
        "minus",
            List.of(
                Fun.<Long>toMethod(UnaryFunctionsProvider::minus),
                Fun.<Double>toMethod(UnaryFunctionsProvider::minus)),
        "not", List.of(Fun.toMethod(UnaryFunctionsProvider::not)));
  }
}
