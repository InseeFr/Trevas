package fr.insee.vtl.engine.functions.providers;

import com.github.hervian.reflection.Fun;
import fr.insee.vtl.engine.functions.BuiltinFunctionProvider;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public final class ConditionalFunctionsProvider implements BuiltinFunctionProvider {

  public static Long ifThenElse(Boolean condition, Long thenExpr, Long elseExpr) {
    if (condition == null) {
      return null;
    }
    return condition ? thenExpr : elseExpr;
  }

  public static Double ifThenElse(Boolean condition, Double thenExpr, Double elseExpr) {
    if (condition == null) {
      return null;
    }
    return condition ? thenExpr : elseExpr;
  }

  public static String ifThenElse(Boolean condition, String thenExpr, String elseExpr) {
    if (condition == null) {
      return null;
    }
    return condition ? thenExpr : elseExpr;
  }

  public static Boolean ifThenElse(Boolean condition, Boolean thenExpr, Boolean elseExpr) {
    if (condition == null) {
      return null;
    }
    return condition ? thenExpr : elseExpr;
  }

  public static Long nvl(Long value, Long defaultValue) {
    return value == null ? defaultValue : value;
  }

  public static Double nvl(Double value, Double defaultValue) {
    return value == null ? defaultValue : value;
  }

  public static Double nvl(Double value, Long defaultValue) {
    return value == null ? defaultValue.doubleValue() : value;
  }

  public static Double nvl(Long value, Double defaultValue) {
    return value == null ? defaultValue : value.doubleValue();
  }

  public static String nvl(String value, String defaultValue) {
    return value == null ? defaultValue : value;
  }

  public static Boolean nvl(Boolean value, Boolean defaultValue) {
    return value == null ? defaultValue : value;
  }

  @Override
  public Map<String, List<Method>> getFunctions() {
    return Map.of(
        "ifThenElse",
            List.of(
                Fun.<Boolean, Long, Long>toMethod(ConditionalFunctionsProvider::ifThenElse),
                Fun.<Boolean, Double, Double>toMethod(ConditionalFunctionsProvider::ifThenElse),
                Fun.<Boolean, String, String>toMethod(ConditionalFunctionsProvider::ifThenElse),
                Fun.<Boolean, Boolean, Boolean>toMethod(ConditionalFunctionsProvider::ifThenElse)),
        "nvl",
            List.of(
                Fun.<Long, Long>toMethod(ConditionalFunctionsProvider::nvl),
                Fun.<Double, Double>toMethod(ConditionalFunctionsProvider::nvl),
                Fun.<Double, Long>toMethod(ConditionalFunctionsProvider::nvl),
                Fun.<Long, Double>toMethod(ConditionalFunctionsProvider::nvl),
                Fun.<String, String>toMethod(ConditionalFunctionsProvider::nvl),
                Fun.<Boolean, Boolean>toMethod(ConditionalFunctionsProvider::nvl)));
  }
}
