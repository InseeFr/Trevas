package fr.insee.vtl.engine.functions.providers;

import com.github.hervian.reflection.Fun;
import fr.insee.vtl.engine.functions.BuiltinFunctionProvider;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public final class BooleanFunctionsProvider implements BuiltinFunctionProvider {

  public static Boolean and(Boolean left, Boolean right) {
    if (left != null && !left) return false;
    if (right != null && !right) return false;
    if (left == null || right == null) return null;
    return true;
  }

  public static Boolean or(Boolean left, Boolean right) {
    if (left != null && left) {
      return true;
    }
    if (right != null && right) {
      return true;
    }
    if (left == null || right == null) {
      return null;
    }
    return false;
  }

  public static Boolean xor(Boolean left, Boolean right) {
    if (left == null || right == null) {
      return null;
    }
    return left ^ right;
  }

  @Override
  public Map<String, List<Method>> getFunctions() {
    return Map.of(
        "and", List.of(Fun.toMethod(BooleanFunctionsProvider::and)),
        "or", List.of(Fun.toMethod(BooleanFunctionsProvider::or)),
        "xor", List.of(Fun.toMethod(BooleanFunctionsProvider::xor)));
  }
}
