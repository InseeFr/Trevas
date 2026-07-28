package fr.insee.vtl.engine.functions.providers;

import com.github.hervian.reflection.Fun;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;
import java.util.Map;

public final class ComparisonOperatorFunctionsProvider {

  private static Integer compare(Object left, Object right) throws Exception {
    if (left == null || right == null) {
      return null;
    }
    if (left instanceof Number leftNumber && right instanceof Number rightNumber) {
      if (left instanceof Long leftLong && right instanceof Long rightLong) {
        return Long.compare(leftLong, rightLong);
      }
      return Double.compare(leftNumber.doubleValue(), rightNumber.doubleValue());
    }
    if (left instanceof Boolean leftBoolean && right instanceof Boolean rightBoolean) {
      return Boolean.compare(leftBoolean, rightBoolean);
    }
    if (left instanceof String leftString && right instanceof String rightString) {
      return leftString.compareTo(rightString);
    }
    if (left instanceof Date leftDate && right instanceof Date rightDate) {
      return leftDate.compareTo(rightDate);
    }
    throw new Exception("Comparisons require Comparable params");
  }

  public static Boolean isEqual(Object left, Object right) throws Exception {
    Integer result = compare(left, right);
    if (result == null) {
      return null;
    }
    return result == 0;
  }

  public static Boolean isNotEqual(Object left, Object right) throws Exception {
    Integer result = compare(left, right);
    if (result == null) {
      return null;
    }
    return result != 0;
  }

  public static Boolean isLessThan(Object left, Object right) throws Exception {
    Integer result = compare(left, right);
    if (result == null) {
      return null;
    }
    return result < 0;
  }

  public static Boolean isGreaterThan(Object left, Object right) throws Exception {
    Integer result = compare(left, right);
    if (result == null) {
      return null;
    }
    return result > 0;
  }

  public static Boolean isLessThanOrEqual(Object left, Object right) throws Exception {
    Integer result = compare(left, right);
    if (result == null) {
      return null;
    }
    return result <= 0;
  }

  public static Boolean isGreaterThanOrEqual(Object left, Object right) throws Exception {
    Integer result = compare(left, right);
    if (result == null) {
      return null;
    }
    return result >= 0;
  }

  public static Boolean in(Object obj, List<?> list) {
    if (obj == null) {
      return null;
    }
    return list.contains(obj);
  }

  public static Boolean notIn(Object obj, List<?> list) {
    if (obj == null) {
      return null;
    }
    return !list.contains(obj);
  }

  public Map<String, List<Method>> getFunctions() {
    Map<String, List<Method>> functions = new java.util.LinkedHashMap<>();
    functions.put("isEqual", List.of(Fun.toMethod(ComparisonOperatorFunctionsProvider::isEqual)));
    functions.put(
        "isNotEqual", List.of(Fun.toMethod(ComparisonOperatorFunctionsProvider::isNotEqual)));
    functions.put(
        "isLessThan", List.of(Fun.toMethod(ComparisonOperatorFunctionsProvider::isLessThan)));
    functions.put(
        "isGreaterThan", List.of(Fun.toMethod(ComparisonOperatorFunctionsProvider::isGreaterThan)));
    functions.put(
        "isLessThanOrEqual",
        List.of(Fun.toMethod(ComparisonOperatorFunctionsProvider::isLessThanOrEqual)));
    functions.put(
        "isGreaterThanOrEqual",
        List.of(Fun.toMethod(ComparisonOperatorFunctionsProvider::isGreaterThanOrEqual)));
    functions.put("in", List.of(Fun.toMethod(ComparisonOperatorFunctionsProvider::in)));
    functions.put("notIn", List.of(Fun.toMethod(ComparisonOperatorFunctionsProvider::notIn)));
    return functions;
  }
}
