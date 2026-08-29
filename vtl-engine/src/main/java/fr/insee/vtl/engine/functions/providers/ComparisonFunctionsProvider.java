package fr.insee.vtl.engine.functions.providers;

import com.github.hervian.reflection.Fun;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ComparisonFunctionsProvider {

  public static Boolean between(Number operand, Number from, Number to) {
    if (operand == null || from == null || to == null) {
      return null;
    }
    BigDecimal operandValue =
        operand instanceof Long
            ? BigDecimal.valueOf(operand.longValue())
            : BigDecimal.valueOf(operand.doubleValue());
    BigDecimal fromValue =
        from instanceof Long
            ? BigDecimal.valueOf(from.longValue())
            : BigDecimal.valueOf(from.doubleValue());
    BigDecimal toValue =
        to instanceof Long
            ? BigDecimal.valueOf(to.longValue())
            : BigDecimal.valueOf(to.doubleValue());
    return operandValue.compareTo(fromValue) >= 0 && operandValue.compareTo(toValue) <= 0;
  }

  public static Boolean charsetMatch(String operandValue, String patternValue) {
    if (operandValue == null || patternValue == null) {
      return null;
    }
    Pattern pattern = Pattern.compile(patternValue);
    Matcher matcher = pattern.matcher(operandValue);
    return matcher.matches();
  }

  public static Boolean isNull(Object obj) {
    if (obj == null) {
      return Boolean.TRUE;
    }
    return Boolean.FALSE;
  }

  public Map<String, List<Method>> getFunctions() {
    return Map.of(
        "between", List.of(Fun.toMethod(ComparisonFunctionsProvider::between)),
        "charsetMatch", List.of(Fun.toMethod(ComparisonFunctionsProvider::charsetMatch)),
        "isNull", List.of(Fun.toMethod(ComparisonFunctionsProvider::isNull)));
  }
}
