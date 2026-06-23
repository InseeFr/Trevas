package fr.insee.vtl.engine.functions.providers;

import com.github.hervian.reflection.Fun;
import fr.insee.vtl.engine.functions.BuiltinFunctionProvider;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public final class ArithmeticFunctionsProvider implements BuiltinFunctionProvider {

  public static Long addition(Long valueA, Long valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA + valueB;
  }

  public static Double addition(Long valueA, Double valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA + valueB;
  }

  public static Double addition(Double valueA, Long valueB) {
    return addition(valueB, valueA);
  }

  public static Double addition(Double valueA, Double valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA + valueB;
  }

  public static Long subtraction(Long valueA, Long valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA - valueB;
  }

  public static Double subtraction(Long valueA, Double valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA - valueB;
  }

  public static Double subtraction(Double valueA, Long valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA - valueB.doubleValue();
  }

  public static Double subtraction(Double valueA, Double valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA - valueB;
  }

  public static String concat(String valueA, String valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA + valueB;
  }

  public static Long multiplication(Long valueA, Long valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA * valueB;
  }

  public static Double multiplication(Long valueA, Double valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA.doubleValue() * valueB;
  }

  public static Double multiplication(Double valueA, Long valueB) {
    return multiplication(valueB, valueA);
  }

  public static Double multiplication(Double valueA, Double valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA * valueB;
  }

  public static Double division(Long valueA, Double valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA.doubleValue() / valueB;
  }

  public static Double division(Double valueA, Long valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA / valueB.doubleValue();
  }

  public static Double division(Long valueA, Long valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return ((double) valueA / valueB);
  }

  public static Double division(Double valueA, Double valueB) {
    if (valueA == null || valueB == null) {
      return null;
    }
    return valueA / valueB;
  }

  @Override
  public Map<String, List<Method>> getFunctions() {
    return Map.of(
        "addition",
            List.of(
                Fun.<Long, Long>toMethod(ArithmeticFunctionsProvider::addition),
                Fun.<Double, Long>toMethod(ArithmeticFunctionsProvider::addition),
                Fun.<Long, Double>toMethod(ArithmeticFunctionsProvider::addition),
                Fun.<Double, Double>toMethod(ArithmeticFunctionsProvider::addition)),
        "subtraction",
            List.of(
                Fun.<Long, Long>toMethod(ArithmeticFunctionsProvider::subtraction),
                Fun.<Double, Long>toMethod(ArithmeticFunctionsProvider::subtraction),
                Fun.<Long, Double>toMethod(ArithmeticFunctionsProvider::subtraction),
                Fun.<Double, Double>toMethod(ArithmeticFunctionsProvider::subtraction)),
        "concat", List.of(Fun.toMethod(ArithmeticFunctionsProvider::concat)),
        "multiplication",
            List.of(
                Fun.<Long, Long>toMethod(ArithmeticFunctionsProvider::multiplication),
                Fun.<Double, Long>toMethod(ArithmeticFunctionsProvider::multiplication),
                Fun.<Long, Double>toMethod(ArithmeticFunctionsProvider::multiplication),
                Fun.<Double, Double>toMethod(ArithmeticFunctionsProvider::multiplication)),
        "division",
            List.of(
                Fun.<Long, Long>toMethod(ArithmeticFunctionsProvider::division),
                Fun.<Double, Long>toMethod(ArithmeticFunctionsProvider::division),
                Fun.<Long, Double>toMethod(ArithmeticFunctionsProvider::division),
                Fun.<Double, Double>toMethod(ArithmeticFunctionsProvider::division)));
  }
}
