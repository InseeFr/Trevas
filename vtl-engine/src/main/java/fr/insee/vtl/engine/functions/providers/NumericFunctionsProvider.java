package fr.insee.vtl.engine.functions.providers;

import com.github.hervian.reflection.Fun;
import fr.insee.vtl.engine.functions.BuiltinFunctionProvider;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Random;

public final class NumericFunctionsProvider implements BuiltinFunctionProvider {

  public static Long ceil(Number value) {
    if (value == null) {
      return null;
    }
    return (long) Math.ceil(value.doubleValue());
  }

  public static Long floor(Number value) {
    if (value == null) {
      return null;
    }
    return (long) Math.floor(value.doubleValue());
  }

  public static Double abs(Number value) {
    if (value == null) {
      return null;
    }
    return Math.abs(value.doubleValue());
  }

  public static Double exp(Number value) {
    if (value == null) {
      return null;
    }
    return Math.exp(value.doubleValue());
  }

  public static Double ln(Number value) {
    if (value == null) {
      return null;
    }
    return Math.log(value.doubleValue());
  }

  public static Double sqrt(Number value) {
    if (value == null) {
      return null;
    }
    if (value.doubleValue() < 0) {
      throw new IllegalArgumentException("operand has to be 0 or positive");
    }
    return Math.sqrt(value.doubleValue());
  }

  public static Double round(Number value, Long decimal) {
    if (decimal == null) {
      decimal = 0L;
    }
    if (value == null) {
      return null;
    }
    BigDecimal bd = new BigDecimal(Double.toString(value.doubleValue()));
    bd = bd.setScale(decimal.intValue(), RoundingMode.HALF_UP);
    return bd.doubleValue();
  }

  public static Double trunc(Number value, Long decimal) {
    if (decimal == null) {
      decimal = 0L;
    }
    if (value == null) {
      return null;
    }
    BigDecimal bd = new BigDecimal(Double.toString(value.doubleValue()));
    bd = bd.setScale(decimal.intValue(), RoundingMode.DOWN);
    return bd.doubleValue();
  }

  public static Double mod(Number left, Number right) {
    if (left == null || right == null) {
      return null;
    }
    if (right.doubleValue() == 0) {
      return left.doubleValue();
    }
    return (left.doubleValue() % right.doubleValue()) * (right.doubleValue() < 0 ? -1 : 1);
  }

  public static Double power(Number left, Number right) {
    if (left == null || right == null) {
      return null;
    }
    return Math.pow(left.doubleValue(), right.doubleValue());
  }

  public static Double random(Long left, Long right) {
    if (left == null || right == null) {
      return null;
    }
    Double res = null;
    Random random = new Random(left);
    for (int i = 0; i < right; i++) {
      res = random.nextDouble();
    }
    return res;
  }

  public static Double log(Number operand, Number base) {
    if (operand == null || base == null) {
      return null;
    }
    if (operand.doubleValue() <= 0) throw new IllegalArgumentException("operand must be positive");
    if (base.doubleValue() < 1)
      throw new IllegalArgumentException("base must be greater or equal than 1");
    return Math.log(operand.doubleValue()) / Math.log(base.doubleValue());
  }

  @Override
  public Map<String, List<Method>> getFunctions() {
    Map<String, List<Method>> functions = new java.util.LinkedHashMap<>();
    functions.put("ceil", List.of(Fun.toMethod(NumericFunctionsProvider::ceil)));
    functions.put("floor", List.of(Fun.toMethod(NumericFunctionsProvider::floor)));
    functions.put("abs", List.of(Fun.toMethod(NumericFunctionsProvider::abs)));
    functions.put("exp", List.of(Fun.toMethod(NumericFunctionsProvider::exp)));
    functions.put("ln", List.of(Fun.toMethod(NumericFunctionsProvider::ln)));
    functions.put("sqrt", List.of(Fun.toMethod(NumericFunctionsProvider::sqrt)));
    functions.put("round", List.of(Fun.toMethod(NumericFunctionsProvider::round)));
    functions.put("trunc", List.of(Fun.toMethod(NumericFunctionsProvider::trunc)));
    functions.put("mod", List.of(Fun.toMethod(NumericFunctionsProvider::mod)));
    functions.put("power", List.of(Fun.toMethod(NumericFunctionsProvider::power)));
    functions.put("random", List.of(Fun.toMethod(NumericFunctionsProvider::random)));
    functions.put("log", List.of(Fun.toMethod(NumericFunctionsProvider::log)));
    return functions;
  }
}
