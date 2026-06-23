package fr.insee.vtl.engine.functions;

import fr.insee.vtl.engine.functions.providers.ArithmeticFunctionsProvider;
import fr.insee.vtl.engine.functions.providers.BooleanFunctionsProvider;
import fr.insee.vtl.engine.functions.providers.ComparisonFunctionsProvider;
import fr.insee.vtl.engine.functions.providers.ComparisonOperatorFunctionsProvider;
import fr.insee.vtl.engine.functions.providers.ConditionalFunctionsProvider;
import fr.insee.vtl.engine.functions.providers.DistanceFunctionsProvider;
import fr.insee.vtl.engine.functions.providers.NumericFunctionsProvider;
import fr.insee.vtl.engine.functions.providers.StringFunctionsProvider;
import fr.insee.vtl.engine.functions.providers.TemporalFunctionsProvider;
import fr.insee.vtl.engine.functions.providers.UnaryFunctionsProvider;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Aggregates all built-in scalar function providers. */
public final class NativeFunctionProviders {

  private static final List<BuiltinFunctionProvider> BUILTINS =
      List.of(
          new NumericFunctionsProvider(),
          new StringFunctionsProvider(),
          new DistanceFunctionsProvider(),
          new ArithmeticFunctionsProvider(),
          new ConditionalFunctionsProvider(),
          new BooleanFunctionsProvider(),
          new UnaryFunctionsProvider(),
          new ComparisonOperatorFunctionsProvider(),
          new ComparisonFunctionsProvider(),
          new TemporalFunctionsProvider());

  private NativeFunctionProviders() {}

  public static Map<String, List<Method>> builtinFunctions() {
    Map<String, List<Method>> functions = new LinkedHashMap<>();
    for (BuiltinFunctionProvider provider : BUILTINS) {
      provider
          .getFunctions()
          .forEach(
              (vtlName, methods) ->
                  functions.merge(
                      vtlName,
                      methods,
                      (left, right) -> {
                        var merged = new ArrayList<>(left);
                        merged.addAll(right);
                        return List.copyOf(merged);
                      }));
    }
    return functions;
  }
}
