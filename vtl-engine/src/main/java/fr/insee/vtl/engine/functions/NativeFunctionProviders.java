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
import fr.insee.vtl.model.FunctionProvider;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import javax.script.ScriptEngine;

/** Built-in scalar function catalogue exposed as a {@link FunctionProvider}. */
public enum NativeFunctionProviders implements FunctionProvider {
  INSTANCE;

  private static final List<Supplier<Map<String, List<Method>>>> PARTS =
      List.of(
          () -> new NumericFunctionsProvider().getFunctions(),
          () -> new StringFunctionsProvider().getFunctions(),
          () -> new DistanceFunctionsProvider().getFunctions(),
          () -> new ArithmeticFunctionsProvider().getFunctions(),
          () -> new ConditionalFunctionsProvider().getFunctions(),
          () -> new BooleanFunctionsProvider().getFunctions(),
          () -> new UnaryFunctionsProvider().getFunctions(),
          () -> new ComparisonOperatorFunctionsProvider().getFunctions(),
          () -> new ComparisonFunctionsProvider().getFunctions(),
          () -> new TemporalFunctionsProvider().getFunctions());

  @Override
  public Map<String, List<Method>> getFunctions(ScriptEngine vtlEngine) {
    return builtinFunctions();
  }

  public static Map<String, List<Method>> builtinFunctions() {
    Map<String, List<Method>> functions = new LinkedHashMap<>();
    for (Supplier<Map<String, List<Method>>> part : PARTS) {
      part.get()
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
