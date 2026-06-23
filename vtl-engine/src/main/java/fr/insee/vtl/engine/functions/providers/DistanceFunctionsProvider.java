package fr.insee.vtl.engine.functions.providers;

import com.github.hervian.reflection.Fun;
import fr.insee.vtl.engine.functions.BuiltinFunctionProvider;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.apache.commons.text.similarity.LevenshteinDistance;

public final class DistanceFunctionsProvider implements BuiltinFunctionProvider {

  public static Long levenshtein(String stringA, String stringB) {
    if (stringA == null || stringB == null) {
      return null;
    }
    return Long.valueOf(LevenshteinDistance.getDefaultInstance().apply(stringA, stringB));
  }

  @Override
  public Map<String, List<Method>> getFunctions() {
    return Map.of("levenshtein", List.of(Fun.toMethod(DistanceFunctionsProvider::levenshtein)));
  }
}
