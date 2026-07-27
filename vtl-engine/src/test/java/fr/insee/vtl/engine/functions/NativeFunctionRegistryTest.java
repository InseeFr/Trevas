package fr.insee.vtl.engine.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.model.Positioned;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NativeFunctionRegistryTest {

  private static final Positioned POS = () -> new Positioned.Position("", 0, 0, 0, 0);

  private NativeFunctionRegistry registry;

  @BeforeEach
  void setUp() {
    registry = NativeFunctionRegistry.empty();
  }

  @Test
  void resolveReturnsSingleSoftMatch() throws Exception {
    Method ceil = Samples.class.getMethod("roundDouble", Double.class);
    registry.register("ceil", ceil);

    Method resolved = registry.resolve("ceil", List.of(Double.class)).getMethod(POS);

    assertThat(resolved).isEqualTo(ceil);
  }

  @Test
  void resolvePicksAssignableOverload() throws Exception {
    Method longRound = Samples.class.getMethod("roundLong", Long.class);
    Method doubleRound = Samples.class.getMethod("roundDouble", Double.class);
    registry.register("round", longRound);
    registry.register("round", doubleRound);

    assertThat(registry.resolve("round", List.of(Long.class)).getMethod(POS)).isEqualTo(longRound);
    assertThat(registry.resolve("round", List.of(Double.class)).getMethod(POS))
        .isEqualTo(doubleRound);
  }

  @Test
  void resolveThrowsWhenUnknownNameOrNoMatch() throws Exception {
    registry.register("ceil", Samples.class.getMethod("roundDouble", Double.class));

    assertThatThrownBy(() -> registry.resolve("missing", List.of(Double.class)))
        .isInstanceOf(NoSuchMethodException.class)
        .hasMessageContaining("missing(Double)");

    assertThatThrownBy(() -> registry.resolve("ceil", List.of(String.class)))
        .isInstanceOf(NoSuchMethodException.class)
        .hasMessageContaining("ceil(String)");
  }

  @Test
  void registerRejectsDuplicateParameterTypesRegardlessOfJavaMethodName() throws Exception {
    registry.register("f", Samples.class.getMethod("left", String.class));

    assertThatThrownBy(() -> registry.register("f", Samples.class.getMethod("right", String.class)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("duplicate native function binding for f");
  }

  @Test
  void putAndReturnPreviousReplacesOnlyMatchingOverload() throws Exception {
    Method longRound = Samples.class.getMethod("roundLong", Long.class);
    Method doubleRound = Samples.class.getMethod("roundDouble", Double.class);
    Method replacement = Samples.class.getMethod("roundLongAlt", Long.class);

    registry.register("round", longRound);
    registry.register("round", doubleRound);

    Method previous = registry.putAndReturnPrevious("round", replacement);

    assertThat(previous).isSameAs(longRound);
    assertThat(registry.resolve("round", List.of(Long.class)).getMethod(POS))
        .isEqualTo(replacement);
    assertThat(registry.resolve("round", List.of(Double.class)).getMethod(POS))
        .isEqualTo(doubleRound);
  }

  @Test
  void putAndReturnPreviousAppendsWhenSignatureIsNew() throws Exception {
    Method longRound = Samples.class.getMethod("roundLong", Long.class);
    Method doubleRound = Samples.class.getMethod("roundDouble", Double.class);
    registry.register("round", longRound);

    assertThat(registry.putAndReturnPrevious("round", doubleRound)).isNull();
    assertThat(registry.resolve("round", List.of(Double.class)).getMethod(POS))
        .isEqualTo(doubleRound);
    assertThat(registry.resolve("round", List.of(Long.class)).getMethod(POS)).isEqualTo(longRound);
  }

  @Test
  void matchParametersAcceptsConsistentTypeVariables() throws Exception {
    Method method = Samples.class.getMethod("sameType", Comparable.class, Comparable.class);

    assertThat(NativeFunctionRegistry.matchParameters(method, String.class, String.class)).isTrue();
    assertThat(NativeFunctionRegistry.matchParameters(method, Long.class, Long.class)).isTrue();
    assertThat(NativeFunctionRegistry.matchParameters(method, Long.class, String.class)).isFalse();
    assertThat(NativeFunctionRegistry.matchParameters(method, String.class)).isFalse();
  }

  @Test
  void resolveOrNullReturnsNullInsteadOfThrowing() {
    assertThat(registry.resolveOrNull("missing", List.of(String.class))).isNull();
  }

  @Test
  void resolveFallsBackToExactMatchWhenSeveralSoftMatches() throws Exception {
    Method takesNumber = Samples.class.getMethod("takesNumber", Number.class);
    Method takesLong = Samples.class.getMethod("takesLong", Long.class);
    registry.register("f", takesNumber);
    registry.register("f", takesLong);

    // Long is soft-assignable to both Number and Long → exact identity picks Long.
    assertThat(registry.resolve("f", List.of(Long.class)).getMethod(POS)).isEqualTo(takesLong);
  }

  @Test
  void registerAllMergesProviderBindings() throws Exception {
    Method trim = Samples.class.getMethod("left", String.class);
    registry.registerAll(Map.of("trim", List.of(trim)));

    assertThat(registry.resolve("trim", List.of(String.class)).getMethod(POS)).isEqualTo(trim);
  }

  public static final class Samples {
    public static Long roundLong(Long value) {
      return value;
    }

    public static Long roundLongAlt(Long value) {
      return value;
    }

    public static Double roundDouble(Double value) {
      return value;
    }

    public static String left(String value) {
      return value;
    }

    public static String right(String value) {
      return value;
    }

    public static Number takesNumber(Number value) {
      return value;
    }

    public static Long takesLong(Long value) {
      return value;
    }

    public static <T extends Comparable<T>> boolean sameType(T left, T right) {
      return true;
    }
  }
}
