package fr.insee.vtl.engine.visitors.expression.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * VTL set operators other than {@code union}: {@code setdiff}, {@code intersect}, {@code symdiff}.
 * Comparison is by identifier values; result rows keep the leftmost datapoint when keys collide.
 */
class SetFunctionsTest {

  private static final Map<String, Class<?>> TYPES =
      Map.of("Id_1", String.class, "Me_1", Long.class);
  private static final Map<String, Dataset.Role> ROLES =
      Map.of("Id_1", Dataset.Role.IDENTIFIER, "Me_1", Dataset.Role.MEASURE);

  private ScriptEngine engine;

  @BeforeEach
  void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  void setdiffKeepsRowsWhoseIdsAreAbsentFromRight() throws ScriptException {
    bind(
        "ds1",
        List.of(
            Map.of("Id_1", "A", "Me_1", 1L),
            Map.of("Id_1", "B", "Me_1", 2L),
            Map.of("Id_1", "C", "Me_1", 3L)));
    bind("ds2", List.of(Map.of("Id_1", "B", "Me_1", 20L), Map.of("Id_1", "C", "Me_1", 30L)));

    engine.eval("res := setdiff(ds1, ds2);");

    assertThat(((Dataset) engine.get("res")).getDataAsMap())
        .containsExactly(Map.of("Id_1", "A", "Me_1", 1L));
  }

  @Test
  void intersectKeepsLeftmostRowForSharedIds() throws ScriptException {
    bind("ds1", List.of(Map.of("Id_1", "G", "Me_1", 2L), Map.of("Id_1", "B", "Me_1", 1L)));
    bind("ds2", List.of(Map.of("Id_1", "G", "Me_1", 99L), Map.of("Id_1", "M", "Me_1", 40L)));

    engine.eval("res := intersect(ds1, ds2);");

    assertThat(((Dataset) engine.get("res")).getDataAsMap())
        .containsExactly(Map.of("Id_1", "G", "Me_1", 2L));
  }

  @Test
  void intersectThreeOperandsRequiresPresenceInAll() throws ScriptException {
    bind(
        "ds1",
        List.of(
            Map.of("Id_1", "A", "Me_1", 1L),
            Map.of("Id_1", "B", "Me_1", 2L),
            Map.of("Id_1", "C", "Me_1", 3L)));
    bind("ds2", List.of(Map.of("Id_1", "A", "Me_1", 10L), Map.of("Id_1", "B", "Me_1", 20L)));
    bind("ds3", List.of(Map.of("Id_1", "A", "Me_1", 100L), Map.of("Id_1", "C", "Me_1", 300L)));

    engine.eval("res := intersect(ds1, ds2, ds3);");

    assertThat(((Dataset) engine.get("res")).getDataAsMap())
        .containsExactly(Map.of("Id_1", "A", "Me_1", 1L));
  }

  @Test
  void symdiffIsExclusiveOrOnIdentifierKeys() throws ScriptException {
    bind("ds1", List.of(Map.of("Id_1", "A", "Me_1", 1L), Map.of("Id_1", "B", "Me_1", 2L)));
    bind("ds2", List.of(Map.of("Id_1", "B", "Me_1", 20L), Map.of("Id_1", "C", "Me_1", 3L)));

    engine.eval("res := symdiff(ds1, ds2);");

    assertThat(((Dataset) engine.get("res")).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("Id_1", "A", "Me_1", 1L), Map.of("Id_1", "C", "Me_1", 3L));
  }

  @Test
  void setOperatorsRejectIncompatibleStructures() {
    engine
        .getContext()
        .setAttribute(
            "ds1",
            new InMemoryDataset(
                List.of(),
                Map.of("Id_1", String.class, "Me_1", Long.class),
                Map.of("Id_1", Dataset.Role.IDENTIFIER, "Me_1", Dataset.Role.MEASURE)),
            ScriptContext.ENGINE_SCOPE);
    engine
        .getContext()
        .setAttribute(
            "ds2",
            new InMemoryDataset(
                List.of(),
                Map.of("Id_1", String.class, "Me_2", Long.class),
                Map.of("Id_1", Dataset.Role.IDENTIFIER, "Me_2", Dataset.Role.MEASURE)),
            ScriptContext.ENGINE_SCOPE);

    assertThatThrownBy(() -> engine.eval("res := setdiff(ds1, ds2);"))
        .hasMessageContaining("incompatible");
    assertThatThrownBy(() -> engine.eval("res := intersect(ds1, ds2);"))
        .hasMessageContaining("incompatible");
    assertThatThrownBy(() -> engine.eval("res := symdiff(ds1, ds2);"))
        .hasMessageContaining("incompatible");
  }

  private void bind(String name, List<Map<String, Object>> rows) {
    engine
        .getContext()
        .setAttribute(name, new InMemoryDataset(rows, TYPES, ROLES), ScriptContext.ENGINE_SCOPE);
  }
}
