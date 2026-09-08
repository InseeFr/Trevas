package fr.insee.vtl.engine.visitors.expression.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * {@code exists_in(op1, op2, retain)}: identifiers of op1 plus {@code bool_var}; match on common
 * identifier combinations.
 */
class ExistsInTest {

  private ScriptEngine engine;

  @BeforeEach
  void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  void retainAllMarksMembershipOnCommonIdentifiers() throws ScriptException {
    bindSameIdDatasets();

    engine.eval("res := exists_in(ds1, ds2, all);");

    assertThat(((Dataset) engine.get("res")).getDataAsMap())
        .containsExactlyInAnyOrder(
            row("B", "Total", true),
            row("G", "Total", false),
            row("S", "Total", true),
            row("M", "Total", false),
            row("F", "Total", true),
            row("W", "Total", true));
    assertThat(((Dataset) engine.get("res")).getDataStructure().get("bool_var").getType())
        .isEqualTo(Boolean.class);
  }

  @Test
  void omittedRetainDefaultsToAll() throws ScriptException {
    bindSameIdDatasets();

    engine.eval("res := exists_in(ds1, ds2);");

    assertThat(((Dataset) engine.get("res")).getDataAsMap()).hasSize(6);
  }

  @Test
  void retainTrueKeepsOnlyMatches() throws ScriptException {
    bindSameIdDatasets();

    engine.eval("res := exists_in(ds1, ds2, true);");

    assertThat(((Dataset) engine.get("res")).getDataAsMap())
        .containsExactlyInAnyOrder(
            row("B", "Total", true),
            row("S", "Total", true),
            row("F", "Total", true),
            row("W", "Total", true));
  }

  @Test
  void retainFalseKeepsOnlyNonMatches() throws ScriptException {
    bindSameIdDatasets();

    engine.eval("res := exists_in(ds1, ds2, false);");

    assertThat(((Dataset) engine.get("res")).getDataAsMap())
        .containsExactlyInAnyOrder(row("G", "Total", false), row("M", "Total", false));
  }

  @Test
  void matchesOnCommonIdentifiersWhenRightHasFewerIds() throws ScriptException {
    bind(
        "ds1",
        List.of(
            Map.of("Id_1", "A", "Id_2", "x", "Me_1", 1L),
            Map.of("Id_1", "A", "Id_2", "y", "Me_1", 2L),
            Map.of("Id_1", "B", "Id_2", "x", "Me_1", 3L)),
        Map.of(
            "Id_1", String.class,
            "Id_2", String.class,
            "Me_1", Long.class),
        Map.of(
            "Id_1", Dataset.Role.IDENTIFIER,
            "Id_2", Dataset.Role.IDENTIFIER,
            "Me_1", Dataset.Role.MEASURE));
    bind(
        "ds2",
        List.of(Map.of("Id_1", "A", "Me_2", 10L)),
        Map.of("Id_1", String.class, "Me_2", Long.class),
        Map.of("Id_1", Dataset.Role.IDENTIFIER, "Me_2", Dataset.Role.MEASURE));

    engine.eval("res := exists_in(ds1, ds2);");

    assertThat(((Dataset) engine.get("res")).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("Id_1", "A", "Id_2", "x", "bool_var", true),
            Map.of("Id_1", "A", "Id_2", "y", "bool_var", true),
            Map.of("Id_1", "B", "Id_2", "x", "bool_var", false));
  }

  @Test
  void rejectsIncompatibleIdentifierSets() {
    bind(
        "ds1",
        List.of(Map.of("Id_1", "A", "Me_1", 1L)),
        Map.of("Id_1", String.class, "Me_1", Long.class),
        Map.of("Id_1", Dataset.Role.IDENTIFIER, "Me_1", Dataset.Role.MEASURE));
    bind(
        "ds2",
        List.of(Map.of("Id_2", "A", "Me_2", 1L)),
        Map.of("Id_2", String.class, "Me_2", Long.class),
        Map.of("Id_2", Dataset.Role.IDENTIFIER, "Me_2", Dataset.Role.MEASURE));

    assertThatThrownBy(() -> engine.eval("res := exists_in(ds1, ds2);"))
        .hasMessageContaining("identifier");
  }

  private void bindSameIdDatasets() {
    Map<String, Class<?>> types =
        Map.of(
            "Id_2", String.class,
            "Id_4", String.class,
            "Me_1", Long.class);
    Map<String, Dataset.Role> roles =
        Map.of(
            "Id_2", Dataset.Role.IDENTIFIER,
            "Id_4", Dataset.Role.IDENTIFIER,
            "Me_1", Dataset.Role.MEASURE);

    bind(
        "ds1",
        List.of(
            Map.of("Id_2", "B", "Id_4", "Total", "Me_1", 1L),
            Map.of("Id_2", "G", "Id_4", "Total", "Me_1", 2L),
            Map.of("Id_2", "S", "Id_4", "Total", "Me_1", 3L),
            Map.of("Id_2", "M", "Id_4", "Total", "Me_1", 4L),
            Map.of("Id_2", "F", "Id_4", "Total", "Me_1", 5L),
            Map.of("Id_2", "W", "Id_4", "Total", "Me_1", 6L)),
        types,
        roles);
    bind(
        "ds2",
        List.of(
            Map.of("Id_2", "B", "Id_4", "Total", "Me_1", 0L),
            Map.of("Id_2", "G", "Id_4", "M", "Me_1", 0L),
            Map.of("Id_2", "S", "Id_4", "Total", "Me_1", 0L),
            Map.of("Id_2", "M", "Id_4", "M", "Me_1", 0L),
            Map.of("Id_2", "F", "Id_4", "Total", "Me_1", 0L),
            Map.of("Id_2", "W", "Id_4", "Total", "Me_1", 0L)),
        types,
        roles);
  }

  private static Map<String, Object> row(String id2, String id4, boolean exists) {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("Id_2", id2);
    row.put("Id_4", id4);
    row.put("bool_var", exists);
    return row;
  }

  private void bind(
      String name,
      List<Map<String, Object>> rows,
      Map<String, Class<?>> types,
      Map<String, Dataset.Role> roles) {
    engine
        .getContext()
        .setAttribute(name, new InMemoryDataset(rows, types, roles), ScriptContext.ENGINE_SCOPE);
  }
}
