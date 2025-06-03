package fr.insee.vtl.engine.visitors.expression.functions;

import static fr.insee.vtl.engine.VtlScriptEngineTest.atPosition;
import static fr.insee.vtl.model.Dataset.Role;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JoinFunctionsTest {

  private final InMemoryDataset ds1 =
      new InMemoryDataset(
          List.of(
              List.of("a", 1L, 1L),
              List.of("a", 2L, 2L),
              List.of("b", 1L, 3L),
              List.of("b", 2L, 4L),
              List.of("c", 1L, 5L),
              List.of("c", 2L, 6L)),
          List.of(
              new Structured.Component("id1", String.class, Role.IDENTIFIER),
              new Structured.Component("id2", Long.class, Role.IDENTIFIER),
              new Structured.Component("m1", Long.class, Role.MEASURE)));
  private final InMemoryDataset ds2 =
      new InMemoryDataset(
          List.of(
              List.of("a", 1L, 7L),
              List.of("a", 2L, 8L),
              List.of("b", 1L, 9L),
              List.of("b", 2L, 10L),
              List.of("d", 3L, 11L),
              List.of("d", 4L, 12L)),
          List.of(
              new Structured.Component("id1", String.class, Role.IDENTIFIER),
              new Structured.Component("id2", Long.class, Role.IDENTIFIER),
              new Structured.Component("m2", Long.class, Role.MEASURE)));
  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  public void testJoinWithExpressionFails() {
    engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    engine.getContext().setAttribute("ds2", ds2, ScriptContext.ENGINE_SCOPE);

    assertThatThrownBy(
            () -> engine.eval("result := left_join(ds1[filter true], ds2[filter true]);"))
        .is(atPosition(0, 20, 36))
        .hasMessage("cannot use expression without alias in join clause");
    assertThatThrownBy(() -> engine.eval("result := inner_join(ds1, ds2[filter true]);"))
        .is(atPosition(0, 26, 42))
        .hasMessage("cannot use expression without alias in join clause");
    assertThatThrownBy(() -> engine.eval("result := cross_join(ds1[filter true], ds2);"))
        .is(atPosition(0, 21, 37))
        .hasMessage("cannot use expression without alias in join clause");
    assertThatThrownBy(
            () -> engine.eval("result := full_join(ds1[filter true], ds2[filter true]);"))
        .is(atPosition(0, 20, 36))
        .hasMessage("cannot use expression without alias in join clause");
  }

  @Test
  public void testLeftJoinWithUsing() throws ScriptException {

    engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    engine.getContext().setAttribute("ds2", ds2, ScriptContext.ENGINE_SCOPE);
    engine.eval("result := left_join(ds1, ds2 as aliasDs using id1);");

    var result = (Dataset) engine.getContext().getAttribute("result");
    assertThat(result.getColumnNames()).containsExactlyInAnyOrder("id1", "id2", "m1", "m2");
    assertThat(result.getDataAsList())
        .containsExactlyInAnyOrder(
            Arrays.asList("a", 1L, 1L, 7L),
            Arrays.asList("a", 1L, 2L, 8L),
            Arrays.asList("a", 2L, 1L, 7L),
            Arrays.asList("a", 2L, 2L, 8L),
            Arrays.asList("b", 3L, 1L, 9L),
            Arrays.asList("b", 3L, 2L, 10L),
            Arrays.asList("b", 4L, 1L, 9L),
            Arrays.asList("b", 4L, 2L, 10L),
            Arrays.asList("c", 5L, null, null),
            Arrays.asList("c", 6L, null, null));
  }

  @Test
  public void testLeftJoinWithDifferentIdentifiers() throws ScriptException {
    InMemoryDataset dataset1 =
        new InMemoryDataset(
            List.of(List.of(1L, 1L), List.of(1L, 2L), List.of(2L, 1L)),
            List.of(
                new Structured.Component("Id_1", Long.class, Role.IDENTIFIER),
                new Structured.Component("Id_2", Long.class, Role.IDENTIFIER)));
    InMemoryDataset dataset2 =
        new InMemoryDataset(
            List.of(List.of(1L, "X"), List.of(2L, "Y")),
            List.of(
                new Structured.Component("Id_2", Long.class, Role.IDENTIFIER),
                new Structured.Component("Me_1", String.class, Role.MEASURE)));

    ScriptContext context = engine.getContext();
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_1", dataset1);
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_2", dataset2);

    engine.eval("result := left_join(ds_1, ds_2 using Id_2);");
    Dataset result = ((Dataset) engine.getContext().getAttribute("result"));
    assertThat(result.getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("Id_1", 1L, "Id_2", 1L, "Me_1", "X"),
            Map.of("Id_1", 1L, "Id_2", 2L, "Me_1", "Y"),
            Map.of("Id_1", 2L, "Id_2", 1L, "Me_1", "X"));
    assertThat(result.getDataStructure().get("Id_1").getRole()).isEqualTo(Role.IDENTIFIER);
    assertThat(result.getDataStructure().get("Id_2").getRole()).isEqualTo(Role.IDENTIFIER);
    assertThat(result.getDataStructure().get("Me_1").getRole()).isEqualTo(Role.MEASURE);

    assertThatThrownBy(
            () ->
                engine.eval(
                    """
                ds_1 := ds_1[calc measure Id_2 := Id_2];
                result := left_join(ds_1, ds_2 using Id_2);\
                """))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessage("using component Id_2 has to be an identifier");
  }

  @Test
  public void testLeftJoinWithDouble() throws ScriptException {
    InMemoryDataset dataset1 =
        new InMemoryDataset(
            List.of(List.of("Hadrien", 10L), List.of("Nico", 11L)),
            List.of(
                new Structured.Component("name", String.class, Role.IDENTIFIER),
                new Structured.Component("age", Long.class, Role.MEASURE)));
    InMemoryDataset dataset2 =
        new InMemoryDataset(
            List.of(List.of("Hadrien", 20L), List.of("Nico", 22L)),
            List.of(
                new Structured.Component("name", String.class, Role.IDENTIFIER),
                new Structured.Component("age", Long.class, Role.MEASURE)));

    ScriptContext context = engine.getContext();
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_1", dataset1);
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_2", dataset2);

    assertThatThrownBy(() -> engine.eval("result := left_join(ds_1, ds_2);"))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessage(
            "It is not allowed that two or more Components in the virtual Data Set have the same name (age)");

    engine.eval("result := left_join(ds_1 as ds1, ds_2 as ds2);");
    assertThat(((Dataset) engine.getContext().getAttribute("result")).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("name", "Hadrien", "age", 20L), Map.of("name", "Nico", "age", 22L));
  }

  @Test
  public void testLeftJoinMixedStructure() throws ScriptException {

    InMemoryDataset dataset1 =
        new InMemoryDataset(
            List.of(
                Map.of("id", "K001", "measure1", 1L, "measure2", 9L, "color", "yellow"),
                Map.of("id", "K002", "measure1", 2L, "measure2", 8L, "color", "blue")),
            Map.of(
                "id",
                String.class,
                "measure1",
                Long.class,
                "measure2",
                Long.class,
                "color",
                String.class),
            Map.of(
                "id",
                Role.IDENTIFIER,
                "measure1",
                Role.MEASURE,
                "measure2",
                Role.MEASURE,
                "color",
                Role.MEASURE));

    InMemoryDataset dataset2 =
        new InMemoryDataset(
            List.of(
                Map.of(
                    "id", "K001", "intermezzo", 0L, "measure2", 11L, "measure1", 19L, "stale", ""),
                Map.of(
                    "id", "K003", "intermezzo", 0L, "measure2", 7L, "measure1", 3L, "stale", "")),
            Map.of(
                "id",
                String.class,
                "intermezzo",
                Long.class,
                "measure2",
                Long.class,
                "measure1",
                Long.class,
                "stale",
                String.class),
            Map.of(
                "id",
                Role.IDENTIFIER,
                "intermezzo",
                Role.MEASURE,
                "measure2",
                Role.MEASURE,
                "measure1",
                Role.MEASURE,
                "stale",
                Role.MEASURE));

    engine.getContext().setAttribute("ds1", dataset1, ScriptContext.ENGINE_SCOPE);
    engine.getContext().setAttribute("ds2", dataset2, ScriptContext.ENGINE_SCOPE);

    engine.eval(
        "unionData := union(ds1[keep id, measure1, measure2], ds2[keep id, measure1, measure2]);");
    engine.eval("ds1_keep := ds1[keep id, color];");
    engine.eval("joinData := left_join(unionData, ds1_keep);");

    Dataset joinData = (Dataset) engine.getBindings(ScriptContext.ENGINE_SCOPE).get("joinData");

    // Build Map with null value
    Map<String, Object> thirdLine =
        new LinkedHashMap<>() {
          {
            put("id", "K003");
            put("measure1", 3L);
            put("measure2", 7L);
            put("color", null);
          }
        };

    assertThat(joinData.getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "K001", "measure1", 1L, "measure2", 9L, "color", "yellow"),
            Map.of("id", "K002", "measure1", 2L, "measure2", 8L, "color", "blue"),
            thirdLine);
  }

  @Test
  public void testInnerJoin() throws ScriptException {

    engine.getContext().setAttribute("ds_1", ds1, ScriptContext.ENGINE_SCOPE);
    engine.getContext().setAttribute("ds_2", ds2, ScriptContext.ENGINE_SCOPE);
    engine.eval("result := inner_join(ds_1[keep id1, id2, m1] as ds1, ds_2 as ds2);");

    var result = (Dataset) engine.getContext().getAttribute("result");
    assertThat(result.getColumnNames()).containsExactlyInAnyOrder("id1", "id2", "m1", "m2");
    assertThat(result.getDataAsList())
        .containsExactlyInAnyOrder(
            Arrays.asList("a", 1L, 1L, 7L),
            Arrays.asList("a", 2L, 2L, 8L),
            Arrays.asList("b", 1L, 3L, 9L),
            Arrays.asList("b", 2L, 4L, 10L));

    assertThatThrownBy(
            () ->
                engine.eval("ds_3 := ds_2[rename m2 to m1];" + "result := inner_join(ds_1, ds_3);"))
        .isInstanceOf(InvalidArgumentException.class)
        .hasMessage(
            "It is not allowed that two or more Components in the virtual Data Set have the same name (m1)");

    engine.eval("result := inner_join(ds_1 as ds1, ds_2 as ds2 using id1);");

    result = (Dataset) engine.getContext().getAttribute("result");
    assertThat(result.getColumnNames()).containsExactlyInAnyOrder("id1", "m1", "id2", "m2");
    assertThat(result.getDataAsList())
        .containsExactlyInAnyOrder(
            Arrays.asList("a", 1L, 1L, 7L),
            Arrays.asList("a", 1L, 2L, 8L),
            Arrays.asList("a", 2L, 1L, 7L),
            Arrays.asList("a", 2L, 2L, 8L),
            Arrays.asList("b", 3L, 1L, 9L),
            Arrays.asList("b", 3L, 2L, 10L),
            Arrays.asList("b", 4L, 1L, 9L),
            Arrays.asList("b", 4L, 2L, 10L));
  }

  @Test
  public void testFullJoin() throws ScriptException {
    ScriptContext context = engine.getContext();

    var ds1 =
        new InMemoryDataset(
            List.of(
                new Structured.Component("id", String.class, Role.IDENTIFIER),
                new Structured.Component("m1", Long.class, Role.MEASURE)),
            Arrays.asList("b", 1L),
            Arrays.asList("c", 2L),
            Arrays.asList("d", 3L));

    var ds2 =
        new InMemoryDataset(
            List.of(
                new Structured.Component("id", String.class, Role.IDENTIFIER),
                new Structured.Component("m1", Long.class, Role.MEASURE)),
            Arrays.asList("a", 4L),
            Arrays.asList("b", 5L),
            Arrays.asList("c", 6L));

    var ds3 =
        new InMemoryDataset(
            List.of(
                new Structured.Component("id", String.class, Role.IDENTIFIER),
                new Structured.Component("m1", Long.class, Role.MEASURE)),
            Arrays.asList("a", 7L),
            Arrays.asList("d", 8L));

    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_1", ds1);
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_2", ds2);
    context.getBindings(ScriptContext.ENGINE_SCOPE).put("ds_3", ds3);

    engine.eval("result := full_join(ds_1 as ds1, ds_2 as ds2, ds_3 as ds3);");

    var result = (Dataset) context.getAttribute("result");

    assertThat(result.getDataStructure().values())
        .containsExactly(
            new Structured.Component("id", String.class, Role.IDENTIFIER),
            new Structured.Component("m1", Long.class, Role.MEASURE));

    assertThat(result.getDataAsList())
        .containsExactlyInAnyOrder(
            Arrays.asList("d", 8L),
            Arrays.asList("c", null),
            Arrays.asList("b", null),
            Arrays.asList("a", 7L));
  }

  @Test
  public void testCrossJoin() throws ScriptException {

    InMemoryDataset ds3 =
        new InMemoryDataset(
            List.of(
                List.of("a", 1L, 1L),
                List.of("a", 2L, 2L),
                List.of("a", 3L, 3L),
                List.of("b", 1L, 3L),
                List.of("b", 2L, 4L)),
            List.of(
                new Structured.Component("id1", String.class, Role.IDENTIFIER),
                new Structured.Component("id2", Long.class, Role.IDENTIFIER),
                new Structured.Component("m1", Long.class, Role.MEASURE)));

    InMemoryDataset ds4 =
        new InMemoryDataset(
            List.of(List.of("a", 1L, 7L), List.of("a", 2L, 8L)),
            List.of(
                new Structured.Component("id1", String.class, Role.IDENTIFIER),
                new Structured.Component("id2", Long.class, Role.IDENTIFIER),
                new Structured.Component("m2", Long.class, Role.MEASURE)));

    engine.getContext().setAttribute("ds_3", ds3, ScriptContext.ENGINE_SCOPE);
    engine.getContext().setAttribute("ds_4", ds4, ScriptContext.ENGINE_SCOPE);
    engine.eval("result := cross_join(ds_3 as ds3, ds_4 as ds4);");

    var result = (Dataset) engine.getContext().getAttribute("result");
    assertThat(result.getColumnNames())
        .containsExactly("ds3#id1", "ds3#id2", "ds4#id1", "ds4#id2", "m1", "m2");
    assertThat(result.getDataAsList())
        .containsExactlyInAnyOrder(
            Arrays.asList("a", 1L, "a", 1L, 1L, 7L),
            Arrays.asList("a", 1L, "a", 2L, 1L, 8L),
            Arrays.asList("a", 2L, "a", 1L, 2L, 7L),
            Arrays.asList("a", 2L, "a", 2L, 2L, 8L),
            Arrays.asList("a", 3L, "a", 1L, 3L, 7L),
            Arrays.asList("a", 3L, "a", 2L, 3L, 8L),
            Arrays.asList("b", 1L, "a", 1L, 3L, 7L),
            Arrays.asList("b", 1L, "a", 2L, 3L, 8L),
            Arrays.asList("b", 2L, "a", 1L, 4L, 7L),
            Arrays.asList("b", 2L, "a", 2L, 4L, 8L));
  }
}
