package fr.insee.vtl.engine.utils.dag;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.samples.DatasetSamples;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.exceptions.VtlMultiErrorScriptException;
import fr.insee.vtl.model.exceptions.VtlMultiStatementScriptException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import java.util.*;
import java.util.stream.Stream;
import javax.script.ScriptContext;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DagTest {
  public static final String VTL_ENGINE_USE_DAG = "$vtl.engine.use_dag";

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
              new Structured.Component("id1", String.class, Dataset.Role.IDENTIFIER),
              new Structured.Component("id2", Long.class, Dataset.Role.IDENTIFIER),
              new Structured.Component("m1", Long.class, Dataset.Role.MEASURE)));

  private final InMemoryDataset ds2 =
      new InMemoryDataset(
          List.of(
              List.of("a", 3L, 7L),
              List.of("a", 4L, 8L),
              List.of("b", 3L, 9L),
              List.of("b", 4L, 10L),
              List.of("d", 3L, 11L),
              List.of("d", 4L, 12L)),
          List.of(
              new Structured.Component("id1", String.class, Dataset.Role.IDENTIFIER),
              new Structured.Component("id2", Long.class, Dataset.Role.IDENTIFIER),
              new Structured.Component("m2", Long.class, Dataset.Role.MEASURE)));

  private final InMemoryDataset ds3 =
      new InMemoryDataset(
          List.of(
              List.of("a", 1L, 7L),
              List.of("a", 2L, 8L),
              List.of("b", 1L, 9L),
              List.of("b", 2L, 10L),
              List.of("d", 1L, 11L),
              List.of("d", 2L, 12L)),
          List.of(
              new Structured.Component("id1", String.class, Dataset.Role.IDENTIFIER),
              new Structured.Component("id2", Long.class, Dataset.Role.IDENTIFIER),
              new Structured.Component("m2", Long.class, Dataset.Role.MEASURE)));

  private VtlScriptEngine engine;

  public static Stream<Arguments> shuffledListSource() {
    return permutations(List.of("b := a", "c := b", "d := c", "e := d + c"))
        .map(DagTest::buildScriptFromStatements)
        .map(Arguments::of);
  }

  private static String buildScriptFromStatements(final List<String> statements) {
    return String.join(";", statements) + ";";
  }

  public static <E> Stream<List<E>> permutations(List<E> input) {
    List<List<E>> results = new ArrayList<>();
    List<E> arr = new ArrayList<>(input);
    backtrack(arr, 0, results);
    return results.stream();
  }

  private static <E> void backtrack(List<E> arr, int index, List<List<E>> results) {
    if (index == arr.size() - 1) {
      results.add(new ArrayList<>(arr));
      return;
    }
    for (int i = index; i < arr.size(); i++) {
      Collections.swap(arr, index, i);
      backtrack(arr, index + 1, results);
      Collections.swap(arr, index, i);
    }
  }

  private static Positioned.Position getPositionOfStatementInScript(
      String statement, String script) {

    int startOffset = script.indexOf(statement);
    if (startOffset < 0) {
      throw new IllegalArgumentException("Statement not found in script");
    }
    int endOffset = startOffset + statement.length(); // exclusive char index

    // compute start line/column (0-based)
    int startLine = 0;
    int startColumn = 0;
    for (int i = 0; i < startOffset; i++) {
      char c = script.charAt(i);
      if (c == '\r') {
        // handle CRLF as single newline
        if (i + 1 < script.length() && script.charAt(i + 1) == '\n') {
          i++;
        }
        startLine++;
        startColumn = 0;
      } else if (c == '\n') {
        startLine++;
        startColumn = 0;
      } else {
        startColumn++;
      }
    }

    // compute end line/column (0-based, endColumn is exclusive)
    int endLine = startLine;
    int endColumn = startColumn;
    for (int i = startOffset; i < endOffset; i++) {
      char c = script.charAt(i);
      if (c == '\r') {
        if (i + 1 < script.length() && script.charAt(i + 1) == '\n') {
          i++;
        }
        endLine++;
        endColumn = 0;
      } else if (c == '\n') {
        endLine++;
        endColumn = 0;
      } else {
        endColumn++;
      }
    }

    return new Positioned.Position(startLine, endLine, startColumn, endColumn);
  }

  @BeforeEach
  void setUp() {
    engine = (VtlScriptEngine) new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  void testNoDagConfig() {
    engine.put(VTL_ENGINE_USE_DAG, "false");
    assertThat(engine.isUseDag()).isFalse();
  }

  @Test
  void testUseDagConfig() {
    assertThat(engine.isUseDag()).isTrue();
  }

  @Test
  void testNoReorderingWhenDAGDeactivated() {
    engine.put(VTL_ENGINE_USE_DAG, "false");
    ScriptContext context = engine.getContext();
    context.setAttribute("a", 1L, ScriptContext.ENGINE_SCOPE);
    assertThatExceptionOfType(UndefinedVariableException.class)
        .isThrownBy(() -> engine.eval("b := a; d := c; c := b;"))
        .withMessage("undefined variable c");
  }

  @Test
  void testDagSimpleExampleNoReordering() throws ScriptException {
    ScriptContext context = engine.getContext();
    context.setAttribute("a", 1L, ScriptContext.ENGINE_SCOPE);
    engine.eval("tmp := a; b := tmp;");
    assertThat(context.getBindings(ScriptContext.ENGINE_SCOPE)).containsKey("b");
    assertThat((Long) context.getAttribute("b")).isEqualTo(1L);
  }

  @Test
  void testDagSimpleExampleWithReordering() throws ScriptException {
    ScriptContext context = engine.getContext();
    context.setAttribute("a", 1L, ScriptContext.ENGINE_SCOPE);
    engine.eval("b := a; d := c; c := b;");
    assertThat(context.getBindings(ScriptContext.ENGINE_SCOPE)).containsKey("b");
    assertThat((Long) context.getAttribute("b")).isEqualTo(1L);
  }

  @Test
  void testDagCycle() {
    final String script =
        """
                        e := a;
                        b := a;
                        c := b;
                        a := c;
                        f := a;""";

    final Positioned.Position mainPosition = getPositionOfStatementInScript("a := c", script);
    final List<Positioned.Position> otherPositions =
        List.of(
            getPositionOfStatementInScript("b := a", script),
            getPositionOfStatementInScript("c := b", script));

    assertThatExceptionOfType(VtlMultiStatementScriptException.class)
        .isThrownBy(() -> engine.eval(script))
        .withMessage("assignment creates a cycle: [a <- c <- b <- a]")
        .satisfies(
            ex -> {
              assertThat(ex.getPosition()).isEqualTo(mainPosition);
              assertThat(ex.getOtherPositions())
                  .containsExactlyInAnyOrderElementsOf(otherPositions);
              assertThat(ex.getAllPositions())
                  .containsExactlyInAnyOrderElementsOf(
                      Stream.of(List.of(mainPosition), otherPositions)
                          .flatMap(Collection::stream)
                          .toList());
            });
  }

  @Test
  void testMultipleCycles() {
    final String script =
        """
                        h := g;
                        i := join(h, input_ds);
                        g := i;
                        e := a;
                        b := a;
                        c := b;
                        a := c;
                        f := a;""";

    final Positioned.Position mainExceptionMainPosition =
        getPositionOfStatementInScript("g := i", script);
    final List<Positioned.Position> mainExceptionOtherPositions =
        List.of(
            getPositionOfStatementInScript("h := g", script),
            getPositionOfStatementInScript("i := join(h, input_ds)", script));
    final Positioned.Position otherCycleExceptionMainPosition =
        getPositionOfStatementInScript("a := c", script);
    final List<Positioned.Position> otherCycleExceptionOtherPositions =
        List.of(
            getPositionOfStatementInScript("b := a", script),
            getPositionOfStatementInScript("c := b", script));
    assertThatExceptionOfType(VtlMultiErrorScriptException.class)
        .isThrownBy(() -> engine.eval(script))
        .withMessage(
            "fr.insee.vtl.model.exceptions.VtlMultiStatementScriptException: assignment creates a cycle: [g <- i <- h <- g]")
        .satisfies(
            ex -> {
              assertThat(ex.getPosition()).isEqualTo(mainExceptionMainPosition);
              assertThat(ex.getAllPositions())
                  .containsExactlyInAnyOrderElementsOf(
                      Stream.of(
                              List.of(mainExceptionMainPosition, otherCycleExceptionMainPosition),
                              mainExceptionOtherPositions,
                              otherCycleExceptionOtherPositions)
                          .flatMap(Collection::stream)
                          .toList());

              Throwable cause = ex.getCause();
              assertThat(cause).isInstanceOf(VtlMultiStatementScriptException.class);

              VtlMultiStatementScriptException vtlCause = (VtlMultiStatementScriptException) cause;

              assertThat(vtlCause.getOtherPositions())
                  .containsExactlyInAnyOrderElementsOf(mainExceptionOtherPositions);

              // Other cycle
              Collection<? extends VtlScriptException> otherCycleExs = ex.getOtherExceptions();
              assertThat(otherCycleExs).size().isEqualTo(1);

              assertThat((VtlMultiStatementScriptException) otherCycleExs.iterator().next())
                  .hasMessage("assignment creates a cycle: [a <- c <- b <- a]")
                  .satisfies(
                      otherCycleEx -> {
                        assertThat(otherCycleEx.getPosition())
                            .isEqualTo(otherCycleExceptionMainPosition);

                        assertThat(otherCycleEx.getOtherPositions())
                            .containsExactlyInAnyOrderElementsOf(otherCycleExceptionOtherPositions);
                      });
            });
  }

  @Test
  void testDagDoubleAssignment() {
    ScriptContext context = engine.getContext();
    context.setAttribute("a", 1L, ScriptContext.ENGINE_SCOPE);
    // Note that the double assignment is not detected while building the DAG but later during
    // execution
    assertThatThrownBy(() -> engine.eval("b := a; b := 1;"))
        .isInstanceOf(VtlScriptException.class)
        .hasMessage("Dataset b has already been assigned");
  }

  @Test
  void testDagMultipleAssignments() {
    Dataset ds = new InMemoryDataset(List.of());
    ScriptContext context = engine.getContext();
    context.setAttribute("ds", ds, ScriptContext.ENGINE_SCOPE);

    VtlScriptException exception =
        assertThrows(VtlScriptException.class, () -> engine.eval("ds1 := ds; ds1 <- ds;"));
    assertThat(exception).isInstanceOf(VtlScriptException.class);
    assertThat(exception.getMessage()).isEqualTo("Dataset ds1 has already been assigned");
    assertThat(exception.getPosition().startLine()).isZero();
    assertThat(exception.getPosition().startColumn()).isEqualTo(11);
  }

  @ParameterizedTest
  @MethodSource("shuffledListSource")
  void testDagShuffledList(final String script) throws ScriptException {
    // Script is permutation of statements ("b := a", "c := b", "d := c", "e := d + c")
    ScriptContext context = engine.getContext();
    context.setAttribute("a", 1L, ScriptContext.ENGINE_SCOPE);
    engine.eval(script);
    assertThat(context.getBindings(ScriptContext.ENGINE_SCOPE)).containsKey("e");
    assertThat((Long) context.getAttribute("e")).isEqualTo(2L);
  }

  @Test
  void testDagLeftJoin() throws ScriptException {
    engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    engine.getContext().setAttribute("ds2", ds2, ScriptContext.ENGINE_SCOPE);
    engine.eval("result := left_join(tmp1, tmp2 as aliasDs using id1); tmp1 := ds1; tmp2 := ds2;");

    Dataset result = (Dataset) engine.getContext().getAttribute("result");
    assertThat(result.getColumnNames()).containsExactlyInAnyOrder("id1", "id2", "m1", "m2");
    assertThat(result.getDataAsList())
        .containsExactlyInAnyOrder(
            Arrays.asList("a", 1L, 3L, 7L),
            Arrays.asList("a", 1L, 4L, 8L),
            Arrays.asList("a", 2L, 3L, 7L),
            Arrays.asList("a", 2L, 4L, 8L),
            Arrays.asList("b", 3L, 3L, 9L),
            Arrays.asList("b", 3L, 4L, 10L),
            Arrays.asList("b", 4L, 3L, 9L),
            Arrays.asList("b", 4L, 4L, 10L),
            Arrays.asList("c", 5L, null, null),
            Arrays.asList("c", 6L, null, null));
  }

  @Test
  void testDagInnerJoin() throws ScriptException {
    engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    engine.getContext().setAttribute("ds2", ds2, ScriptContext.ENGINE_SCOPE);
    engine.eval("result := inner_join(tmp1, tmp2 as aliasDs using id1); tmp1 := ds1; tmp2 := ds2;");

    var result = (Dataset) engine.getContext().getAttribute("result");
    assertThat(result.getColumnNames()).containsExactlyInAnyOrder("id1", "id2", "m1", "m2");
    assertThat(result.getDataAsList())
        .containsExactlyInAnyOrder(
            Arrays.asList("a", 1L, 3L, 7L),
            Arrays.asList("a", 1L, 4L, 8L),
            Arrays.asList("a", 2L, 3L, 7L),
            Arrays.asList("a", 2L, 4L, 8L),
            Arrays.asList("b", 3L, 3L, 9L),
            Arrays.asList("b", 3L, 4L, 10L),
            Arrays.asList("b", 4L, 3L, 9L),
            Arrays.asList("b", 4L, 4L, 10L));
  }

  @Test
  void testDagInnerJoinOnTwoCols() throws ScriptException {
    engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    engine.getContext().setAttribute("ds3", ds3, ScriptContext.ENGINE_SCOPE);
    engine.eval(
        "result := inner_join(tmp1, ds3 as aliasDs using id1, id2); tmp1 := ds1; tmp3 := ds3;");

    var result = (Dataset) engine.getContext().getAttribute("result");
    assertThat(result.getColumnNames()).containsExactlyInAnyOrder("id1", "id2", "m1", "m2");
    assertThat(result.getDataAsList())
        .containsExactlyInAnyOrder(
            Arrays.asList("a", 1L, 1L, 7L),
            Arrays.asList("a", 2L, 2L, 8L),
            Arrays.asList("b", 1L, 3L, 9L),
            Arrays.asList("b", 2L, 4L, 10L));
  }

  @Test
  void testDagInnerJoinWithFunctions() throws ScriptException {
    engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    engine.getContext().setAttribute("ds3", ds3, ScriptContext.ENGINE_SCOPE);
    engine.eval(
        "result := inner_join(tmp1, tmp3 using id1, id2 "
            + "filter 1>2); tmp3 := ds3; tmp1 := ds1;");

    var result = (Dataset) engine.getContext().getAttribute("result");
    assertThat(result.getColumnNames()).containsExactlyInAnyOrder("id1", "id2", "m1", "m2");
    assertThat(result.getDataAsList())
        .containsExactlyInAnyOrder(
            Arrays.asList("a", 1L, 1L, 7L),
            Arrays.asList("a", 2L, 2L, 8L),
            Arrays.asList("b", 1L, 3L, 9L),
            Arrays.asList("b", 2L, 4L, 10L));
  }

  @Test
  void testDagConcatFunction() throws ScriptException {
    engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    engine.eval("result := tmp1[calc m2:=id1||id1]; tmp1 := ds1;");

    var result = (Dataset) engine.getContext().getAttribute("result");
    assertThat(result.getColumnNames()).containsExactlyInAnyOrder("id1", "id2", "m1", "m2");
    assertThat(result.getDataAsList())
        .containsExactlyInAnyOrder(
            Arrays.asList("a", 1L, 1L, "aa"),
            Arrays.asList("a", 2L, 2L, "aa"),
            Arrays.asList("b", 1L, 3L, "bb"),
            Arrays.asList("b", 2L, 4L, "bb"),
            Arrays.asList("c", 1L, 5L, "cc"),
            Arrays.asList("c", 2L, 6L, "cc"));
  }

  @Test
  void testDagComponentProjection() throws ScriptException {
    engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    engine.eval("result := tmp1#m1; tmp1 := ds1;");

    var result = (Dataset) engine.getContext().getAttribute("result");
    assertThat(result.getColumnNames()).containsExactlyInAnyOrder("id1", "id2", "m1");
    assertThat(result.getDataAsList())
        .containsExactlyInAnyOrder(
            Arrays.asList("a", 1L, 1L),
            Arrays.asList("a", 2L, 2L),
            Arrays.asList("b", 1L, 3L),
            Arrays.asList("b", 2L, 4L),
            Arrays.asList("c", 1L, 5L),
            Arrays.asList("c", 2L, 6L));
  }

  @Test
  void testDagIfExpr() throws ScriptException {
    engine.getContext().setAttribute("ds_1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    engine.getContext().setAttribute("ds_2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    engine.eval(
        "res := if ds1 > ds2 then ds1 else ds2; "
            + "ds1 := ds_1[keep long1][rename long1 to bool_var]; "
            + "ds2 := ds_2[keep long1][rename long1 to bool_var];");
    var res = engine.getContext().getAttribute("res");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "bool_var", 150L),
            Map.of("id", "Nico", "bool_var", 20L),
            Map.of("id", "Franck", "bool_var", 100L));
    assertThat(((Dataset) res).getDataStructure().get("bool_var").getType()).isEqualTo(Long.class);
  }

  @Test
  void testDagCaseExpr() throws ScriptException {
    engine.getContext().setAttribute("ds_1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    engine.getContext().setAttribute("ds_2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    engine.eval(
        "res0 <- tmp0[calc c := case when long1 > 30 then \"ok\" else \"ko\"][drop long1]; "
            + "tmp0 := ds_1[keep long1];");
    Object res0 = engine.getContext().getAttribute("res0");
    assertThat(((Dataset) res0).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "c", "ko"),
            Map.of("id", "Hadrien", "c", "ko"),
            Map.of("id", "Nico", "c", "ko"),
            Map.of("id", "Franck", "c", "ok"));
    assertThat(((Dataset) res0).getDataStructure().get("c").getType()).isEqualTo(String.class);
    engine.eval(
        "res1 <- tmp1[calc c := case when long1 > 30 then 1 else 0][drop long1]; "
            + "tmp1 := ds_1[keep long1];");
    Object res1 = engine.getContext().getAttribute("res1");
    assertThat(((Dataset) res1).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "c", 0L),
            Map.of("id", "Hadrien", "c", 0L),
            Map.of("id", "Nico", "c", 0L),
            Map.of("id", "Franck", "c", 1L));
    assertThat(((Dataset) res1).getDataStructure().get("c").getType()).isEqualTo(Long.class);
    engine.eval(
        "tmp2_alt_ds1 := ds_1[keep long1][rename long1 to bool_var]; "
            + "res2 <- case when tmp2_alt_ds1 < 30 then tmp2_alt_ds1 else tmp2_alt_ds2; "
            + "tmp2_alt_ds2 := ds_2[keep long1][rename long1 to bool_var];");
    Object resDs = engine.getContext().getAttribute("res2");
    assertThat(((Dataset) resDs).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "bool_var", 10L),
            Map.of("id", "Nico", "bool_var", 20L),
            Map.of("id", "Franck", "bool_var", 100L));
  }

  @Test
  void testDagNvlExpr() throws ScriptException {
    engine.getContext().setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    engine.eval("res <- nvl(tmp1[keep long1], 0); tmp1 := ds1;");
    var res = engine.getContext().getAttribute("res");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "long1", 30L),
            Map.of("id", "Hadrien", "long1", 10L),
            Map.of("id", "Nico", "long1", 20L),
            Map.of("id", "Franck", "long1", 100L));
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Long.class);
  }

  @Test
  void testDagNvlImplicitCast() throws ScriptException {
    engine.getContext().setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
    engine.eval("res := nvl(tmp1[keep long1], 0.1); tmp1 <- ds1;");
    var res = engine.getContext().getAttribute("res");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Toto", "long1", 30D),
            Map.of("id", "Hadrien", "long1", 10D),
            Map.of("id", "Nico", "long1", 20D),
            Map.of("id", "Franck", "long1", 100D));
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Double.class);
  }

  @Test
  void testDagUnaryExpr() throws ScriptException {
    ScriptContext context = engine.getContext();

    context.setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    Object res = engine.eval("res := + tmp1[keep long1, double1]; tmp1 <- ds2;");
    assertThat(((Dataset) res).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "long1", 150L, "double1", 1.1D),
            Map.of("id", "Nico", "long1", 20L, "double1", 2.2D),
            Map.of("id", "Franck", "long1", 100L, "double1", -1.21D));
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Long.class);

    context.setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
    Object res2 = engine.eval("res2 := - tmp2[keep long1, double1]; tmp2 := ds2;");
    assertThat(((Dataset) res2).getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("id", "Hadrien", "long1", -150L, "double1", -1.1D),
            Map.of("id", "Nico", "long1", -20L, "double1", -2.2D),
            Map.of("id", "Franck", "long1", -100L, "double1", 1.21D));
    assertThat(((Dataset) res).getDataStructure().get("long1").getType()).isEqualTo(Long.class);
  }
}
