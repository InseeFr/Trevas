package fr.insee.vtl.prov;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.prov.prov.DataframeInstance;
import fr.insee.vtl.prov.prov.Program;
import fr.insee.vtl.prov.prov.ProgramStep;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration tests for {@link Provenance#run} (replaces legacy ProvenanceListenerTest). */
public class ProvenanceTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    SparkSession spark = TestSparkSessionFactory.create();

    ScriptEngineManager mgr = new ScriptEngineManager();
    engine = mgr.getEngineByExtension("vtl");
    engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, TestSparkEngineConfig.getEngineName());
    engine.put("$vtl.spark.session", spark);
  }

  @Test
  public void simpleTest() throws ScriptException {
    String simpleScript =
        """
                        ds_sum := ds1 + ds2;
                        ds_mul := ds_sum * 3;
                        ds_res <- ds_mul[filter mod(var1, 2) = 0][calc var_sum := var1 + var2];
                        ds_aggr <- ds_res[aggr a := sum(var1) group by id];
                        """;

    bindStandardPair();

    Program program =
        Provenance.run(engine, simpleScript, "trevas-simple-test", "Simple test from Trevas tests");
    assertThat(program.getProgramSteps()).hasSize(4);
    ProgramStep dsMulProgram =
        program.getProgramSteps().stream()
            .filter(p -> p.getLabel().equals("ds_mul"))
            .findFirst()
            .get();
    assertThat(
            dsMulProgram.getConsumedDataframes().stream().map(DataframeInstance::getLabel).toList())
        .contains("ds_sum");
  }

  @Test
  public void testWithEmptyLines() throws ScriptException {
    String script =
        """


                            ds1_c := ds1;
                            ds2_c := ds2;
                            ds_sum := ds1_c + ds2_c;
                            ds_mul <- ds_sum * 3;
                            ds_res <- ds_mul[filter mod(var1, 2) = 0][calc var_sum := var1 + var2];



                        """;
    bindStandardPair();

    Program program =
        Provenance.run(engine, script, "trevas-simple-test", "Simple test from Trevas tests");
    assertThat(program.getProgramSteps()).hasSize(5);
  }

  @Test
  void testValidation() throws ScriptException {
    String validationExpr =
        """
                              define datapoint ruleset test (variable sex) is
                                  myrule : sex in {"M"} errorcode "ERROR"
                              end datapoint ruleset;
                              ds_result <- check_datapoint(ds1, test all);
                        """;

    Map<String, Class<?>> types = Map.of("id", String.class, "sex", String.class);
    Map<String, Dataset.Role> roles =
        Map.of("id", Dataset.Role.IDENTIFIER, "sex", Dataset.Role.MEASURE);
    InMemoryDataset ds1 =
        new InMemoryDataset(
            List.of(
                Map.of("id", "1", "sex", "M"),
                Map.of("id", "2", "sex", "F"),
                Map.of("id", "3", "sex", "M")),
            types,
            roles);

    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    Program programWithBindings =
        Provenance.run(engine, validationExpr, "trevas-validation-test", "Trevas validation test");
    assertThat(programWithBindings.getProgramSteps()).hasSize(1);
  }

  @Test
  void testJoin() throws ScriptException {
    String validationExpr =
        """
                              ds2 := ds1[rename sex to sex_old];
                              ds1_1 := inner_join(ds1, ds2);
                              ds1_2 := inner_join(ds1, ds2 using id);
                        """;

    Map<String, Class<?>> types = Map.of("id", String.class, "sex", String.class);
    Map<String, Dataset.Role> roles =
        Map.of("id", Dataset.Role.IDENTIFIER, "sex", Dataset.Role.MEASURE);
    InMemoryDataset ds1 =
        new InMemoryDataset(
            List.of(
                Map.of("id", "1", "sex", "M"),
                Map.of("id", "2", "sex", "F"),
                Map.of("id", "3", "sex", "M")),
            types,
            roles);

    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    Program programWithBindings =
        Provenance.run(engine, validationExpr, "trevas-join-test", "Trevas join test");
    assertThat(programWithBindings.getProgramSteps()).hasSize(3);
  }

  @Test
  void testScriptError() {
    String failedExpr = "ds1 := ds1;";

    Map<String, Class<?>> types = Map.of("id", String.class, "sex", String.class);
    Map<String, Dataset.Role> roles =
        Map.of("id", Dataset.Role.IDENTIFIER, "sex", Dataset.Role.MEASURE);
    InMemoryDataset ds1 =
        new InMemoryDataset(
            List.of(
                Map.of("id", "1", "sex", "M"),
                Map.of("id", "2", "sex", "F"),
                Map.of("id", "3", "sex", "M")),
            types,
            roles);
    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    assertThatThrownBy(
            () -> {
              Provenance.run(engine, failedExpr, "trevas-failed-test", "Trevas failed test");
            })
        .isInstanceOf(VtlScriptException.class)
        .hasMessage("Dataset ds1 is part of the bindings and therefore cannot be assigned");
  }

  @Test
  public void registeredLoadCsvThenClauses() throws Exception {
    ((VtlScriptEngine) engine)
        .registerMethod("loadCSV", StubLoads.class.getMethod("loadCSV", String.class));

    String script =
        """
        tmp := loadCSV("./test.csv");
        inp := tmp[calc identifier vendor_id := vendor_id];
        fil := inp[filter vendor_id = "VTS"];
        out := fil[drop extra];
        """;

    Program program = Provenance.run(engine, script, "registered-load", "loadCSV chain");
    assertThat(program.getProgramSteps()).hasSize(4);
    assertThat(program.getProgramSteps().stream().map(ProgramStep::getLabel).toList())
        .contains("tmp", "inp", "fil", "out");
  }

  public static final class StubLoads {
    private StubLoads() {}

    public static InMemoryDataset loadCSV(String path) {
      return new InMemoryDataset(
          List.of(
              Map.of("vendor_id", "VTS", "extra", "a"), Map.of("vendor_id", "CMT", "extra", "b")),
          Map.of("vendor_id", String.class, "extra", String.class),
          Map.of(
              "vendor_id", Dataset.Role.MEASURE,
              "extra", Dataset.Role.MEASURE));
    }
  }

  private void bindStandardPair() {
    Map<String, Class<?>> types =
        Map.of("id", String.class, "var1", Long.class, "var2", Long.class);
    Map<String, Dataset.Role> roles =
        Map.of(
            "id",
            Dataset.Role.IDENTIFIER,
            "var1",
            Dataset.Role.MEASURE,
            "var2",
            Dataset.Role.MEASURE);
    InMemoryDataset ds1 =
        new InMemoryDataset(
            List.of(
                Map.of("id", "1", "var1", 10L, "var2", 11L),
                Map.of("id", "2", "var1", 11L, "var2", 10L),
                Map.of("id", "3", "var1", 12L, "var2", 9L)),
            types,
            roles);
    InMemoryDataset ds2 =
        new InMemoryDataset(
            List.of(
                Map.of("id", "1", "var1", 20L, "var2", 110L),
                Map.of("id", "2", "var1", -1L, "var2", 10L),
                Map.of("id", "3", "var1", 0L, "var2", 9L)),
            types,
            roles);
    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    context.setAttribute("ds2", ds2, ScriptContext.ENGINE_SCOPE);
  }
}
