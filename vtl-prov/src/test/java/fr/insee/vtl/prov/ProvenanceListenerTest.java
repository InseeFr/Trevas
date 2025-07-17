package fr.insee.vtl.prov;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.prov.prov.DataframeInstance;
import fr.insee.vtl.prov.prov.Program;
import fr.insee.vtl.prov.prov.ProgramStep;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProvenanceListenerTest {

  private ScriptEngine engine;

  @BeforeEach
  public void setUp() {
    SparkSession spark = SparkSession.builder().appName("test").master("local").getOrCreate();

    ScriptEngineManager mgr = new ScriptEngineManager();
    engine = mgr.getEngineByExtension("vtl");
    engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    engine.put("$vtl.spark.session", spark);
  }

  @Test
  public void simpleTestWithBindings() {
    String simpleScript =
        """
                              ds_sum := ds1 + ds2;
                              ds_mul := ds_sum * 3;\s
                              ds_res <- ds_mul[filter mod(var1, 2) = 0][calc var_sum := var1 + var2];\
                        """;

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

    Program program =
        ProvenanceListener.run(
            engine, simpleScript, "trevas-simple-test", "Simple test from Trevas tests");
    assertThat(program.getProgramSteps()).hasSize(3);
    ProgramStep dsMulProgram =
        program.getProgramSteps().stream()
            .filter(p -> p.getLabel().equals("ds_mul"))
            .findFirst()
            .get();
    assertThat(
            dsMulProgram.getConsumedDataframe().stream().map(DataframeInstance::getLabel).toList())
        .contains("ds_sum");
  }

  @Test
  public void testWithEmptyLines() {
    String script =
        """


                            ds1 := 'data.ds1'[calc identifier id1 := id1, var1 := cast(var1, integer), var2 := cast(var2, integer)];


                            ds2_out := 'other.ds2'[calc identifier id1 := id1, var1 := cast(var1, integer), var2 := cast(var2, integer)];
                            ds_sum := ds1 + ds2_out;
                            ds_mul <- ds_sum * 3;
                                    'data.ds_res' <- ds_mul[filter mod(var1, 2) = 0][calc var_sum := var1 + var2];



                        """;

    Program program =
        ProvenanceListener.run(
            engine, script, "trevas-simple-test", "Simple test from Trevas tests");
    assertThat(program.getProgramSteps()).hasSize(5);
  }

  @Test
  void testValidationWithBindings() {
    String validationExpr =
        """
                              define datapoint ruleset test (variable sex) is
                                  myrule : sex in {"M"} errorcode "ERROR"
                              end datapoint ruleset;
                              pengfei.ds_result <- check_datapoint(pengfei.pengfei, test);
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
    context.setAttribute("pengfei.pengfei", ds1, ScriptContext.ENGINE_SCOPE);
    Program programWithBindings =
        ProvenanceListener.run(
            engine, validationExpr, "trevas-validation-test", "Trevas validation test");
    assertThat(programWithBindings.getProgramSteps()).hasSize(1);
  }
}
