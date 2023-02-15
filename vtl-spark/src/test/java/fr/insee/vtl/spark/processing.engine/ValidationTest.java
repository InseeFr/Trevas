package fr.insee.vtl.spark.processing.engine;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.spark.SparkDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class ValidationTest {

    private final String DEFAULT_NULL_STR = "null";
    private final InMemoryDataset dataset = new InMemoryDataset(
            List.of(
                    List.of("2011", "I", "CREDIT", 10L),
                    List.of("2011", "I", "DEBIT", -2L),
                    List.of("2012", "I", "CREDIT", 10L),
                    List.of("2012", "I", "DEBIT", 2L)
            ),
            List.of(
                    new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_3", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE)
            )
    );
    private SparkSession spark;
    private ScriptEngine engine;

    private static <T, K> Map<K, T> replaceNullValues(Map<K, T> map, T defaultValue) {

        // Replace the null value
        map = map.entrySet()
                .stream()
                .map(entry -> {
                    if (entry.getValue() == null)
                        entry.setValue(defaultValue);
                    return entry;
                })
                .collect(Collectors.toMap(Map.Entry::getKey,
                        Map.Entry::getValue));

        return map;
    }

    @BeforeEach
    public void setUp() {

        ScriptEngineManager mgr = new ScriptEngineManager();
        engine = mgr.getEngineByExtension("vtl");

        spark = SparkSession.builder()
                .appName("test")
                .master("local")
                .getOrCreate();
        SparkSession.setActiveSession(spark);

        engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    }

    @AfterEach
    public void tearDown() {
        if (spark != null)
            spark.close();
    }

    @Test
    public void testValidateDPruleset() throws ScriptException {

        ScriptContext context = engine.getContext();
        context.setAttribute("DS_1", dataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("define datapoint ruleset dpr1 (variable Id_3, Me_1) is " +
                "ruleA : when Id_3 = \"CREDIT\" then Me_1 >= 0 errorcode \"Bad credit\"; " +
                "when Id_3 = \"DEBIT\" then Me_1 >= 0 errorcode \"Bad debit\" errorlevel 1 " +
                "end datapoint ruleset; " +
                "DS_r := check_datapoint(DS_1, dpr1); " +
                "DS_r_invalid := check_datapoint(DS_1, dpr1 invalid); " +
                "DS_r_all := check_datapoint(DS_1, dpr1 all); " +
                "DS_r_all_measures := check_datapoint(DS_1, dpr1 all_measures);");

        Dataset DS_r = (Dataset) engine.getContext().getAttribute("DS_r");
        assertThat(DS_r).isInstanceOf(Dataset.class);
        Dataset DS_r_invalid = (Dataset) engine.getContext().getAttribute("DS_r_invalid");
        assertThat(DS_r_invalid).isInstanceOf(Dataset.class);
        Dataset DS_r_all = (Dataset) engine.getContext().getAttribute("DS_r_all");
        assertThat(DS_r).isInstanceOf(Dataset.class);
        Dataset DS_r_all_measures = (Dataset) engine.getContext().getAttribute("DS_r_all_measures");
        assertThat(DS_r_all_measures).isInstanceOf(Dataset.class);

        List<Map<String, Object>> DS_rWithNull = DS_r.getDataAsMap();
        List<Map<String, Object>> DS_r_invalidWithNull = DS_r_invalid.getDataAsMap();
        List<Map<String, Object>> DS_r_allWithNull = DS_r_all.getDataAsMap();
        List<Map<String, Object>> DS_r_all_measuresWithNull = DS_r_all_measures.getDataAsMap();

        List<Map<String, Object>> DS_rWithoutNull = new ArrayList<>();
        for (Map<String, Object> map : DS_rWithNull) {
            DS_rWithoutNull.add(replaceNullValues(map, DEFAULT_NULL_STR));
        }
        List<Map<String, Object>> DS_r_invalidWithoutNull = new ArrayList<>();
        for (Map<String, Object> map : DS_r_invalidWithNull) {
            DS_r_invalidWithoutNull.add(replaceNullValues(map, DEFAULT_NULL_STR));
        }
        List<Map<String, Object>> DS_r_allWithoutNull = new ArrayList<>();
        for (Map<String, Object> map : DS_r_allWithNull) {
            DS_r_allWithoutNull.add(replaceNullValues(map, DEFAULT_NULL_STR));
        }
        List<Map<String, Object>> DS_r_all_measuresWithoutNull = new ArrayList<>();
        for (Map<String, Object> map : DS_r_all_measuresWithNull) {
            DS_r_all_measuresWithoutNull.add(replaceNullValues(map, DEFAULT_NULL_STR));
        }

        assertThat(DS_rWithoutNull).containsExactlyInAnyOrder(
                Map.of("Id_1", "2011", "Id_2", "I", "Id_3", "DEBIT",
                        "Me_1", -2L, "ruleid", "dpr1_2",
                        "errorcode", "Bad debit", "errorlevel", 1L)
        ).containsExactlyInAnyOrderElementsOf(DS_r_invalidWithoutNull);

        assertThat(DS_r_allWithoutNull).containsExactlyInAnyOrder(
                Map.of("Id_1", "2011", "Id_2", "I", "Id_3", "CREDIT",
                        "Me_1", 10L, "ruleid", "ruleA", "bool_var", true,
                        "errorcode", "null", "errorlevel", "null"),
                Map.of("Id_1", "2011", "Id_2", "I", "Id_3", "CREDIT",
                        "Me_1", 10L, "ruleid", "dpr1_2", "bool_var", true,
                        "errorcode", "null", "errorlevel", "null"),
                Map.of("Id_1", "2011", "Id_2", "I", "Id_3", "DEBIT",
                        "Me_1", -2L, "ruleid", "ruleA", "bool_var", true,
                        "errorcode", "null", "errorlevel", "null"),
                Map.of("Id_1", "2011", "Id_2", "I", "Id_3", "DEBIT",
                        "Me_1", -2L, "ruleid", "dpr1_2", "bool_var", false,
                        "errorcode", "Bad debit", "errorlevel", 1L),
                Map.of("Id_1", "2012", "Id_2", "I", "Id_3", "CREDIT",
                        "Me_1", 10L, "ruleid", "ruleA", "bool_var", true,
                        "errorcode", "null", "errorlevel", "null"),
                Map.of("Id_1", "2012", "Id_2", "I", "Id_3", "CREDIT",
                        "Me_1", 10L, "ruleid", "dpr1_2", "bool_var", true,
                        "errorcode", "null", "errorlevel", "null"),
                Map.of("Id_1", "2012", "Id_2", "I", "Id_3", "DEBIT",
                        "Me_1", 2L, "ruleid", "ruleA", "bool_var", true,
                        "errorcode", "null", "errorlevel", "null"),
                Map.of("Id_1", "2012", "Id_2", "I", "Id_3", "DEBIT",
                        "Me_1", 2L, "ruleid", "dpr1_2", "bool_var", true,
                        "errorcode", "null", "errorlevel", "null")
        ).containsExactlyInAnyOrderElementsOf(DS_r_all_measuresWithoutNull);
    }

    @Test
    public void testValidateDPrulesetWithAlias() throws ScriptException {

        ScriptContext context = engine.getContext();
        context.setAttribute("DS_1", dataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("define datapoint ruleset dpr1 (variable Id_3 as AA, Me_1) is " +
                "when AA = \"CREDIT\" then Me_1 >= 0 errorcode \"Bad credit\"; " +
                "when AA = \"DEBIT\" then Me_1 >= 0 errorcode \"Bad debit\" " +
                "end datapoint ruleset; " +
                "DS_r := check_datapoint(DS_1, dpr1);");

        Dataset DS_r = (Dataset) engine.getContext().getAttribute("DS_r");
        assertThat(DS_r).isInstanceOf(Dataset.class);

        List<Map<String, Object>> DS_rWithNull = DS_r.getDataAsMap();
        List<Map<String, Object>> DS_rWithoutNull = new ArrayList<>();
        for (Map<String, Object> map : DS_rWithNull) {
            DS_rWithoutNull.add(replaceNullValues(map, DEFAULT_NULL_STR));
        }

        assertThat(DS_rWithoutNull).containsExactlyInAnyOrder(
                Map.of("Id_1", "2011", "Id_2", "I", "Id_3", "DEBIT",
                        "Me_1", -2L, "ruleid", "dpr1_2",
                        "errorcode", "Bad debit", "errorlevel", "null")
        );

    }

    @Test
    public void testValidateDPrulesetSerialization() throws ScriptException {
        org.apache.spark.sql.Dataset<Row> parquet = spark.read().parquet("src/main/resources/input_sample");
        SparkDataset sparkDataset = new SparkDataset(parquet);

        ScriptContext context = engine.getContext();
        context.setAttribute("DS_1", sparkDataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("define datapoint ruleset dpr1 (variable student_number) is " +
                "my_rule : student_number < 100 errorcode \"Not enough\" " +
                "end datapoint ruleset; " +
                "DS_r := check_datapoint(DS_1, dpr1);");

        Dataset DS_r = (Dataset) engine.getContext().getAttribute("DS_r");
        List<Structured.DataPoint> dataPoints = DS_r.getDataPoints();
    }
}
