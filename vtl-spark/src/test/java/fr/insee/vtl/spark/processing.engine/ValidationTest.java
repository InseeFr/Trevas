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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    private final InMemoryDataset ds_1_check = new InMemoryDataset(
            List.of(
                    List.of("2010", "I", 1L),
                    List.of("2011", "I", 2L),
                    List.of("2012", "I", 10L),
                    List.of("2013", "I", 4L),
                    List.of("2014", "I", 5L),
                    List.of("2015", "I", 6L),
                    List.of("2010", "D", 25L),
                    List.of("2011", "D", 35L),
                    List.of("2012", "D", 45L),
                    List.of("2013", "D", 55L),
                    List.of("2014", "D", 50L),
                    List.of("2015", "D", 75L)
            ),
            List.of(
                    new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE)
            )
    );
    private final InMemoryDataset ds_2_check = new InMemoryDataset(
            List.of(
                    List.of("2010", "I", 9L),
                    List.of("2011", "I", 2L),
                    List.of("2012", "I", 10L),
                    List.of("2013", "I", 7L),
                    List.of("2014", "I", 5L),
                    List.of("2015", "I", 6L),
                    List.of("2010", "D", 50L),
                    List.of("2011", "D", 35L),
                    List.of("2012", "D", 40L),
                    List.of("2013", "D", 55L),
                    List.of("2014", "D", 65L),
                    List.of("2015", "D", 75L)
            ),
            List.of(
                    new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE)
            )
    );
    private final Dataset dsExpr = new InMemoryDataset(
            List.of(
                    List.of("2011", "I", "CREDIT", true),
                    List.of("2011", "I", "DEBIT", false),
                    List.of("2012", "I", "CREDIT", false),
                    List.of("2012", "I", "DEBIT", true)
            ),
            List.of(
                    new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_3", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("bool_var", Boolean.class, Dataset.Role.MEASURE)
            )
    );
    private final Dataset dsImbalance = new InMemoryDataset(
            List.of(
                    List.of("2011", "I", "CREDIT", 1L),
                    List.of("2011", "I", "DEBIT", 2L),
                    List.of("2012", "I", "CREDIT", 2L),
                    List.of("2012", "I", "DEBIT", 3L)
            ),
            List.of(
                    new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_3", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("imbalance", Long.class, Dataset.Role.MEASURE)
            )
    );
    private final Dataset dsImbalanceToRename = new InMemoryDataset(
            List.of(
                    List.of("2011", "I", "CREDIT", 1L),
                    List.of("2011", "I", "DEBIT", 2L),
                    List.of("2012", "I", "CREDIT", 2L),
                    List.of("2012", "I", "DEBIT", 3L)
            ),
            List.of(
                    new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_3", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("imbalance_1", Long.class, Dataset.Role.MEASURE)
            )
    );
    private final String hierarchicalRulesetDef = "define hierarchical ruleset HR_1 (variable rule Me_1) is \n" +
            "R010 : A = J + K + L errorlevel 5;\n" +
            "R020 : B = M + N + O errorlevel 5;\n" +
            "R030 : C = P + Q errorcode \"XX\" errorlevel 5;\n" +
            "R040 : D = R + S errorlevel 1;\n" +
            "R050 : E = T + U + V errorlevel 0;\n" +
            "R060 : F = Y + W + Z errorlevel 7;\n" +
            "R070 : G = B + C;\n" +
            "R080 : H = D + E errorlevel 0;\n" +
            "R090 : I = D + G errorcode \"YY\" errorlevel 0;\n" +
            "R100 : M >= N errorlevel 5;\n" +
            "R110 : M <= G errorlevel 5\n" +
            "end hierarchical ruleset; \n";
    private final Dataset DS_1_HR = new InMemoryDataset(
            List.of(
                    List.of("2010", "A", 5L),
                    List.of("2010", "B", 11L),
                    List.of("2010", "C", 0L),
                    List.of("2010", "G", 19L),
                    Stream.of("2010", "H", null).collect(Collectors.toList()),
                    List.of("2010", "I", 14L),
                    List.of("2010", "M", 2L),
                    List.of("2010", "N", 5L),
                    List.of("2010", "O", 4L),
                    List.of("2010", "P", 7L),
                    List.of("2010", "Q", -7L),
                    List.of("2010", "S", 3L),
                    List.of("2010", "T", 9L),
                    Stream.of("2010", "U", null).collect(Collectors.toList()),
                    List.of("2010", "V", 6L)
            ),
            List.of(
                    new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
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
    public void testCheck() throws ScriptException {
        ScriptContext context = engine.getContext();
        context.setAttribute("DS1", ds_1_check, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("DS2", ds_2_check, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds := check(DS1 >= DS2 errorcode \"err\" errorlevel 1 imbalance DS1 - DS2);" +
                "ds1 := check(DS1 >= DS2 errorcode \"err\" errorlevel 1 imbalance DS1 - DS2 invalid);");

        var ds = (Dataset) engine.getContext().getAttribute("ds");
        assertThat(ds).isInstanceOf(Dataset.class);

        List<Map<String, Object>> dsWithNull = ds.getDataAsMap();
        List<Map<String, Object>> dsWithoutNull = new ArrayList<>();
        for (Map<String, Object> map : dsWithNull) {
            dsWithoutNull.add(replaceNullValues(map, DEFAULT_NULL_STR));
        }

        assertThat(dsWithoutNull).isEqualTo(List.of(
                Map.of("Id_1", "2010", "Id_2", "I", "bool_var", false,
                        "imbalance", -8L, "errorcode", "err", "errorlevel", 1L),
                Map.of("Id_1", "2011", "Id_2", "I", "bool_var", true,
                        "imbalance", 0L, "errorcode", "null", "errorlevel", "null"),
                Map.of("Id_1", "2012", "Id_2", "I", "bool_var", true,
                        "imbalance", 0L, "errorcode", "null", "errorlevel", "null"),
                Map.of("Id_1", "2013", "Id_2", "I", "bool_var", false,
                        "imbalance", -3L, "errorcode", "err", "errorlevel", 1L),
                Map.of("Id_1", "2014", "Id_2", "I", "bool_var", true,
                        "imbalance", 0L, "errorcode", "null", "errorlevel", "null"),
                Map.of("Id_1", "2015", "Id_2", "I", "bool_var", true,
                        "imbalance", 0L, "errorcode", "null", "errorlevel", "null"),
                Map.of("Id_1", "2010", "Id_2", "D", "bool_var", false,
                        "imbalance", -25L, "errorcode", "err", "errorlevel", 1L),
                Map.of("Id_1", "2011", "Id_2", "D", "bool_var", true,
                        "imbalance", 0L, "errorcode", "null", "errorlevel", "null"),
                Map.of("Id_1", "2012", "Id_2", "D", "bool_var", true,
                        "imbalance", 5L, "errorcode", "null", "errorlevel", "null"),
                Map.of("Id_1", "2013", "Id_2", "D", "bool_var", true,
                        "imbalance", 0L, "errorcode", "null", "errorlevel", "null"),
                Map.of("Id_1", "2014", "Id_2", "D", "bool_var", false,
                        "imbalance", -15L, "errorcode", "err", "errorlevel", 1L),
                Map.of("Id_1", "2015", "Id_2", "D", "bool_var", true,
                        "imbalance", 0L, "errorcode", "null", "errorlevel", "null")));
        assertThat(ds.getDataStructure()).containsValues(
                new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("bool_var", Boolean.class, Dataset.Role.MEASURE),
                new Structured.Component("imbalance", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("errorcode", String.class, Dataset.Role.MEASURE),
                new Structured.Component("errorlevel", Long.class, Dataset.Role.MEASURE)
        );

        var ds1 = (Dataset) engine.getContext().getAttribute("ds1");
        assertThat(ds1).isInstanceOf(Dataset.class);

        List<Map<String, Object>> ds1WithNull = ds1.getDataAsMap();
        List<Map<String, Object>> ds1WithoutNull = new ArrayList<>();
        for (Map<String, Object> map : ds1WithNull) {
            ds1WithoutNull.add(replaceNullValues(map, DEFAULT_NULL_STR));
        }

        assertThat(ds1WithoutNull).isEqualTo(List.of(
                Map.of("Id_1", "2010", "Id_2", "I",
                        "imbalance", -8L, "errorcode", "err", "errorlevel", 1L),
                Map.of("Id_1", "2013", "Id_2", "I",
                        "imbalance", -3L, "errorcode", "err", "errorlevel", 1L),
                Map.of("Id_1", "2010", "Id_2", "D",
                        "imbalance", -25L, "errorcode", "err", "errorlevel", 1L),
                Map.of("Id_1", "2014", "Id_2", "D",
                        "imbalance", -15L, "errorcode", "err", "errorlevel", 1L)));
    }

    @Test
    public void testValidationSimpleException() throws ScriptException {

        ScriptContext context = engine.getContext();
        context.setAttribute("dsExpr", dsExpr, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("dsImbalance", dsImbalance, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("dsImbalanceToRename", dsImbalanceToRename, ScriptContext.ENGINE_SCOPE);

        engine.eval("DS_r := check(dsExpr errorcode \"error\" errorlevel 1 imbalance dsImbalance);");
        engine.eval("DS_r_invalid := check(dsExpr errorcode \"error\" errorlevel 1 imbalance dsImbalance invalid);");
        engine.eval("DS_r_to_rename := check(dsExpr errorcode \"error\" errorlevel 1 imbalance dsImbalanceToRename);");
        Dataset DS_r = (Dataset) engine.getContext().getAttribute("DS_r");
        assertThat(DS_r.getDataAsMap().size()).isEqualTo(4);
        Dataset DS_r_invalid = (Dataset) engine.getContext().getAttribute("DS_r_invalid");
        assertThat(DS_r_invalid.getDataAsMap().size()).isEqualTo(2);
        Dataset DS_r_to_rename = (Dataset) engine.getContext().getAttribute("DS_r_to_rename");
        List<String> DS_r_to_renameMeasure = DS_r_to_rename.getDataStructure().values()
                .stream().filter(c -> c.isMeasure())
                .map(c -> c.getName()).collect(Collectors.toList());
        assertThat(DS_r_to_renameMeasure.size()).isEqualTo(4);
        assertThat(DS_r_to_renameMeasure.contains("imbalance")).isTrue();
    }

    @Test
    public void serializationCheckDatapointTest() throws ScriptException {
        ScriptContext context = engine.getContext();
        org.apache.spark.sql.Dataset<Row> ds1_csv = spark.read()
                .option("delimiter", ";")
                .option("header", "true")
                .csv("src/main/resources/ds1.csv");
        SparkDataset sparkDataset1 = new SparkDataset(ds1_csv);
        context.setAttribute("ds1", sparkDataset1, ScriptContext.ENGINE_SCOPE);
        org.apache.spark.sql.Dataset<Row> ds2_csv = spark.read()
                .option("delimiter", ";")
                .option("header", "true")
                .csv("src/main/resources/ds2.csv");
        SparkDataset sparkDataset2 = new SparkDataset(ds1_csv);
        context.setAttribute("ds2", sparkDataset2, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds1 := ds1[calc identifier id := id, long1 := cast(long1, integer), double1 := cast(double1, number), bool1 := cast(bool1, boolean)]; " +
                "ds2 := ds2[calc identifier id := id, long1 := cast(long1, integer), double1 := cast(double1, number), bool1 := cast(bool1, boolean)]; " +
                "ds_concat := ds1#string1 || \" and \" || ds2#string1; " +
                "ds1_num := ds1[keep id, long1, double1]; " +
                "ds2_num := ds2[keep id, long1, double1]; " +
                "ds_mod := mod(ds1_num, 2); " +
                "ds_sum := ds1_num + ds2_num; " +
                "ds_compare := ds1_num = ds2_num; " +
                "define datapoint ruleset dpr1 ( variable double1, long1 ) is " +
                "   my_rule_1 : double1 > 0 errorcode \"Double <= 0\" errorlevel 1; " +
                "   my_rule_2 : long1 > 0 errorcode \"Long <= 0\" errorlevel 100 " +
                "end datapoint ruleset; " +
                "ds_check_datapoint := check_datapoint(ds1_num, dpr1 all); " +
                "ds_check := check(ds1#long1 > ds2#long1 errorcode \"error\" errorlevel 1 imbalance ds1#long1 + ds2#long1 invalid);");
        List<Structured.DataPoint> dsCheckDatapoint = ((Dataset) engine.getContext().getAttribute("ds_check_datapoint")).getDataPoints();
    }

    @Test
    @Disabled
    public void checkHierarchy() throws ScriptException {
        ScriptContext context = engine.getContext();
        context.setAttribute("DS_1", DS_1_HR, ScriptContext.ENGINE_SCOPE);

        engine.eval(hierarchicalRulesetDef +
                "DS_r := check_hierarchy(DS_1, HR_1 rule Id_2); " /*+
                "DS_r_all := check_hierarchy(DS_1, HR_1 rule Id_2 all); " +
                "DS_r_all_measures := check_hierarchy(DS_1, HR_1 rule Id_2 all_measures"*/
        );

        Dataset dsR = (Dataset) engine.getContext().getAttribute("DS_r");
        List<Map<String, Object>> dsRWithNull = dsR.getDataAsMap();
        List<Map<String, Object>> dsRWithoutNull = new ArrayList<>();
        for (Map<String, Object> map : dsRWithNull) {
            dsRWithoutNull.add(replaceNullValues(map, DEFAULT_NULL_STR));
        }

//        assertThat(dsRWithoutNull).isEqualTo(List.of(
//                Map.of("Id_1", "2010", "Id_2", "I", "bool_var", false,
//                        "imbalance", -8L, "errorcode", "err", "errorlevel", 1L))
//        );
        assertThat(dsR.getDataStructure()).containsValues(
                new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("ruleid", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("imbalance", Long.class, Dataset.Role.MEASURE),
                new Structured.Component("errorcode", String.class, Dataset.Role.MEASURE),
                new Structured.Component("errorlevel", Long.class, Dataset.Role.MEASURE)
        );

//        Dataset dsRAll = (Dataset) engine.getContext().getAttribute("DS_r_all");
//
//        Dataset dsRAllMeasures = (Dataset) engine.getContext().getAttribute("DS_r_all_measures");
    }

    @Test
    public void checkHierarchyException() {
        Dataset DS_2_HR = new InMemoryDataset(
                List.of(
                        List.of("2010", "A", 5L, 5L)
                ),
                List.of(
                        new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE),
                        new Structured.Component("Me_2", Long.class, Dataset.Role.MEASURE)
                )
        );

        Dataset DS_3_HR = new InMemoryDataset(
                List.of(
                        List.of("2010", "A", "5")
                ),
                List.of(
                        new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("Me_1", String.class, Dataset.Role.MEASURE)
                )
        );

        Dataset DS_4_HR = new InMemoryDataset(
                List.of(
                        List.of("2010", "A", 5L)
                ),
                List.of(
                        new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE)
                )
        );

        ScriptContext context = engine.getContext();
        context.setAttribute("DS_1", DS_1_HR, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("DS_2", DS_2_HR, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("DS_3", DS_3_HR, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("DS_4", DS_4_HR, ScriptContext.ENGINE_SCOPE);

        assertThatThrownBy(() -> engine.eval(hierarchicalRulesetDef +
                "DS_r := check_hierarchy(DS_1, HR_1 rule Id_2 dataset_priority all);"))
                .hasMessageContaining("dataset_priority input mode is not supported in check_hierarchy");

        assertThatThrownBy(() -> engine.eval(hierarchicalRulesetDef +
                "DS_r := check_hierarchy(DS_1, HR_1 rule Id_2 partial_null all);"))
                .hasMessageContaining("partial_null validation mode is not supported in check_hierarchy");

        assertThatThrownBy(() -> engine.eval(hierarchicalRulesetDef +
                "DS_r := check_hierarchy(DS_2, HR_1 rule Id_2 partial_null all);"))
                .hasMessageContaining("Dataset DS_2 is not monomeasure");

        assertThatThrownBy(() -> engine.eval(hierarchicalRulesetDef +
                "DS_r := check_hierarchy(DS_3, HR_1 rule Id_2 partial_null all);"))
                .hasMessageContaining("Dataset DS_3 measure Me_1 has to have number type");

        assertThatThrownBy(() -> engine.eval(hierarchicalRulesetDef +
                "DS_r := check_hierarchy(DS_4, HR_1 rule Id_3 partial_null all);"))
                .hasMessageContaining("ComponentID Id_3 not contained in dataset DS_4");
    }
}
