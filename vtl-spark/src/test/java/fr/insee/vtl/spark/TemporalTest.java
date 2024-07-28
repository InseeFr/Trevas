package fr.insee.vtl.spark;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.utils.Java8Helpers;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.time.Instant;

import static fr.insee.vtl.model.Structured.Component;
import static org.assertj.core.api.Assertions.assertThat;

public class TemporalTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        SparkSession spark = SparkSession.builder()
                .appName("test")
                .master("local")
                .getOrCreate();

        ScriptEngineManager mgr = new ScriptEngineManager();
        engine = mgr.getEngineByExtension("vtl");
        engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    }

    @Test
    public void testFlowToStock() throws ScriptException {
        InMemoryDataset ds = new InMemoryDataset(
                Java8Helpers.listOf(
                        new Component("id1", String.class, Dataset.Role.IDENTIFIER),
                        new Component("id2", Instant.class, Dataset.Role.IDENTIFIER),
                        new Component("me1", Long.class, Dataset.Role.MEASURE)
                ),
                Java8Helpers.listOf("A", Instant.parse("2009-01-01T00:00:00Z"), 2L),
                Java8Helpers.listOf("A", Instant.parse("2011-01-01T00:00:00Z"), 5L),
                Java8Helpers.listOf("A", Instant.parse("2012-01-01T00:00:00Z"), -3L),
                Java8Helpers.listOf("B", Instant.parse("2010-01-01T00:00:00Z"), 9L),
                Java8Helpers.listOf("B", Instant.parse("2011-01-01T00:00:00Z"), 4L),
                Java8Helpers.listOf("B", Instant.parse("2013-01-01T00:00:00Z"), -6L)
        );
        engine.put("ds", ds);
        engine.eval("r := flow_to_stock(ds);");
        assertThat(engine.get("r")).isInstanceOf(Dataset.class);
        assertThat(((Dataset) engine.get("r")).getDataAsList()).containsExactlyInAnyOrder(
                Java8Helpers.listOf("A", Instant.parse("2009-01-01T00:00:00Z"), 2L),
                Java8Helpers.listOf("A", Instant.parse("2011-01-01T00:00:00Z"), 7L),
                Java8Helpers.listOf("A", Instant.parse("2012-01-01T00:00:00Z"), 4L),
                Java8Helpers.listOf("B", Instant.parse("2010-01-01T00:00:00Z"), 9L),
                Java8Helpers.listOf("B", Instant.parse("2011-01-01T00:00:00Z"), 13L),
                Java8Helpers.listOf("B", Instant.parse("2013-01-01T00:00:00Z"), 7L)
        );
    }

    @Test
    public void testStockToFlow() throws ScriptException {
        InMemoryDataset ds = new InMemoryDataset(
                Java8Helpers.listOf(
                        new Component("id1", String.class, Dataset.Role.IDENTIFIER),
                        new Component("id2", Instant.class, Dataset.Role.IDENTIFIER),
                        new Component("me1", Long.class, Dataset.Role.MEASURE),
                        new Component("me2", String.class, Dataset.Role.MEASURE)
                ),
                Java8Helpers.listOf("A", Instant.parse("2010-01-01T00:00:00Z"), 2L, "foo"),
                Java8Helpers.listOf("A", Instant.parse("2011-01-01T00:00:00Z"), 7L, "foo"),
                Java8Helpers.listOf("A", Instant.parse("2012-01-01T00:00:00Z"), 4L, "foo"),
                Java8Helpers.listOf("A", Instant.parse("2013-01-01T00:00:00Z"), 13L, "foo"),
                Java8Helpers.listOf("B", Instant.parse("2010-01-01T00:00:00Z"), 4L, "foo"),
                Java8Helpers.listOf("B", Instant.parse("2011-01-01T00:00:00Z"), -4L, "foo"),
                Java8Helpers.listOf("B", Instant.parse("2012-01-01T00:00:00Z"), -4L, "foo"),
                Java8Helpers.listOf("B", Instant.parse("2013-01-01T00:00:00Z"), 2L, "foo")
        );
        engine.put("ds", ds);
        engine.eval("res := stock_to_flow(ds);");
        assertThat(engine.get("res")).isInstanceOf(Dataset.class);
        ((Dataset) engine.get("res")).getDataAsMap().forEach(System.out::println);
        assertThat(((Dataset) engine.get("res")).getDataAsList()).containsExactly(
                Java8Helpers.listOf("A", Instant.parse("2010-01-01T00:00:00Z"), "foo", 2L),
                Java8Helpers.listOf("A", Instant.parse("2011-01-01T00:00:00Z"), "foo", 5L),
                Java8Helpers.listOf("A", Instant.parse("2012-01-01T00:00:00Z"), "foo", -3L),
                Java8Helpers.listOf("A", Instant.parse("2013-01-01T00:00:00Z"), "foo", 9L),
                Java8Helpers.listOf("B", Instant.parse("2010-01-01T00:00:00Z"), "foo", 4L),
                Java8Helpers.listOf("B", Instant.parse("2011-01-01T00:00:00Z"), "foo", -8L),
                Java8Helpers.listOf("B", Instant.parse("2012-01-01T00:00:00Z"), "foo", 0L),
                Java8Helpers.listOf("B", Instant.parse("2013-01-01T00:00:00Z"), "foo", 6L)
        );
    }
}
