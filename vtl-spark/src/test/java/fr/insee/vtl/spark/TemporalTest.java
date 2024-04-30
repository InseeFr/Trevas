package fr.insee.vtl.spark;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.time.Instant;
import java.util.List;

import static fr.insee.vtl.model.Structured.Component;
import static org.assertj.core.api.Assertions.assertThat;

public class TemporalTest {

    private SparkSession spark;
    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        spark = SparkSession.builder()
                .appName("test")
                .master("local")
                .getOrCreate();

        ScriptEngineManager mgr = new ScriptEngineManager();
        engine = mgr.getEngineByExtension("vtl");
        engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    }

    @Test
    public void testFlowToStock() throws ScriptException {
        var ds = new InMemoryDataset(
                List.of(
                        new Component("id1", String.class, Dataset.Role.IDENTIFIER),
                        new Component("id2", Instant.class, Dataset.Role.IDENTIFIER),
                        new Component("me1", Long.class, Dataset.Role.MEASURE)
                ),
                List.of("A", Instant.parse("2009-01-01T00:00:00Z"), 2L),
                List.of("A", Instant.parse("2011-01-01T00:00:00Z"), 5L),
                List.of("A", Instant.parse("2012-01-01T00:00:00Z"), -3L),
                List.of("B", Instant.parse("2010-01-01T00:00:00Z"), 9L),
                List.of("B", Instant.parse("2011-01-01T00:00:00Z"), 4L),
                List.of("B", Instant.parse("2013-01-01T00:00:00Z"), -6L)
        );
        engine.put("ds", ds);
        engine.eval("r := flow_to_stock(ds);");
        assertThat(engine.get("r")).isInstanceOf(Dataset.class);
        assertThat(((Dataset) engine.get("r")).getDataAsList()).containsExactlyInAnyOrder(
                List.of("A", Instant.parse("2009-01-01T00:00:00Z"), 2L),
                List.of("A", Instant.parse("2011-01-01T00:00:00Z"), 7L),
                List.of("A", Instant.parse("2012-01-01T00:00:00Z"), 4L),
                List.of("B", Instant.parse("2010-01-01T00:00:00Z"), 9L),
                List.of("B", Instant.parse("2011-01-01T00:00:00Z"), 13L),
                List.of("B", Instant.parse("2013-01-01T00:00:00Z"), 7L)
        );
    }

    @Test
    public void testStockToFlow() throws ScriptException {
        var ds = new InMemoryDataset(
                List.of(
                        new Component("id1", String.class, Dataset.Role.IDENTIFIER),
                        new Component("id2", Instant.class, Dataset.Role.IDENTIFIER),
                        new Component("me1", Long.class, Dataset.Role.MEASURE)
                ),
                List.of("A", Instant.parse("2010-01-01T00:00:00Z"), 2L),
                List.of("A", Instant.parse("2011-01-01T00:00:00Z"), 5L),
                List.of("A", Instant.parse("2012-01-01T00:00:00Z"), -3L),
                List.of("A", Instant.parse("2013-01-01T00:00:00Z"), 9L),
                List.of("A", Instant.parse("2014-01-01T00:00:00Z"), 4L)
        );
        engine.put("ds", ds);
        engine.eval("r := flow_to_stock(ds);");
        assertThat(engine.get("r")).isInstanceOf(Dataset.class);
        assertThat(((Dataset) engine.get("r")).getDataAsList()).containsExactly(
                List.of("A", Instant.parse("2010-01-01T00:00:00Z"), 2L),
                List.of("A", Instant.parse("2011-01-01T00:00:00Z"), 7L),
                List.of("A", Instant.parse("2012-01-01T00:00:00Z"), 4L),
                List.of("A", Instant.parse("2013-01-01T00:00:00Z"), 13L),
                List.of("A", Instant.parse("2014-01-01T00:00:00Z"), 17L)
        );
    }
}
