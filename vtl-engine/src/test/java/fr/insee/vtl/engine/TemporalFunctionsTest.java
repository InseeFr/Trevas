package fr.insee.vtl.engine;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.threeten.extra.Interval;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import java.util.List;
import java.util.Map;

import static fr.insee.vtl.model.Dataset.*;
import static org.assertj.core.api.Assertions.assertThat;

class TemporalFunctionsTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void periodIndicator() throws ScriptException {

        engine.eval("d1 := cast(\"2012Q1\", time_period , \"YYYY\\Qq\");");
        Object d1 = engine.getBindings(ScriptContext.ENGINE_SCOPE).get("d1");
        System.out.println(d1);
        throw new UnsupportedOperationException("FIX ME");
        // D Day -
        // W Week -
        // M Month -
        // Q Quarter -
        // S Semester -
        // A Year -


    }

    @Test
    public void testTimeshift() throws ScriptException {
        Dataset ds1 = new InMemoryDataset(
                new DataStructure(List.of(
                        new Component("id", String.class, Role.IDENTIFIER),
                        new Component("time", Interval.class, Role.IDENTIFIER),
                        new Component("measure", Long.class, Role.MEASURE)
                )),
                List.of("a", Interval.parse("2010-01-01T00:00:00Z/P1Y"), 1L),
                List.of("b", Interval.parse("2011-01-01T00:00:00Z/P1Y"), 2L),
                List.of("c", Interval.parse("2012-01-01T00:00:00Z/P1Y"), 4L),
                List.of("d", Interval.parse("2013-01-01T00:00:00Z/P1Y"), 8L)
        );
        engine.getBindings(ScriptContext.ENGINE_SCOPE).put("ds1", ds1);

        engine.eval("d1 := timeshift(ds1, -1);");
        Object d1 = engine.getBindings(ScriptContext.ENGINE_SCOPE).get("d1");
        assertThat(d1).isInstanceOf(Dataset.class);
        assertThat(((Dataset)d1).getDataAsMap()).containsExactly(
                Map.of("id", "a", "measure", 1L, "time", Interval.parse("2009-01-01T00:00:00Z/P1Y")),
                Map.of("id", "b", "measure", 2L, "time", Interval.parse("2010-01-01T00:00:00Z/P1Y")),
                Map.of("id", "c", "measure", 4L, "time", Interval.parse("2011-01-01T00:00:00Z/P1Y")),
                Map.of("id", "d", "measure", 8L, "time", Interval.parse("2012-01-01T00:00:00Z/P1Y"))
        );

        engine.getBindings(ScriptContext.ENGINE_SCOPE).put("t", Interval.parse("2010-01-01T00:00:00Z/P1Y"));
        engine.eval("tt := timeshift(t, 10");
        Object tt = engine.getBindings(ScriptContext.ENGINE_SCOPE).get("d1");
        assertThat(tt).isInstanceOf(Interval.class);
        assertThat(((Interval)tt)).isEqualTo(Interval.parse("2010-01-01T00:00:00Z/P1Y"));
    }


}