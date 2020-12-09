package fr.insee.vtl.engine.visitors.expression.functions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JoinFunctionsTest {
    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    void testLeftJoin() {
        assertThatThrownBy(() -> engine.eval("result := left_join(a, b, c)"))
                .hasMessage("TODO: left_join");
    }

    @Test
    void testInnerJoin() {
        assertThatThrownBy(() -> engine.eval("result := inner_join(a, b, c)"))
                .hasMessage("TODO: inner_join");
    }

    @Test
    void testCrossJoin() {
        assertThatThrownBy(() -> engine.eval("result := cross_join(a, b, c)"))
                .hasMessage("TODO: cross_join");
    }

    @Test
    void testFullJoin() {
        assertThatThrownBy(() -> engine.eval("result := full_join(a, b, c)"))
                .hasMessage("TODO: full_join");
    }
}