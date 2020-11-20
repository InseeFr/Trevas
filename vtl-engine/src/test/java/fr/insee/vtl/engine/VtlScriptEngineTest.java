package fr.insee.vtl.engine;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.VtlScriptException;
import fr.insee.vtl.model.ProcessingEngine;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class VtlScriptEngineTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    public static <T extends Throwable> Condition<T> atPosition(Integer line, Integer startColumn, Integer endColumn) {
        return atPosition(line, line, startColumn, endColumn);
    }

    public static <T extends Throwable> Condition<T> atPosition(Integer startLine, Integer endLine,
                                                                Integer startColumn, Integer endColumn) {
        return new Condition<>(throwable -> {
            var scriptException = (VtlScriptException) throwable;
            var position = scriptException.getPosition();
            return position.getStartLine().equals(startLine) &&
                    position.getEndLine().equals(endLine) &&
                    position.getStartColumn().equals(startColumn) &&
                    position.getEndColumn().equals(endColumn);
        }, "at position <%d:%d-%d:%d>",
                startLine, endLine, startColumn, endColumn);
    }

    @Test
    public void testProcessingEngines() {
        VtlScriptEngine vtlScriptEngine = (VtlScriptEngine) engine;
        ProcessingEngine processingEngines = vtlScriptEngine.getProcessingEngine();
        assertThat(processingEngines).isNotNull();
    }

    @Test
    public void testExceptions() {
        assertThatThrownBy(() -> {
            engine.eval("var := undefinedVariable + 42;");
        }).isInstanceOf(UndefinedVariableException.class)
                .is(atPosition(0, 7, 24))
                .hasMessage("undefined variable undefinedVariable");

        assertThatThrownBy(() -> {
            engine.eval("var := true and (10 +\n" +
                    "10);");
        }).isInstanceOf(InvalidTypeException.class)
                .is(atPosition(0, 1, 16, 3))
                .hasMessage("invalid type Long, expected (10+10) to be Boolean");
    }
}