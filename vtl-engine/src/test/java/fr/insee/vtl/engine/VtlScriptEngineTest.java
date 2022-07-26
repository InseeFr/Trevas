package fr.insee.vtl.engine;

import com.github.hervian.reflection.Fun;
import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.VtlScriptException;
import fr.insee.vtl.engine.exceptions.VtlSyntaxException;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.Structured;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Arrays;
import java.util.List;

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

    @Test
    public void testFunctions() throws ScriptException, NoSuchMethodException {
        VtlScriptEngine engine = (VtlScriptEngine) this.engine;
        engine.registerMethod("testTrim", Trim.class.getMethod("trim", String.class));
        engine.registerMethod("testUpper", Fun.toMethod(Trim::upper));

        engine.eval("" +
                "res := testUpper(\"  foo bar \");\n" +
                "res := testTrim(res);" +
                "");
        assertThat(engine.get("res")).isEqualTo("FOO BAR");
    }

    @Test
    public void testSyntaxError() {
        assertThatThrownBy(() -> {
            engine.eval("var := 40 + 42");
        })
                .isInstanceOf(VtlSyntaxException.class)
                .hasMessage("missing ';' at '<EOF>'")
                .is(atPosition(0, 14, 14));
    }

    public static class Trim {
        public static String trim(String str) {
            return str.trim();
        }

        public static String upper(String str) {
            return str.toUpperCase();
        }

        public static Dataset loadS3() {
            return new InMemoryDataset(
                    List.of(
                            new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                            new Structured.Component("age", Long.class, Dataset.Role.MEASURE),
                            new Structured.Component("weight", Long.class, Dataset.Role.MEASURE)
                    ),
                    Arrays.asList("Toto", null, 100L),
                    Arrays.asList("Hadrien", 10L, 11L),
                    Arrays.asList("Nico", 11L, 10L),
                    Arrays.asList("Franck", 12L, 9L)
            );
        }
    }
}
