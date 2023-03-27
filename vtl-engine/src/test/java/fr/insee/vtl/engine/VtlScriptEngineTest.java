package fr.insee.vtl.engine;

import com.github.hervian.reflection.Fun;
import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.VtlSyntaxException;
import fr.insee.vtl.engine.processors.InMemoryProcessingEngine;
import fr.insee.vtl.engine.visitors.expression.ComparisonVisitor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.exceptions.InvalidTypeException;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class VtlScriptEngineTest {

    private ScriptEngine engine;

    public static <T extends Throwable> Condition<T> atPosition(Integer line, Integer startColumn, Integer endColumn) {
        return atPosition(line, line, startColumn, endColumn);
    }

    public static <T extends Throwable> Condition<T> atPosition(Integer startLine, Integer endLine,
                                                                Integer startColumn, Integer endColumn) {
        return new Condition<>(throwable -> {
            var scriptException = (VtlScriptException) throwable;
            var position = scriptException.getPosition();
            return position.startLine.equals(startLine) &&
                    position.endLine.equals(endLine) &&
                    position.startColumn.equals(startColumn) &&
                    position.endColumn.equals(endColumn);
        }, "at position <%d:%d-%d:%d>",
                startLine, endLine, startColumn, endColumn);
    }

    private static <T extends Comparable<T>> boolean isEqual(T left, T right) {
        return left.compareTo(right) == 0;
    }

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testProcessingEngines() {
        VtlScriptEngine vtlScriptEngine = (VtlScriptEngine) engine;
        ProcessingEngine processingEngines = vtlScriptEngine.getProcessingEngine();
        assertThat(processingEngines).isNotNull();
    }

    @Test
    public void testFunctions2() throws NoSuchMethodException, ScriptException {

        InMemoryDataset ds = new InMemoryDataset(
                List.of(
                        new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("age", Double.class, Dataset.Role.MEASURE),
                        new Structured.Component("weight", Double.class, Dataset.Role.MEASURE)
                ),
                Arrays.asList("Toto", null, 12.34),
                Arrays.asList("Hadrien", 12.34, 12.34),
                Arrays.asList("Nico", 12.34, 12.34)
        );

        var ceilMethod = MathFunctions.class.getMethod("ceil", Double.class);

        VtlScriptEngine engine = (VtlScriptEngine) this.engine;
        engine.registerMethod("myCustomCeil", ceilMethod);
        ScriptContext context = engine.getContext();
        context.setAttribute("ds", ds, ScriptContext.ENGINE_SCOPE);

        Object res1 = engine.eval("res := myCustomCeil(ds);");
        assertThat(res1).isNotNull();
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
                .hasMessage("invalid type Long, expected Boolean");
    }

    @Test
    public void testFunctions() throws ScriptException, NoSuchMethodException {
        VtlScriptEngine engine = (VtlScriptEngine) this.engine;
        engine.registerMethod("testTrim", TextFunctions.class.getMethod("trim", String.class));
        engine.registerMethod("testUpper", Fun.toMethod(TextFunctions::upper));

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

    @Test
    public void testSerialization() throws Exception {
//        VtlScriptEngine engine = (VtlScriptEngine) this.engine;
//        var o = new PipedOutputStream();
//        var i = new PipedInputStream(o, 8192);
//        var out = new ObjectOutputStream(o);
//        var in = new ObjectInputStream(i);
//
//        var bar = ResolvableExpression.withType(String.class).using(c -> "bar");
//        var baz = ResolvableExpression.withType(String.class).using(c -> "baz");
//        var exprVisitor = new ExpressionVisitor(Map.of(), new InMemoryProcessingEngine(), engine);
//        var comparisonVisitor = new ComparisonVisitor(exprVisitor);
//        var condition = comparisonVisitor.compareExpressions(bar, baz, VtlScriptEngineTest::isEqual);
//        var expr = ResolvableExpression.withTypeCasting(String.class, (clazz, ctx) ->
//                condition.resolve(ctx) ?
//                        clazz.cast(bar.resolve(ctx)) : clazz.cast(baz.resolve(ctx)));
//        out.writeObject(expr);
//
//        var res = in.readObject();
//        System.out.println(res);
    }

    public static class MathFunctions {
        public static Double ceil(Double op) {
            if (op == null) {
                return null;
            }
            return Math.ceil(op);
        }
    }

}
