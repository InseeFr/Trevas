package fr.insee.vtl.engine;

import com.github.hervian.reflection.Fun;
import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.VtlScriptException;
import fr.insee.vtl.engine.exceptions.VtlSyntaxException;
import fr.insee.vtl.engine.processors.InMemoryProcessingEngine;
import fr.insee.vtl.engine.visitors.expression.ComparisonVisitor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.engine.visitors.expression.functions.GenericFunctionsVisitor;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.LongExpression;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.StringExpression;
import fr.insee.vtl.model.Structured;
import org.assertj.core.api.Condition;
import org.jetbrains.annotations.NotNull;
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.registerCustomDateFormat;

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
            return position.getStartLine().equals(startLine) &&
                    position.getEndLine().equals(endLine) &&
                    position.getStartColumn().equals(startColumn) &&
                    position.getEndColumn().equals(endColumn);
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

    public static class MathFunctions {
        public static Double ceil(Double op) {
            if (op == null) {
                return null;
            }
            return Math.ceil(op);
        }
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
        var dsOperand = DatasetExpression.of(ds);

        var ceilMethod = MathFunctions.class.getMethod("ceil", Double.class);
        var ceilDsExpression = new GenericFunctionsVisitor.DatasetFunctionExpression(ceilMethod, dsOperand);
        Dataset resolve = ceilDsExpression.resolve(Map.of());
        System.out.println(resolve.getDataAsMap());

        VtlScriptEngine engine = (VtlScriptEngine) this.engine;
        engine.registerMethod("ceil2", ceilMethod);
        ScriptContext context = engine.getContext();
        context.setAttribute("ds", ds, ScriptContext.ENGINE_SCOPE);

        Object res1 = engine.eval("res1 := ceil(ds);");
        assertThat(res1).isNotNull();
        System.out.println("Builtin");
        System.out.println(((Dataset) res1).getDataAsMap());

        Object res2 = engine.eval("res2 := abs( ceil( ceil2( ds ) ) );");
        assertThat(res2).isNotNull();
        System.out.println("Custom");
        System.out.println(((Dataset) res2).getDataAsMap());
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
        VtlScriptEngine engine = (VtlScriptEngine) this.engine;
        var o = new PipedOutputStream();
        var i = new PipedInputStream(o, 8192);
        var out = new ObjectOutputStream(o);
        var in = new ObjectInputStream(i);

        var bar = StringExpression.of("bar");
        var baz = StringExpression.of("baz");
        var exprVisitor = new ExpressionVisitor(Map.of(), new InMemoryProcessingEngine(), engine);
        var comparisonVisitor = new ComparisonVisitor(exprVisitor);
        var condition = comparisonVisitor.compareExpressions(bar, baz, VtlScriptEngineTest::isEqual);
        var expr = ResolvableExpression.withTypeCasting(String.class, (clazz, ctx) ->
                condition.resolve(ctx) ?
                        clazz.cast(bar.resolve(ctx)) : clazz.cast(baz.resolve(ctx)));
        out.writeObject(expr);

        var res = in.readObject();
        System.out.println(res);
    }

}
