package fr.insee.vtl.engine;

import com.github.hervian.reflection.Fun;
import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.VtlSyntaxException;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.ProcessingEngine;
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
import java.util.Arrays;
import java.util.List;

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
    public void testFunctionsExpression() throws NoSuchMethodException, ScriptException {

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
    void testMultipleParameterFunctionExpressions() throws ScriptException {
        // TODO: Method with more that one dataset parameters.
        //          ie: parameterTypes.contains(Dataset.class)
        //        context.setAttribute("ds", ds, ScriptContext.ENGINE_SCOPE);
        //        Object res = engine.eval("res := mod(ds, 0);");

        // Examples:
        // round(Number, Number)

        // ds1 (id1, num1, num2)
        // ds2 (id1, num1, num2)

        // round(10,  ds2) (id1, num1, num2)
        // round(ds1, 10)
        // num1 := round(ds1#num1, 10)
        // num2 := round(ds1#num2, 10)
        // ...
        // inner_join(num1, num2, ...)

        // Invalid?
        // ds1(id, num1, num2, num3)
        // ds2(id, num1, num2)

        // Invalid?
        // fn(number, string, bool)
        // ds1(id, m1<string>, m2<number>, m3<number>)
        // ds2(id, mA<string>, mB<bool>)
        // ds3(id, mFoo<string>, mBar<bool>)
        // join(
        //      id, ds1#m1<string>, ds1#m2<number>, ds1#m3<number>,
        //      ds2#mA<string>, ds2#mB<bool>,
        //      ds3#mFoo<string>, ds3#mBar<bool>
        //      )
        // fn(ds1, ds2, ds3)
        //
        // ds1 (id, num1<number>, num2<number>)
        // ds2 (id, num1<number>, num2<number>)
        // join(
        //      id,
        //      ds1#num1<number>, ds1#num2<number>
        //      ds2#num1<number>, ds2#num2<number>
        //      )
        //
        // sum(ds1,  ds2) (id1, num1, num2)


        // res := ds1 < ds2
        // res(id, bool_var<boolean>)

        // res := ds1 + ds2
        // res(id, m1, m2, mN, ...)


        // round(ds1, ds2) (id1, num1, num2)
        // join := inner_join(ds1, ds2)
        // num1 := round(join#ds1_num1, join#ds2_num1) (id1, num1, num2)
        // num2 := round(join#_ds1num2, join#ds2_num2) (id1, num1, num2)
        // ...
        // inner_join(num1, num2, ...)

        // tmp := inner_join(ds1, ds2)
        // tmp := tmp[calc m1 := eq(ds1_m1, ds2_m1)
        // tmp := tmp[calc m2 := eq(ds1_m2, ds2_m2)

        // eq(Comparable, Comparable)
        // ds1 (id1, m1, m2)
        // ds2 (id1, m1, m2)

        // eq(ds1, ds2) -> (id1, m1<bool>, m2<bool>)
        // tmp := inner_join(ds1, ds2)
        // tmp := tmp[calc m1 := eq(ds1_m1, ds2_m1)
        // tmp := tmp[calc m2 := eq(ds1_m2, ds2_m2)

        var ds1 = new InMemoryDataset(List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("m1", Double.class, Dataset.Role.MEASURE)
        ),
                Arrays.asList("Toto", 12.2),
                Arrays.asList("Hadrien", 1.4),
                Arrays.asList("Nico", 12.34)
        );
        var ds2 = new InMemoryDataset(List.of(
                new Structured.Component("id", String.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("m1", Long.class, Dataset.Role.MEASURE)
        ),
                Arrays.asList("Toto", 1L),
                Arrays.asList("Hadrien", 2L),
                Arrays.asList("Nico", 3L)
        );
        engine.getContext().setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
        engine.getContext().setAttribute("ds2", ds2, ScriptContext.ENGINE_SCOPE);
        engine.eval("res := round(ds1, ds2);");

        System.out.println(((Dataset) engine.get("res")).getDataPoints());
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
                .is(atPosition(0, 1, 17, 2))
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
