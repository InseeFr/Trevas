package fr.insee.vtl.engine;

import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.VtlScriptException;
import fr.insee.vtl.engine.exceptions.VtlSyntaxException;
import fr.insee.vtl.engine.processors.InMemoryProcessingEngine;
import fr.insee.vtl.engine.visitors.expression.ComparisonVisitor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.model.StringExpression;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
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
        var o = new PipedOutputStream();
        var i = new PipedInputStream(o, 8192);
        var out = new ObjectOutputStream(o);
        var in = new ObjectInputStream(i);

        var bar = StringExpression.of("bar");
        var baz = StringExpression.of("baz");
        var exprVisitor = new ExpressionVisitor(Map.of(), new InMemoryProcessingEngine());
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
