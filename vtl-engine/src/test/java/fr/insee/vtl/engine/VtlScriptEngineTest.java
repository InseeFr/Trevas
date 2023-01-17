package fr.insee.vtl.engine;

import com.github.hervian.reflection.Fun;
import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.VtlScriptException;
import fr.insee.vtl.engine.exceptions.VtlSyntaxException;
import fr.insee.vtl.engine.processors.InMemoryProcessingEngine;
import fr.insee.vtl.engine.visitors.expression.ComparisonVisitor;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
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

    public ResolvableExpression transposeExpression(ResolvableExpression expression) {

        return new DatasetExpression() {
            @Override
            public Dataset resolve(Map<String, Object> context) {
                return null;
            }

            @Override
            public DataStructure getDataStructure() {
                return null;
            }
        };
    }

    public ResolvableExpression ceilExpr(ResolvableExpression operand) {
        if (operand instanceof DatasetExpression) {
            return new DatasetExpression() {
                @Override
                public Dataset resolve(Map<String, Object> context) {
                    Dataset dsOperand = (Dataset) operand.resolve(context);
                    return null;
                }

                @Override
                public DataStructure getDataStructure() {
                    return null;
                }
            };
        } else {
            return LongExpression.of(context -> {
                Number exprNumber = (Number) operand.resolve(context);
                if (exprNumber == null) return null;
                return ((Double) (Math.ceil(exprNumber.doubleValue()))).longValue();
            });
        }

    }

    public static class MathFunctions {
        public static Double ceil(Double op) {
            if (op == null) {
                return null;
            }
            return Math.ceil(op);
        }
    }

    public static class DatasetFunctionExpression extends DatasetExpression {

        private final Method method;
        private final DatasetExpression operand;
        private final Map<Component, ResolvableExpression> expressions;
        private final DataStructure structure;

        public DatasetFunctionExpression(Method method, DatasetExpression operand) {
            this.method = Objects.requireNonNull(method);
            this.operand = Objects.requireNonNull(operand);

            if (this.method.getParameterTypes().length != 1) {
                throw new RuntimeException("only supports unary operators");
            }
            Class<?> parameterType = this.method.getParameterTypes()[0];

            this.expressions = createExpressionMap(method, operand, parameterType);

            List<Component> components = new ArrayList<>();
            for (Component component : operand.getDataStructure().values()) {
                if (expressions.containsKey(component)) {
                    components.add(new Component(
                            component.getName(),
                            expressions.get(component).getType(),
                            component.getRole()
                    ));
                } else {
                    components.add(component);
                }
            }
            this.structure = new DataStructure(components);

        }

        @NotNull
        private Map<Component, ResolvableExpression> createExpressionMap(Method method, DatasetExpression operand, Class<?> parameterType) {
            Map<Component, ResolvableExpression> parameters = new LinkedHashMap<>();
            for (Component component : operand.getDataStructure().values()) {
                if (!component.isMeasure()) {
                    continue;
                }
                if (component.getType() != parameterType) {
                    throw new RuntimeException("invalid type");
                }
                parameters.put(component, new FunctionExpression(method, List.of(new ComponentExpression(component))));
            }
            return Map.copyOf(parameters);
        }

        @Override
        public Dataset resolve(Map<String, Object> context) {
            // TODO: How to pass this to the engines? Maybe visitor pattern?
            var dataset = operand.resolve(context);
            List<List<Object>> result = dataset.getDataPoints().stream().map(dataPoint -> {
                var newDataPoint = new DataPoint(getDataStructure(), dataPoint);
                for (Component component : expressions.keySet()) {
                    newDataPoint.set(component.getName(), expressions.get(component).resolve(dataPoint));
                }
                return newDataPoint;
            }).collect(Collectors.toList());
            return new InMemoryDataset(result, getDataStructure());
        }

        @Override
        public DataStructure getDataStructure() {
            return this.structure;
        }
    }

    public static class FunctionExpression implements ResolvableExpression {

        private final Method method;
        private final List<ResolvableExpression> parameters;

        public FunctionExpression(Method method, List<ResolvableExpression> parameters) {
            this.method = Objects.requireNonNull(method);

            // Type check.
            // TODO: Add context.
            Class<?>[] methodParameterTypes = this.method.getParameterTypes();
            if (parameters.size() < methodParameterTypes.length) {
                throw new RuntimeException("too many parameters");
            } else if (parameters.size() > methodParameterTypes.length) {
                throw new RuntimeException("missing parameters");
            }
            for (int i = 0; i < parameters.size(); i++) {
                if (parameters.get(i).getType() != methodParameterTypes[i]) {
                    throw new RuntimeException(String.format("invalid parameter type %s, need %s",
                            parameters.get(i).getType(),
                            methodParameterTypes[i])
                    );
                }
            }

            this.parameters = Objects.requireNonNull(parameters);
        }

        @Override
        public Object resolve(Map<String, Object> context) {
            Object[] evaluatedParameters = parameters.stream().map(p -> p.resolve(context)).toArray();
            try {
                return method.invoke(null, evaluatedParameters);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Class<?> getType() {
            return method.getReturnType();
        }
    }

    public static class ComponentExpression implements ResolvableExpression {

        private final Structured.Component component;

        public ComponentExpression(Structured.Component component) {
            this.component = Objects.requireNonNull(component);
        }

        @Override
        public Object resolve(Map<String, Object> context) {
            return context.get(component.getName());
        }

        @Override
        public Class<?> getType() {
            return component.getType();
        }
    }

    @Test
    public void testFunctions2() throws NoSuchMethodException {

        // ds1 { i1 ident string, a measure int, b measure  int,   c measure int }
        // ds2 { i1 ident string, a measure int, b measure  int,   c attribute int }
        // ds3 { i1 ident string, a measure int, b measure string, c attribute int }

        // filter out attributes.
        // ds3 { i1 ident string, a measure int, b measure string, c attribute int }
        // ds3 { i1 ident string, a measure int, b measure string }

        // fail if all type != input type
        // ds3 { i1 ident string, a measure int }

        // ds1 := ceil(ds)

        // public DatasetExpression executeCalc(DatasetExpression expression, Map<String, ResolvableExpression> expressions,
        //                                         Map<String, Dataset.Role> roles, Map<String, String> expressionStrings) {

        var dsOperand = DatasetExpression.of(new InMemoryDataset(
                List.of(
                        new Structured.Component("name", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("age", Double.class, Dataset.Role.MEASURE),
                        new Structured.Component("weight", Double.class, Dataset.Role.MEASURE)
                ),
                Arrays.asList("Toto", null, 12.34),
                Arrays.asList("Hadrien", 12.34, 12.34),
                Arrays.asList("Nico", 12.34, 12.34)
        ));

        var ceilMethod = MathFunctions.class.getMethod("ceil", Double.class);
        var ceilDsExpression = new DatasetFunctionExpression(ceilMethod, dsOperand);
        Dataset resolve = ceilDsExpression.resolve(Map.of());
        System.out.println(resolve.getDataAsMap());

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
