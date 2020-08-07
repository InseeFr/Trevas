package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.UnsupportedTypeException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.Bindings;
import javax.script.ScriptContext;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class VarIdVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ScriptContext context;

    public VarIdVisitor(ScriptContext context) {
        this.context = Objects.requireNonNull(context);
    }

    @Override
    public ResolvableExpression visitVarIdExpr(VtlParser.VarIdExprContext ctx) {

        final String variableName = ctx.getText();
        final Bindings bindings = context.getBindings(ScriptContext.ENGINE_SCOPE);

        if (!bindings.containsKey(variableName)) {
            throw new VtlRuntimeException(new UndefinedVariableException(ctx));
        }

        Object value = bindings.get(variableName);
        if (value instanceof Dataset) {
            Dataset dataset = (Dataset) value;
            return new DatasetExpression() {

                @Override
                public List<Dataset.Component> getDataStructure() {
                    return dataset.getDataStructure();
                }

                @Override
                public Dataset resolve(Map<String, Object> na) {
                    return dataset;
                }
            };
        }

        if (value instanceof Integer || value instanceof Long) {
            return new LongExpression() {
                @Override
                public Long resolve(Map<String, Object> context) {
                    return ((Number) value).longValue();
                }
            };
        }

        if (value instanceof Float || value instanceof Double) {
            return new DoubleExpression() {
                @Override
                public Double resolve(Map<String, Object> context) {
                    return ((Number) value).doubleValue();
                }
            };
        }

        if (value instanceof Boolean) {
            return new BooleanExpression() {
                @Override
                public Boolean resolve(Map<String, Object> context) {
                    return (Boolean) value;
                }
            };
        }

        if (value instanceof CharSequence) {
            return new StringExpression() {
                @Override
                public String resolve(Map<String, Object> context) {
                    return (String) value;
                }
            };
        }

        if (value == null) {
            return new ResolvableExpression() {
                @Override
                public Object resolve(Map<String, Object> context) {
                    return null;
                }

                @Override
                public Class<?> getType() {
                    return Object.class;
                }
            };
        }

        throw new VtlRuntimeException(new UnsupportedTypeException(ctx, value.getClass()));

    }
}
