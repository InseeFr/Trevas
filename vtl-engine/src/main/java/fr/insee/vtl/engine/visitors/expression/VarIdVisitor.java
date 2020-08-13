package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.UnsupportedTypeException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.Bindings;
import javax.script.ScriptContext;
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
            return DatasetExpression.of((Dataset) value);

        }

        if (value instanceof Integer || value instanceof Long) {
            return LongExpression.of(((Number) value).longValue());
        }

        if (value instanceof Float || value instanceof Double) {
            return DoubleExpression.of(((Number) value).doubleValue());
        }

        if (value instanceof Boolean) {
            return BooleanExpression.of((Boolean) value);
        }

        if (value instanceof CharSequence) {
            return StringExpression.of((CharSequence) value);
        }

        if (value == null) {
            // TODO: Should probably be a static value.
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
