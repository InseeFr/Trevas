package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.exceptions.UndefinedVariableException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

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
        if (!context.getBindings(ScriptContext.ENGINE_SCOPE).containsKey(variableName)) {
            throw new VtlRuntimeException(new UndefinedVariableException(ctx));
        }

        // TODO: Fix this.
        if (context.getAttribute(variableName) instanceof DatasetExpression) {
            return (ResolvableExpression) context.getAttribute(variableName);
        }

        // TODO: Maybe extract in its own class?
        return new ResolvableExpression() {

            @Override
            public Object resolve(Map<String, Object> context) {
                Object value = context.get(variableName);
                if (value instanceof Integer) {
                    return ((Integer) value).longValue();
                } else if (value instanceof Float) {
                    return ((Float) value).doubleValue();
                } else {
                    return value;
                }
            }

            @Override
            public Class<?> getType() {
                Object value = context.getAttribute(variableName);
                if (value == null) {
                    return Object.class;
                } else {
                    Class<?> valueClass = value.getClass();
                    if (valueClass.equals(Integer.class)) {
                        return Long.class;
                    } else if (valueClass.equals(Float.class)) {
                        return Double.class;
                    } else {
                        return valueClass;
                    }
                }
            }
        };
    }
}
