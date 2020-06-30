package fr.insee.vtl.engine.visitors.expression;

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

        // TODO: Fix this.
        if (context.getAttribute(ctx.getText()) instanceof DatasetExpression) {
            return (ResolvableExpression) context.getAttribute(ctx.getText());
        }

        // TODO: Maybe extract in its own class?
        return new ResolvableExpression() {
            @Override
            public Object resolve(ScriptContext context) {
                Object value = context.getAttribute(ctx.getText());
                if (value instanceof Integer) {
                    return ((Integer) value).longValue();
                } else if (value instanceof Float) {
                    return ((Float) value).doubleValue();
                } else {
                    return value;
                }
            }

            @Override
            public Object resolve(Map<String, Object> context) {
                throw new UnsupportedOperationException("TODO: refactor");
            }

            @Override
            public Class<?> getType() {
                Object value = context.getAttribute(ctx.getText());
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
