package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;
import java.util.Objects;

public class VarIdVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ScriptContext context;

    public VarIdVisitor(ScriptContext context) {
        this.context = Objects.requireNonNull(context);
    }

    @Override
    public ResolvableExpression visitVarIdExpr(VtlParser.VarIdExprContext ctx) {
        // TODO: Maybe extract in its own class?
        return new ResolvableExpression() {
            @Override
            public Object resolve(ScriptContext context) {
                return context.getAttribute(ctx.getText());
            }

            @Override
            public Class<?> getType() {
                Object value = context.getAttribute(ctx.getText());
                if (value == null) {
                    return Object.class;
                } else {
                    return value.getClass();
                }
            }
        };
    }
}
