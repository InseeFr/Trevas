package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;

public class VarIdVisitor extends VtlBaseVisitor<ResolvableExpression> {

    @Override
    public ResolvableExpression visitVarIdExpr(VtlParser.VarIdExprContext ctx) {
        // TODO: Maybe extract in its own class?
        return new ResolvableExpression() {
            @Override
            public Object resolve(ScriptContext context) {
                return context.getAttribute(ctx.getText());
            }

            @Override
            public Class<?> getType(ScriptContext context) {
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
