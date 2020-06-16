package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;

public class ExpressionVisitor extends VtlBaseVisitor<ResolvableExpression> {

    @Override
    public ResolvableExpression visitConstant(VtlParser.ConstantContext ctx) {
        if (ctx.INTEGER_CONSTANT() != null) {
            return ResolvableExpression.withType(Long.class, context -> Long.parseLong(ctx.getText()));
        }
        if (ctx.NUMBER_CONSTANT() != null) {
            return ResolvableExpression.withType(Double.class, context -> Double.parseDouble(ctx.getText()));
        }
        if (ctx.BOOLEAN_CONSTANT() != null) {
            return ResolvableExpression.withType(Boolean.class, context -> Boolean.parseBoolean(ctx.getText()));
        }
        if (ctx.STRING_CONSTANT() != null) {
            return ResolvableExpression.withType(String.class, context -> {
                String text = ctx.getText();
                return text.substring(1, text.length() - 1);
            });
        }
        if (ctx.NULL_CONSTANT() != null) {
            return ResolvableExpression.withType(Object.class, context -> null);
        }
        throw new UnsupportedOperationException("unknown constant type " + ctx);
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

    @Override
    public ResolvableExpression visitBooleanExpr(VtlParser.BooleanExprContext ctx) {
        ResolvableExpression leftExpression = visit(ctx.left);
        ResolvableExpression rightExpression = visit(ctx.right);
        switch (ctx.op.getType()) {
            case VtlParser.AND:
                return ResolvableExpression.withType(Boolean.class, context -> {
                    Boolean leftValue = (Boolean) leftExpression.resolve(context);
                    Boolean rightValue = (Boolean) rightExpression.resolve(context);
                    return leftValue && rightValue;
                });
            case VtlParser.OR:
                return ResolvableExpression.withType(Boolean.class, context -> {
                    Boolean leftValue = (Boolean) leftExpression.resolve(context);
                    Boolean rightValue = (Boolean) rightExpression.resolve(context);
                    return leftValue || rightValue;
                });
            case VtlParser.XOR:
                return ResolvableExpression.withType(Boolean.class, context -> {
                    Boolean leftValue = (Boolean) leftExpression.resolve(context);
                    Boolean rightValue = (Boolean) rightExpression.resolve(context);
                    return leftValue ^ rightValue;
                });
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }
    }

    @Override
    public ResolvableExpression visitArithmeticExpr(VtlParser.ArithmeticExprContext ctx) {
        // TODO: deal with Long & Double dynamically
        ResolvableExpression leftExpression = visit(ctx.left);
        ResolvableExpression rightExpression = visit(ctx.right);
        switch (ctx.op.getType()) {
            case VtlParser.MUL:
                return ResolvableExpression.withType(Long.class, context -> {
                    Long leftValue = (Long) leftExpression.resolve(context);
                    Long rightValue = (Long) rightExpression.resolve(context);
                    return leftValue * rightValue;
                });
            case VtlParser.DIV:
                return ResolvableExpression.withType(Long.class, context -> {
                    Long leftValue = (Long) leftExpression.resolve(context);
                    Long rightValue = (Long) rightExpression.resolve(context);
                    return leftValue / rightValue;
                });
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }
    }

    @Override
    public ResolvableExpression visitArithmeticExprOrConcat(VtlParser.ArithmeticExprOrConcatContext ctx) {
        // TODO: deal with Long & Double dynamically
        ResolvableExpression leftExpression = visit(ctx.left);
        ResolvableExpression rightExpression = visit(ctx.right);
        switch (ctx.op.getType()) {
            case VtlParser.PLUS:
                return ResolvableExpression.withType(Long.class, context -> {
                    Long leftValue = (Long) leftExpression.resolve(context);
                    Long rightValue = (Long) rightExpression.resolve(context);
                    return leftValue + rightValue;
                });
            case VtlParser.MINUS:
                return ResolvableExpression.withType(Long.class, context -> {
                    Long leftValue = (Long) leftExpression.resolve(context);
                    Long rightValue = (Long) rightExpression.resolve(context);
                    return leftValue - rightValue;
                });
            case VtlParser.CONCAT:
                return ResolvableExpression.withType(Long.class, context -> {
                    Long leftValue = (Long) leftExpression.resolve(context);
                    Long rightValue = (Long) rightExpression.resolve(context);
                    return Long.parseLong((leftValue.toString() + rightValue.toString()));
                });
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }
    }

    @Override
    public ResolvableExpression visitParenthesisExpr(VtlParser.ParenthesisExprContext ctx) {
        return visit(ctx.expr());
    }
}
