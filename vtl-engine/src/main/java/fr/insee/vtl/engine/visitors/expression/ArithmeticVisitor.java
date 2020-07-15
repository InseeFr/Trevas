package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;
import java.util.List;

public class ArithmeticVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    public ArithmeticVisitor(ScriptContext context) {
        exprVisitor = new ExpressionVisitor(context);;
    }

    @Override
    public ResolvableExpression visitArithmeticExpr(VtlParser.ArithmeticExprContext ctx) {
        // TODO: deal with Long & Double dynamically
        ResolvableExpression leftExpression = exprVisitor.visit(ctx.left);
        ResolvableExpression rightExpression = exprVisitor.visit(ctx.right);

        // TODO: Explore the possibility of using subclasses for all expressions
        //  NumberExpression, LongExpression etc.
        if (TypeChecking.isNumber(leftExpression) && TypeChecking.isNumber(rightExpression)) {
            return handleNumberArithmeticExpr(leftExpression, rightExpression, ctx);
        } if (TypeChecking.isDataset(leftExpression) && TypeChecking.isDataset(leftExpression)) {
            throw new UnsupportedOperationException("TODO");
        } else {
            throw new UnsupportedOperationException("unsupported types");
        }

    }

    private ResolvableExpression handleMultiplication(ResolvableExpression leftExpression,
                                                      ResolvableExpression rightExpression) {
        if (TypeChecking.isLong(leftExpression) && TypeChecking.isDouble(rightExpression)) {
            return ResolvableExpression.withType(Double.class, context -> {
                return ((Long) leftExpression.resolve(context)) * ((Double) rightExpression.resolve(context));
            });
        } else if (TypeChecking.isDouble(leftExpression) && TypeChecking.isLong(rightExpression)) {
            return ResolvableExpression.withType(Double.class, context -> {
                return ((Double) leftExpression.resolve(context)) * ((Long) rightExpression.resolve(context));
            });
        } else if (TypeChecking.isLong(leftExpression) && TypeChecking.isLong(rightExpression)) {
            return ResolvableExpression.withType(Long.class, context -> {
                return ((Long) leftExpression.resolve(context)) * ((Long) rightExpression.resolve(context));
            });
        } else if (TypeChecking.isDouble(leftExpression) && TypeChecking.isDouble(rightExpression)) {
            return ResolvableExpression.withType(Double.class, context -> {
                return ((Double) leftExpression.resolve(context)) * ((Double) rightExpression.resolve(context));
            });
        } else {
            throw new UnsupportedOperationException("unsupported types " + List.of(leftExpression, rightExpression));
        }
    }

    private ResolvableExpression handleDivision(ResolvableExpression leftExpression,
                                                ResolvableExpression rightExpression) {
        if (TypeChecking.isLong(leftExpression) && TypeChecking.isDouble(rightExpression)) {
            return ResolvableExpression.withType(Double.class, context -> {
                return ((Long) leftExpression.resolve(context)) / ((Double) rightExpression.resolve(context));
            });
        } else if (TypeChecking.isDouble(leftExpression) && TypeChecking.isLong(rightExpression)) {
            return ResolvableExpression.withType(Double.class, context -> {
                return ((Double) leftExpression.resolve(context)) / ((Long) rightExpression.resolve(context));
            });
        } else if (TypeChecking.isLong(leftExpression) && TypeChecking.isLong(rightExpression)) {
            return ResolvableExpression.withType(Double.class, context -> {
                // Note the required cast to avoid returning Long type.
                return ((Long) leftExpression.resolve(context)).doubleValue() / ((Long) rightExpression.resolve(context));
            });
        } else if (TypeChecking.isDouble(leftExpression) && TypeChecking.isDouble(rightExpression)) {
            return ResolvableExpression.withType(Double.class, context -> {
                return ((Double) leftExpression.resolve(context)) / ((Double) rightExpression.resolve(context));
            });
        } else {
            throw new UnsupportedOperationException("unsupported types " + List.of(leftExpression, rightExpression));
        }
    }

    private ResolvableExpression handleNumberArithmeticExpr(ResolvableExpression leftExpression,
                                                            ResolvableExpression rightExpression,
                                                            VtlParser.ArithmeticExprContext ctx) {
        switch (ctx.op.getType()) {
            case VtlParser.MUL:
                return handleMultiplication(leftExpression, rightExpression);
            case VtlParser.DIV:
                return handleDivision(leftExpression, rightExpression);
            default:
                throw new UnsupportedOperationException("unknown operator " + ctx);
        }


    }
}
