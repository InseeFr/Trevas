package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.DatasetExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.Objects;

public class JoinFunctionsVisitor extends VtlBaseVisitor<DatasetExpression> {

    private final ExpressionVisitor expressionVisitor;

    public JoinFunctionsVisitor(ExpressionVisitor expressionVisitor) {
        this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
    }

    @Override
    public DatasetExpression visitJoinExpr(VtlParser.JoinExprContext ctx) {
        if (ctx.LEFT_JOIN() != null) {
            return leftJoin(ctx);
        } else if (ctx.INNER_JOIN() != null) {
            return innerJoin(ctx);
        } else if (ctx.FULL_JOIN() != null) {
            return fullJoin(ctx);
        } else if (ctx.CROSS_JOIN() != null) {
            return crossJoin(ctx);
        }
        throw new UnsupportedOperationException("unknown join type");
    }

    private DatasetExpression leftJoin(VtlParser.JoinExprContext ctx) {
        throw new UnsupportedOperationException("TODO: left_join");
    }

    private DatasetExpression crossJoin(VtlParser.JoinExprContext ctx) {
        throw new UnsupportedOperationException("TODO: cross_join");
    }

    private DatasetExpression fullJoin(VtlParser.JoinExprContext ctx) {
        throw new UnsupportedOperationException("TODO: full_join");
    }

    private DatasetExpression innerJoin(VtlParser.JoinExprContext ctx) {
        throw new UnsupportedOperationException("TODO: inner_join");
    }


}
