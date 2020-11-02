package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import java.util.Objects;

public class SetFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor expressionVisitor;

    public SetFunctionsVisitor(ExpressionVisitor expressionVisitor) {
        this.expressionVisitor = Objects.requireNonNull(expressionVisitor);
    }

    @Override
    public ResolvableExpression visitUnionAtom(VtlParser.UnionAtomContext ctx) {
        throw new UnsupportedOperationException("TODO");
    }
}
