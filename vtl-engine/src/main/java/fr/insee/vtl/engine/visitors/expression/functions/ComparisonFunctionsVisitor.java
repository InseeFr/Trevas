package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.exceptions.ConflictingTypesException;
import fr.insee.vtl.engine.exceptions.InvalidTypeException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.utils.TypeChecking;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ComparisonFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    public ComparisonFunctionsVisitor(ScriptContext context) {
        exprVisitor = new ExpressionVisitor(context);
    }


    @Override
    public ResolvableExpression visitBetweenAtom(VtlParser.BetweenAtomContext ctx) {
        ResolvableExpression operandExpression = exprVisitor.visit(ctx.op);
        ResolvableExpression fromExpression = exprVisitor.visit(ctx.from_);
        ResolvableExpression toExpression = exprVisitor.visit(ctx.to_);

        if (operandExpression.getType() != fromExpression.getType() &&
                operandExpression.getType() != toExpression.getType())
            throw new VtlRuntimeException(
                    new ConflictingTypesException(ctx, List.of(operandExpression.getType(),
                            fromExpression.getType(), toExpression.getType()))
            );
        // TODO: handle other types (dates?)
        if (TypeChecking.isLong(operandExpression))
            return ResolvableExpression.withType(Boolean.class, context -> {
                Long operandValue = (Long) operandExpression.resolve(context);
                Long fromValue = (Long) fromExpression.resolve(context);
                Long toValue = (Long) toExpression.resolve(context);
                return operandValue >= fromValue && operandValue <= toValue;
            });
        return ResolvableExpression.withType(Boolean.class, context -> {
            Double operandValue = (Double) operandExpression.resolve(context);
            Double fromValue = (Double) fromExpression.resolve(context);
            Double toValue = (Double) toExpression.resolve(context);
            return operandValue >= fromValue && operandValue <= toValue;
        });
    }

    @Override
    public ResolvableExpression visitCharsetMatchAtom(VtlParser.CharsetMatchAtomContext ctx) {
        ResolvableExpression operandExpression = exprVisitor.visit(ctx.op);
        ResolvableExpression patternExpression = exprVisitor.visit(ctx.pattern);

        if (!operandExpression.getType().equals(String.class)) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(ctx.op, String.class, operandExpression.getType())
            );
        }
        if (!patternExpression.getType().equals(String.class)) {
            throw new VtlRuntimeException(
                    new InvalidTypeException(ctx.pattern, String.class, patternExpression.getType())
            );
        }

        return ResolvableExpression.withType(Boolean.class, context -> {
            String operandValue = (String) operandExpression.resolve(context);
            String patternValue = (String) patternExpression.resolve(context);
            Pattern pattern = Pattern.compile(patternValue);
            Matcher matcher = pattern.matcher(operandValue);
            return matcher.matches();
        });
    }

    @Override
    public ResolvableExpression visitIsNullAtom(VtlParser.IsNullAtomContext ctx) {
        ResolvableExpression operandExpression = exprVisitor.visit(ctx.expr());
        return ResolvableExpression.withType(Boolean.class, context ->
                operandExpression.resolve(context) == null
        );
    }
    
}
