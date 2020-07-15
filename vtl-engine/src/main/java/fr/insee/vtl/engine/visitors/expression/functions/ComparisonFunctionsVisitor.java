package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ComparisonFunctionsVisitor extends VtlBaseVisitor<ResolvableExpression> {

    private final ExpressionVisitor exprVisitor;

    public ComparisonFunctionsVisitor(ScriptContext context) {
        exprVisitor = new ExpressionVisitor(context);;
    }


    // betweenAtom

    @Override
    public ResolvableExpression visitCharsetMatchAtom(VtlParser.CharsetMatchAtomContext ctx) {
        ResolvableExpression operandExpression = exprVisitor.visit(ctx.op);
        ResolvableExpression patternExpression = exprVisitor.visit(ctx.pattern);
        return ResolvableExpression.withType(Boolean.class, context -> {
            String operandValue = (String) operandExpression.resolve(context);
            String patternValue = (String) patternExpression.resolve(context);
            Pattern pattern = Pattern.compile(patternValue);
            Matcher matcher = pattern.matcher(operandValue);
            return matcher.matches();
        });
    }

    // isNullAtom

    // existInAtom
}
