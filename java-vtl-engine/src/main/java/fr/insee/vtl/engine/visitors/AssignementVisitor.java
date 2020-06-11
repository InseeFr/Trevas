package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.ScriptContext;
import java.util.Objects;

public class AssignementVisitor extends VtlBaseVisitor<Object> {

    private final ScriptContext context;
    private final ExpressionVisitor expressionVisitor;

    public AssignementVisitor(ScriptContext context) {
        this.context = Objects.requireNonNull(context);
        expressionVisitor = new ExpressionVisitor();
    }

    @Override
    public Object visitTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
        ResolvableExpression resolvableExpression = expressionVisitor.visit(ctx.expr());
        Object assignedObject = resolvableExpression.resolve(context);
        String variableIdentifier = ctx.varID().getText();
        context.setAttribute(variableIdentifier, assignedObject, ScriptContext.ENGINE_SCOPE);
        return assignedObject;
    }
}
