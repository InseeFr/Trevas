package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ResolvableExpression;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;

import javax.script.Bindings;
import javax.script.ScriptContext;
import java.util.Objects;

public class AssignmentVisitor extends VtlBaseVisitor<Object> {

    private final ScriptContext context;
    private final ExpressionVisitor expressionVisitor;

    public AssignmentVisitor(ScriptContext context, ProcessingEngine processingEngine) {
        this.context = Objects.requireNonNull(context);
        expressionVisitor = new ExpressionVisitor(
                context.getBindings(ScriptContext.ENGINE_SCOPE),
                processingEngine
        );
    }

    @Override
    public Object visitTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
        ResolvableExpression resolvableExpression = expressionVisitor.visit(ctx.expr());
        Bindings bindings = context.getBindings(ScriptContext.ENGINE_SCOPE);
        Object assignedObject = resolvableExpression.resolve(bindings);
        String variableIdentifier = ctx.varID().getText();
        bindings.put(variableIdentifier, assignedObject);
        return assignedObject;
    }
}
