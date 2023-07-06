package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.visitors.expression.ExpressionVisitor;
import fr.insee.vtl.model.*;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.script.Bindings;
import javax.script.ScriptContext;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

/**
 * <code>AssignmentVisitor</code> is the visitor for VTL assignment expressions.
 */
public class AssignmentVisitor extends VtlBaseVisitor<Object> {

    private final VtlScriptEngine engine;
    private final ProcessingEngine processingEngine;
    private final ExpressionVisitor expressionVisitor;

    /**
     * Constructor taking a scripting engine and a processing engine.
     *
     * @param engine           The scripting engine.
     * @param processingEngine The processing engine.
     */
    public AssignmentVisitor(VtlScriptEngine engine, ProcessingEngine processingEngine) {
        this.engine = Objects.requireNonNull(engine);
        this.processingEngine = Objects.requireNonNull(processingEngine);
        expressionVisitor = new ExpressionVisitor(
                engine.getBindings(ScriptContext.ENGINE_SCOPE),
                processingEngine,
                engine);
    }

    @Override
    public Object visitTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
        ResolvableExpression resolvableExpression = expressionVisitor.visit(ctx.expr());
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        Object assignedObject = resolvableExpression.resolve(bindings);
        String variableIdentifier = ctx.varID().getText();
        bindings.put(variableIdentifier, assignedObject);
        return assignedObject;
    }

    @Override
    public Object visitDefDatapointRuleset(VtlParser.DefDatapointRulesetContext ctx) {
        var pos = fromContext(ctx);
        String rulesetName = ctx.rulesetID().getText();
        List<VtlParser.SignatureContext> signature = ctx.rulesetSignature().signature();
        List<String> variables = signature.stream()
                .map(s -> s.varID().getText())
                .collect(Collectors.toList());
        Map<String, String> alias = signature.stream()
                .filter(s -> null != s.alias())
                .collect(Collectors.toMap(k -> k.varID().getText(), v -> v.alias().getText()));

        Set<Class> erCodeTypes = ctx.ruleClauseDatapoint().ruleItemDatapoint().stream().map(c -> {
            VtlParser.ErCodeContext erCodeContext = c.erCode();
            if (null == erCodeContext) return Object.class;
            return expressionVisitor.visit(c.erCode()).getType();
        }).collect(Collectors.toSet());
        List<Class> filteredErCodeTypes = erCodeTypes.stream().filter(t -> !t.equals(Object.class)).collect(Collectors.toList());
        if (filteredErCodeTypes.size() > 1) {
            throw new VtlRuntimeException(
                    new InvalidArgumentException("Error codes of rules have different types", pos)
            );
        }
        Class erCodeType = filteredErCodeTypes.isEmpty() ? String.class : filteredErCodeTypes.iterator().next();

        Set<Class> erLevelTypes = ctx.ruleClauseDatapoint().ruleItemDatapoint().stream().map(c -> {
            VtlParser.ErLevelContext erLevelContext = c.erLevel();
            if (null == erLevelContext) return Object.class;
            return expressionVisitor.visit(c.erLevel()).getType();
        }).collect(Collectors.toSet());
        List<Class> filteredErLevelTypes = erLevelTypes.stream().filter(t -> !t.equals(Object.class)).collect(Collectors.toList());
        if (filteredErLevelTypes.size() > 1) {
            throw new VtlRuntimeException(
                    new InvalidArgumentException("Error levels of rules have different types", pos)
            );
        }
        Class erLevelType = filteredErLevelTypes.isEmpty() ? Long.class : filteredErLevelTypes.iterator().next();

        AtomicInteger index = new AtomicInteger();
        List<DataPointRule> rules = ctx.ruleClauseDatapoint().ruleItemDatapoint()
                .stream()
                .map(c -> {
                    TerminalNode identifier = c.IDENTIFIER();
                    int i = index.getAndIncrement() + 1;
                    String name = null != identifier ? identifier.getText() : rulesetName + "_" + i;

                    VtlParser.ExprContext antecedentContiditonContext = c.antecedentContiditon;
                    VtlParser.ExprContext consequentConditionContext = c.consequentCondition;

                    ResolvableExpression errorCodeExpression = null != c.erCode() ? expressionVisitor.visit(c.erCode()) : null;
                    ResolvableExpression errorLevelExpression = null != c.erLevel() ? expressionVisitor.visit(c.erLevel()) : null;
                    return new DataPointRule(
                            name,
                            dataStructure -> {
                                if (antecedentContiditonContext != null) {
                                    Map<String, Object> componentMap = dataStructure.values().stream()
                                            .collect(Collectors.toMap(Dataset.Component::getName, component -> component));
                                    return new ExpressionVisitor(componentMap, processingEngine, engine).visit(antecedentContiditonContext);
                                } else {
                                    return ResolvableExpression.withType(Boolean.class)
                                            .withPosition(pos)
                                            .using(cc -> Boolean.TRUE);
                                }
                            },
                            dataStructure -> {
                                Map<String, Object> componentMap = dataStructure.values().stream()
                                        .collect(Collectors.toMap(Dataset.Component::getName, component -> component));
                                return new ExpressionVisitor(componentMap, processingEngine, engine).visit(consequentConditionContext);
                            },
                            errorCodeExpression,
                            errorLevelExpression
                    );
                })
                .collect(Collectors.toList());
        DataPointRuleset dataPointRuleset = new DataPointRuleset(
                rulesetName,
                rules,
                variables,
                alias,
                erCodeType,
                erLevelType);
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        bindings.put(rulesetName, dataPointRuleset);
        return dataPointRuleset;
    }

    @Override
    public Object visitDefHierarchical(VtlParser.DefHierarchicalContext ctx) {
        var pos = fromContext(ctx);
        String rulesetName = ctx.rulesetID().getText();
        // Only support variables, not valuedomain
        String variable = ctx.hierRuleSignature().IDENTIFIER().getText();

        Set<Class> erCodeTypes = ctx.ruleClauseHierarchical().ruleItemHierarchical().stream().map(c -> {
            VtlParser.ErCodeContext erCodeContext = c.erCode();
            if (null == erCodeContext) return Object.class;
            return expressionVisitor.visit(c.erCode()).getType();
        }).collect(Collectors.toSet());
        List<Class> filteredErCodeTypes = erCodeTypes.stream().filter(t -> !t.equals(Object.class)).collect(Collectors.toList());
        if (filteredErCodeTypes.size() > 1) {
            throw new VtlRuntimeException(
                    new InvalidArgumentException("Error codes of rules have different types", pos)
            );
        }
        Class erCodeType = filteredErCodeTypes.isEmpty() ? String.class : filteredErCodeTypes.iterator().next();

        Set<Class> erLevelTypes = ctx.ruleClauseHierarchical().ruleItemHierarchical().stream().map(c -> {
            VtlParser.ErLevelContext erLevelContext = c.erLevel();
            if (null == erLevelContext) return Object.class;
            return expressionVisitor.visit(c.erLevel()).getType();
        }).collect(Collectors.toSet());
        List<Class> filteredErLevelTypes = erLevelTypes.stream().filter(t -> !t.equals(Object.class)).collect(Collectors.toList());
        if (filteredErLevelTypes.size() > 1) {
            throw new VtlRuntimeException(
                    new InvalidArgumentException("Error levels of rules have different types", pos)
            );
        }
        Class erLevelType = filteredErLevelTypes.isEmpty() ? Long.class : filteredErLevelTypes.iterator().next();

        //TODO: handle rules
        //TODO: define model object to save hierarchical ruleset

        // Temp
        return null;
    }
}