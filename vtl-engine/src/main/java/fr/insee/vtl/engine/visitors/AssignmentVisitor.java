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
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.util.*;
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
        // TODO: handle alias?
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
        AtomicInteger index = new AtomicInteger();
        List<HierarchicalRule> rules = ctx.ruleClauseHierarchical().ruleItemHierarchical()
                .stream()
                .map(r -> {
                    TerminalNode identifier = r.IDENTIFIER();
                    int i = index.getAndIncrement() + 1;
                    String ruleName = null != identifier ? identifier.getText() : rulesetName + "_" + i;

                    List<String> codeItems = new ArrayList<>();
                    VtlParser.CodeItemRelationContext codeItemRelationContext = r.codeItemRelation();
                    String valueDomainValue = codeItemRelationContext.valueDomainValue().IDENTIFIER().getText();
                    codeItems.add(valueDomainValue);

                    VtlParser.ComparisonOperandContext comparisonOperandContext = codeItemRelationContext.comparisonOperand();

                    StringBuilder codeItemExpressionBuilder = new StringBuilder();
                    codeItemRelationContext.codeItemRelationClause()
                            .forEach(circ -> {
                                TerminalNode minus = circ.MINUS();
                                String rightCodeItem = circ.rightCodeItem.getText();
                                codeItems.add(rightCodeItem);
                                if (minus != null)
                                    codeItemExpressionBuilder.append(" -" + rightCodeItem);
                                // plus value or plus null & minus null mean plus
                                codeItemExpressionBuilder.append(" +" + rightCodeItem);
                            });

                    // TODO: handle when clause
                    // TODO: optimize expr calculation? (without eval?)
                    String leftExpressionToEval = valueDomainValue;
                    String rightExpressionToEval = codeItemExpressionBuilder.toString();
                    String expressionToEval = "bool_var := " +
                            leftExpressionToEval + " " +
                            comparisonOperandContext.getText() + " " +
                            rightExpressionToEval + ";";

                    ResolvableExpression leftExpression = ResolvableExpression.withType(Double.class)
                            .withPosition(pos)
                            .using(context -> {
                                Bindings bindings = new SimpleBindings(context);
                                engine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
                                try {
                                    engine.eval("left := " + leftExpressionToEval + ";");
                                    Object resolvedLeft = engine.getContext().getAttribute("left");
                                    if (resolvedLeft.getClass().isAssignableFrom(Double.class)) {
                                        return (Double) resolvedLeft;
                                    }
                                    return ((Long) resolvedLeft).doubleValue();
                                } catch (ScriptException e) {
                                    throw new RuntimeException(e);
                                }
                            });

                    ResolvableExpression rightExpression = ResolvableExpression.withType(Double.class)
                            .withPosition(pos)
                            .using(context -> {
                                Bindings bindings = new SimpleBindings(context);
                                engine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
                                try {
                                    engine.eval("right := " + rightExpressionToEval + ";");
                                    Object resolvedRight = engine.getContext().getAttribute("right");
                                    if (resolvedRight.getClass().isAssignableFrom(Double.class)) {
                                        return (Double) resolvedRight;
                                    }
                                    return ((Long) resolvedRight).doubleValue();
                                } catch (ScriptException e) {
                                    throw new RuntimeException(e);
                                }
                            });

                    ResolvableExpression expression = ResolvableExpression.withType(Boolean.class)
                            .withPosition(pos)
                            .using(context -> {
                                Bindings bindings = new SimpleBindings(context);
                                engine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
                                try {
                                    engine.eval(expressionToEval);
                                    return (Boolean) engine.getContext().getAttribute("bool_var");
                                } catch (ScriptException e) {
                                    throw new RuntimeException(e);
                                }
                            });

                    ResolvableExpression errorCodeExpression = null != r.erCode() ? expressionVisitor.visit(r.erCode()) : null;
                    ResolvableExpression errorLevelExpression = null != r.erLevel() ? expressionVisitor.visit(r.erLevel()) : null;
                    return new HierarchicalRule(ruleName, valueDomainValue, expression, leftExpression, rightExpression, codeItems, errorCodeExpression, errorLevelExpression);
                }).collect(Collectors.toList());
        HierarchicalRuleset hr = new HierarchicalRuleset(rules, variable, erCodeType, erLevelType);
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        bindings.put(rulesetName, hr);
        return hr;
    }
}