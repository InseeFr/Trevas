package fr.insee.vtl.prov;

import fr.insee.vtl.model.DataPointRuleset;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.parser.VtlBaseListener;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import fr.insee.vtl.prov.prov.DataframeInstance;
import fr.insee.vtl.prov.prov.Program;
import fr.insee.vtl.prov.prov.ProgramStep;
import fr.insee.vtl.prov.prov.VariableInstance;
import fr.insee.vtl.prov.utils.AntlrUtils;
import fr.insee.vtl.prov.utils.ProvenanceUtils;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ANTLR Listener that create provenance objects.
 */
public class ProvenanceListener extends VtlBaseListener {

    private final Program program = new Program();

    private String currentProgramStep;

    private boolean isInDatasetClause;

    private String currentComponentID;

    private String currentDataframeID;

    private int stepIndex = 1;

    private boolean rootAssignment = true;

    // Map of label/UUID id
    private final Map<String, String> availableDataframeUUID = new HashMap<>();

    private Map<String, String> currentAvailableDataframeUUID = new HashMap<>();

    // Map of label/UUID id
    private final Map<String, String> availableVariableUUID = new HashMap<>();

    private Map<String, String> currentAvailableVariableUUID = new HashMap<>();

    public ProvenanceListener(String id, String programName) {
        program.setId(id);
        program.setLabel(programName);
    }

    private String getText(ParserRuleContext ctx) {
        Token start = ctx.getStart();
        Token stop = ctx.getStop();
        return start.getInputStream().getText(new Interval(start.getStartIndex(), stop.getStopIndex()));
    }

    @Override
    public void enterStart(VtlParser.StartContext ctx) {
        program.setSourceCode(getText(ctx));
    }

    @Override
    public void enterTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
        String label = getText(ctx.varID());
        String sourceCode = getText(ctx);
        currentProgramStep = label;
        ProgramStep programStep = new ProgramStep(label, sourceCode, stepIndex);
        stepIndex++;
        String dfId = UUID.randomUUID().toString();
        currentAvailableDataframeUUID.put(label, dfId);
        DataframeInstance df = new DataframeInstance(dfId, label);
        programStep.setProducedDataframe(df);
        program.getProgramSteps().add(programStep);
    }

    @Override
    public void exitTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
        currentProgramStep = null;
        availableDataframeUUID.putAll(currentAvailableDataframeUUID);
        currentAvailableDataframeUUID = new HashMap<>();
        rootAssignment = true;
    }

    @Override
    public void enterPersistAssignment(VtlParser.PersistAssignmentContext ctx) {
        String label = getText(ctx.varID());
        String sourceCode = getText(ctx);
        currentProgramStep = label;
        ProgramStep programStep = new ProgramStep(label, sourceCode, stepIndex);
        stepIndex++;
        String dfId = UUID.randomUUID().toString();
        currentAvailableDataframeUUID.put(label, dfId);
        DataframeInstance df = new DataframeInstance(dfId, label);
        programStep.setProducedDataframe(df);
        program.getProgramSteps().add(programStep);
    }

    @Override
    public void exitPersistAssignment(VtlParser.PersistAssignmentContext ctx) {
        currentProgramStep = null;
        availableDataframeUUID.putAll(currentAvailableDataframeUUID);
        currentAvailableDataframeUUID = new HashMap<>();
        rootAssignment = true;
    }

    @Override
    public void enterVarID(VtlParser.VarIDContext ctx) {
        String label = ctx.IDENTIFIER().getText();
        if (!rootAssignment) {
            ProgramStep programStep = program.getProgramStepByLabel(currentProgramStep);
            if (null == programStep) return;
            if (!isInDatasetClause) {
                Set<DataframeInstance> consumedDataframe = programStep.getConsumedDataframe();
                String dfId = ProvenanceUtils.getOrBuildUUID(availableDataframeUUID, label);
                DataframeInstance df = new DataframeInstance(dfId, label);
                consumedDataframe.add(df);
                // Certainly don't need to reset? To check!
                currentDataframeID = label;
            }
            if (isInDatasetClause && null != currentComponentID) {
                Set<VariableInstance> usedVariables = programStep.getUsedVariables();
                String varUUID =
                        ProvenanceUtils.getOrBuildUUID(availableVariableUUID, currentDataframeID + "|" + label);
                VariableInstance v = new VariableInstance(varUUID, label);
                v.setParentDataframe(currentDataframeID);
                usedVariables.add(v);
            }
        } else {
            rootAssignment = false;
        }
    }

    @Override
    public void enterDatasetClause(VtlParser.DatasetClauseContext ctx) {
        isInDatasetClause = true;
    }

    @Override
    public void exitDatasetClause(VtlParser.DatasetClauseContext ctx) {
        isInDatasetClause = false;
    }

    @Override
    public void enterComponentID(VtlParser.ComponentIDContext ctx) {
        String label = ctx.getText();
        ProgramStep programStep = program.getProgramStepByLabel(currentProgramStep);
        Set<VariableInstance> assignedVariables = programStep.getAssignedVariables();
        String variableUUID = ProvenanceUtils.getOrBuildUUID(availableDataframeUUID, label);
        VariableInstance v = new VariableInstance(variableUUID, label);
        assignedVariables.add(v);
        currentAvailableVariableUUID.put(currentDataframeID + "|" + label, variableUUID);
    }

    @Override
    public void exitComponentID(VtlParser.ComponentIDContext ctx) {
    }

    @Override
    public void enterCalcClauseItem(VtlParser.CalcClauseItemContext ctx) {
        currentComponentID = getText(ctx.componentID());
    }

    @Override
    public void exitCalcClauseItem(VtlParser.CalcClauseItemContext ctx) {
        currentComponentID = null;
        availableVariableUUID.putAll(currentAvailableVariableUUID);
        currentAvailableVariableUUID = new HashMap<>();
    }

    @Override
    public void enterAggrFunctionClause(VtlParser.AggrFunctionClauseContext ctx) {
        currentComponentID = getText(ctx.componentID());
    }

    @Override
    public void exitAggrFunctionClause(VtlParser.AggrFunctionClauseContext ctx) {
        currentComponentID = null;
    }

    /**
     * Returns the program object
     */
    public Program getProgram() {
        return program;
    }

    private static Program prepareProgram(String expr, String id, String programName) {
        CodePointCharStream stream = CharStreams.fromString(expr.trim());
        VtlLexer lexer = new VtlLexer(stream);
        VtlParser parser = new VtlParser(new CommonTokenStream(lexer));

        ProvenanceListener provenanceListener = new ProvenanceListener(id, programName);
        ParseTreeWalker.DEFAULT.walk(provenanceListener, parser.start());
        return provenanceListener.getProgram();
    }

    public static Program run(ScriptEngine engine, String expr, String id, String programName) {
        Program program = prepareProgram(expr, id, programName);
        // 0 check if input dataset are empty?
        // Keep already handled dataset
        List<String> dsHandled = new ArrayList<>();
        // Split script to loop over program steps and run them
        AtomicInteger index = new AtomicInteger(1);
        Map<String, String> defineStatements = AntlrUtils.getDefineStatements(expr.trim());
        // Add all defines to engine context
        try {
            engine.eval(String.join(" ", defineStatements.values()));
        } catch (ScriptException e) {
            throw new RuntimeException(e);
        }
        List<String> assignmentStatements = AntlrUtils.getAssignmentStatements(expr.trim());
        assignmentStatements
                .forEach(
                        stepScript -> {
                            AtomicReference<String> stepScriptWithDefines = new AtomicReference<>(stepScript);
                            int i = index.getAndIncrement();
                            // 1 - Handle input dataset
                            ProgramStep step = program.getProgramStepByIndex(i);
                            Set<DataframeInstance> consumedDataframe = step.getConsumedDataframe();
                            consumedDataframe.forEach(
                                    d -> {
                                        if (!dsHandled.contains(d.getLabel())) {
                                            Object attribute = engine.getContext().getAttribute(d.getLabel());
                                            if (attribute instanceof Dataset ds) {
                                                ds.getDataStructure()
                                                        .values()
                                                        .forEach(
                                                                c -> {
                                                                    VariableInstance variableInstance =
                                                                            step.getUsedVariables().stream()
                                                                                    .filter(
                                                                                            v ->
                                                                                                    v.getParentDataframe().equals(d.getLabel())
                                                                                                            && v.getLabel().equals(c.getName()))
                                                                                    .findFirst()
                                                                                    .orElse(new VariableInstance(c.getName()));
                                                                    variableInstance.setRole(c.getRole());
                                                                    variableInstance.setType(c.getType());
                                                                    d.getHasVariableInstances().add(variableInstance);
                                                                });
                                            }
                                            if (attribute instanceof DataPointRuleset) {
                                                // Remove dpr from consumedDataframe
                                                consumedDataframe.removeIf(df -> d.getLabel().equals(df.getLabel()));
                                                // Add dpr to usedDefines
                                                List<String> usedDefines = step.getUsedDefines();
                                                String defineScript = defineStatements.get(d.getLabel());
                                                stepScriptWithDefines.set(defineScript + "\n" + stepScriptWithDefines);
                                                usedDefines.add(defineStatements.get(d.getLabel()));
                                                step.setUsedDefines(usedDefines);
                                            }
                                        }
                                    });
                            try {
                                engine.eval(stepScriptWithDefines.get());
                            } catch (ScriptException e) {
                                throw new RuntimeException(e);
                            }
                            // Improve built variables attributes
                            DataframeInstance producedDataframe = step.getProducedDataframe();
                            Dataset ds = (Dataset) engine.getContext().getAttribute(producedDataframe.getLabel());

                            ds.getDataStructure()
                                    .values()
                                    .forEach(
                                            c -> {
                                                VariableInstance variableInstance =
                                                        step.getAssignedVariables().stream()
                                                                .filter(v -> v.getLabel().equals(c.getName()))
                                                                .findFirst()
                                                                // TODO: refine variable detection in usedVariable
                                                                .orElse(new VariableInstance(c.getName()));
                                                variableInstance.setRole(c.getRole());
                                                variableInstance.setType(c.getType());
                                                producedDataframe.getHasVariableInstances().add(variableInstance);
                                            });
                            dsHandled.add((producedDataframe.getLabel()));
                            // Correct usedVariables ID checking ds/var of last assignment
                        });

        return program;
    }
}
