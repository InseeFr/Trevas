package fr.insee.vtl.prov;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.parser.VtlBaseListener;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import fr.insee.vtl.prov.prov.DataframeInstance;
import fr.insee.vtl.prov.prov.Program;
import fr.insee.vtl.prov.prov.ProgramStep;
import fr.insee.vtl.prov.prov.VariableInstance;
import fr.insee.vtl.prov.utils.ProvenanceUtils;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ANTLR Listener that create provenance objects.
 */
public class ProvenanceListener extends VtlBaseListener {

    private final Program program = new Program();

    private String currentProgramStep;

    private boolean isInDatasetClause;

    private String currentComponentID;
    private int stepIndex = 1;

    private boolean rootAssignment = true;

    // Map of label/UUID id
    private final Map<String, String> availableDataframeUUID = new HashMap<>();

    private Map<String, String> currentAvailableDataframeUUID = new HashMap<>();

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
            if (!isInDatasetClause) {
                Set<DataframeInstance> consumedDataframe = programStep.getConsumedDataframe();
                String dfId = ProvenanceUtils.getOrBuildUUID(availableDataframeUUID, label);
                DataframeInstance df = new DataframeInstance(dfId, label);
                consumedDataframe.add(df);
            }
            if (isInDatasetClause && null != currentComponentID) {
                Set<VariableInstance> usedVariables = programStep.getUsedVariables();
                VariableInstance v = new VariableInstance(label);
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
        VariableInstance v = new VariableInstance(label);
        assignedVariables.add(v);
    }

    @Override
    public void enterCalcClauseItem(VtlParser.CalcClauseItemContext ctx) {
        currentComponentID = getText(ctx.componentID());
    }

    @Override
    public void exitCalcClauseItem(VtlParser.CalcClauseItemContext ctx) {
        currentComponentID = null;
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

    public static Program run(String expr, String id, String programName) {
        CodePointCharStream stream = CharStreams.fromString(expr);
        VtlLexer lexer = new VtlLexer(stream);
        VtlParser parser = new VtlParser(new CommonTokenStream(lexer));

        ProvenanceListener provenanceListener = new ProvenanceListener(id, programName);
        ParseTreeWalker.DEFAULT.walk(provenanceListener, parser.start());
        return provenanceListener.getProgram();
    }

    public static Program runWithBindings(ScriptEngine engine, String expr, String id, String programName) {
        Program program = run(expr, id, programName);
        // 0 check if input dataset are empty?
        // 1 - Handle input dataset
        ProgramStep initialStep = program.getProgramStepByIndex(1);
        Set<DataframeInstance> consumedDataframe = initialStep.getConsumedDataframe();
        consumedDataframe.forEach(d ->{
            Dataset ds = (Dataset) engine.getContext().getAttribute(d.getLabel());
            ds.getDataStructure().values().forEach(c -> {
                VariableInstance variableInstance = new VariableInstance(c.getName());
                variableInstance.setRole(c.getRole());
                variableInstance.setType(c.getType());
                d.getHasVariableInstances().add(variableInstance);
            });
        });

        // 2 - Split script to loop over program steps and run them
        List<String> scripts = Arrays.stream(expr.split(";"))
                .map(e -> e + ";")
                .collect(Collectors.toList());

        scripts.forEach(s -> {
            try {
                engine.eval(s);
            } catch (ScriptException e) {
                throw new RuntimeException(e);
            }
        });

        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);

        return program;
    }

}
