package fr.insee.vtl.prov;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

/** ANTLR Listener that create provenance objects. */
public class ProvenanceListener extends VtlBaseListener {

  private final Program program = new Program();

  private String currentProgramStep;

  private boolean isInDatasetClause;

  private String currentDataframeID;

  private int stepIndex = 1;

  private boolean rootAssignment = true;

  private String aggrGroupingClause;

  private static final Map<String, String> idMappings = new HashMap<>();

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
    DataframeInstance df = new DataframeInstance(label);
    programStep.setProducedDataframe(df);
    program.getProgramSteps().add(programStep);
  }

  @Override
  public void exitTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
    currentProgramStep = null;
    rootAssignment = true;
  }

  @Override
  public void enterPersistAssignment(VtlParser.PersistAssignmentContext ctx) {
    String label = getText(ctx.varID());
    String sourceCode = getText(ctx);
    currentProgramStep = label;
    ProgramStep programStep = new ProgramStep(label, sourceCode, stepIndex);
    stepIndex++;
    DataframeInstance df = new DataframeInstance(label);
    programStep.setProducedDataframe(df);
    program.getProgramSteps().add(programStep);
  }

  @Override
  public void exitPersistAssignment(VtlParser.PersistAssignmentContext ctx) {
    currentProgramStep = null;
    rootAssignment = true;
  }

  @Override
  public void enterVarID(VtlParser.VarIDContext ctx) {
    String label = ctx.IDENTIFIER().getText();
    if (!rootAssignment) {
      ProgramStep programStep = program.getProgramStepByLabel(currentProgramStep);
      if (null == programStep || label.equals(currentProgramStep)) return;
      if (!isInDatasetClause) {
        Set<DataframeInstance> consumedDataframe = programStep.getConsumedDataframes();
        DataframeInstance df = new DataframeInstance(label);
        consumedDataframe.add(df);
        // Certainly don't need to reset? To check!
        currentDataframeID = label;
      }
      if (isInDatasetClause) {
        Set<VariableInstance> usedVariables = programStep.getUsedVariables();
        VariableInstance v = new VariableInstance(label);
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
  public void enterCalcClauseItem(VtlParser.CalcClauseItemContext ctx) {
    ProgramStep programStep = program.getProgramStepByLabel(currentProgramStep);
    Set<VariableInstance> assignedVariables = programStep.getAssignedVariables();
    VariableInstance assignedVariable =
        new VariableInstance(getText(ctx.componentID()), getText(ctx));
    assignedVariable.setParentDataframe(currentProgramStep);
    assignedVariables.add(assignedVariable);
  }

  @Override
  public void enterAggrClause(VtlParser.AggrClauseContext ctx) {
    aggrGroupingClause = getText(ctx.groupingClause());
  }

  @Override
  public void exitAggrClause(VtlParser.AggrClauseContext ctx) {
    aggrGroupingClause = null;
  }

  @Override
  public void enterAggrFunctionClause(VtlParser.AggrFunctionClauseContext ctx) {
    ProgramStep programStep = program.getProgramStepByLabel(currentProgramStep);
    Set<VariableInstance> assignedVariables = programStep.getAssignedVariables();
    String sourceCode =
        null != aggrGroupingClause ? getText(ctx) + " " + aggrGroupingClause : getText(ctx);
    VariableInstance assignedVariable =
        new VariableInstance(getText(ctx.componentID()), sourceCode);
    assignedVariable.setParentDataframe(currentProgramStep);
    assignedVariables.add(assignedVariable);
  }

  @Override
  public void enterValidateDPruleset(VtlParser.ValidateDPrulesetContext ctx) {
    ProgramStep programStep = program.getProgramStepByLabel(currentProgramStep);
    if (null != programStep) {
      programStep.getRulesets().add(ctx.IDENTIFIER().getText());
    }
  }

  @Override
  public void enterValidateHRruleset(VtlParser.ValidateHRrulesetContext ctx) {
    ProgramStep programStep = program.getProgramStepByLabel(currentProgramStep);
    if (null != programStep) {
      programStep.getRulesets().add(ctx.IDENTIFIER().getText());
    }
  }

  /** Returns the program object */
  public Program getProgram() {
    return program;
  }

  private static Program initProgram(String expr, String id, String programName) {
    CodePointCharStream stream = CharStreams.fromString(expr.trim());
    VtlLexer lexer = new VtlLexer(stream);
    VtlParser parser = new VtlParser(new CommonTokenStream(lexer));

    ProvenanceListener provenanceListener = new ProvenanceListener(id, programName);
    ParseTreeWalker.DEFAULT.walk(provenanceListener, parser.start());
    return provenanceListener.getProgram();
  }

  private static Program refineProgram(ScriptEngine engine, String expr, Program program) {

    Map<String, String> defineStatements = AntlrUtils.getDefineStatements(expr);

    try {
      engine.eval(expr);
    } catch (ScriptException e) {
      throw new RuntimeException(e);
    }

    program
        .getProgramSteps()
        .forEach(
            programStep -> {
              // Handle rulesets
              programStep
                  .getRulesets()
                  .forEach(
                      ruleset -> {
                        String defineScript = defineStatements.get(ruleset);
                        programStep.setSourceCode(
                            defineScript + "\n\n" + programStep.getSourceCode());
                      });
              // producedDataframe
              DataframeInstance producedDataframe = programStep.getProducedDataframe();
              String producedDataframeLabel = producedDataframe.getLabel();
              idMappings.put(producedDataframeLabel, producedDataframe.getId());

              // fill producedDataframe variables
              Dataset ds =
                  (Dataset)
                      engine
                          .getContext()
                          .getAttribute(programStep.getProducedDataframe().getLabel());
              ds.getDataStructure()
                  .values()
                  .forEach(
                      component -> {
                        String variableId = producedDataframeLabel + "|" + component.getName();
                        if (!idMappings.containsKey(variableId)) {
                          idMappings.put(variableId, ProvenanceUtils.generateUUID());
                        }
                        VariableInstance variableInstance =
                            new VariableInstance(component.getName());
                        variableInstance.setType(component.getType());
                        variableInstance.setRole(component.getRole());
                        variableInstance.setId(idMappings.get(variableId));
                        producedDataframe.getHasVariableInstances().add(variableInstance);
                      });

              // fill consumedDataframe and handle define scripts
              Set<DataframeInstance> consumedDataframes = programStep.getConsumedDataframes();

              consumedDataframes.forEach(
                  consumedDataframe -> {
                    String consumedDataframeLabel = consumedDataframe.getLabel();
                    if (!idMappings.containsKey(consumedDataframeLabel)) {
                      idMappings.put(consumedDataframeLabel, consumedDataframe.getId());
                    } else {
                      consumedDataframe.setId(idMappings.get(consumedDataframeLabel));
                    }

                    Object attribute = engine.getContext().getAttribute(consumedDataframeLabel);

                    if (attribute instanceof Dataset consumedDs) {
                      consumedDs
                          .getDataStructure()
                          .values()
                          .forEach(
                              component -> {
                                String variableId =
                                    consumedDataframeLabel + "|" + component.getName();
                                if (!idMappings.containsKey(variableId)) {
                                  idMappings.put(variableId, ProvenanceUtils.generateUUID());
                                }
                                VariableInstance variableInstance =
                                    new VariableInstance(component.getName());
                                variableInstance.setType(component.getType());
                                variableInstance.setRole(component.getRole());
                                variableInstance.setId(idMappings.get(variableId));
                                consumedDataframe.getHasVariableInstances().add(variableInstance);
                              });
                    }
                  });
              // usedVariables
              Set<VariableInstance> usedVariables = programStep.getUsedVariables();
              usedVariables.forEach(
                  usedVariable -> {
                    String usedVariableId =
                        usedVariable.getParentDataframe() + "|" + usedVariable.getLabel();
                    if (idMappings.containsKey(usedVariableId)) {
                      usedVariable.setId(idMappings.get(usedVariableId));
                    } else {
                      idMappings.put(usedVariableId, usedVariable.getId());
                    }
                  });
              // assignedVariables
              Set<VariableInstance> assignedVariables = programStep.getAssignedVariables();
              assignedVariables.forEach(
                  assignedVariable -> {
                    String assignedVariableId =
                        assignedVariable.getParentDataframe() + "|" + assignedVariable.getLabel();
                    if (idMappings.containsKey(assignedVariableId)) {
                      assignedVariable.setId(idMappings.get(assignedVariableId));
                    } else {
                      idMappings.put(assignedVariableId, assignedVariable.getId());
                    }
                  });
            });
    return program;
  }

  public static Program run(ScriptEngine engine, String expr, String id, String programName) {
    Program initialProgram = initProgram(expr, id, programName);
    Program refinedProgram = refineProgram(engine, expr, initialProgram);
    return refinedProgram;
  }
}
