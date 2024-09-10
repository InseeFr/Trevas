package fr.insee.vtl.prov;

import fr.insee.vtl.parser.VtlBaseListener;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import fr.insee.vtl.prov.prov.Program;
import fr.insee.vtl.prov.prov.ProgramStep;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ANTLR Listener that create provenance objects.
 */
public class ProvenanceListener extends VtlBaseListener {

    private Program program = new Program();
    private Map<String, ProgramStep> programSteps = new HashMap<>();

    private String currentProgramStep;

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
        String id = getText(ctx.varID());
        String sourceCode = getText(ctx);
        currentProgramStep = id;
        if (!programSteps.containsKey(id)) {
            ProgramStep programStep = new ProgramStep(id, sourceCode);
            programSteps.put(id, programStep);
        }
        program.getProgramStepIds().add(id);
    }

    @Override
    public void exitTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
        currentProgramStep = null;
    }

    @Override
    public void enterPersistAssignment(VtlParser.PersistAssignmentContext ctx) {
        String id = getText(ctx.varID());
        String sourceCode = getText(ctx);
        currentProgramStep = id;
        if (!programSteps.containsKey(id)) {
            ProgramStep programStep = new ProgramStep(id, sourceCode);
            programSteps.put(id, programStep);
        }
        program.getProgramStepIds().add(id);
    }

    @Override
    public void exitPersistAssignment(VtlParser.PersistAssignmentContext ctx) {
        currentProgramStep = null;
    }

    @Override
    public void enterVarID(VtlParser.VarIDContext ctx) {

    }

    /**
     * Returns the provenance objects
     */
    public List<Object> getObjects() {
        List<Object> obj = new ArrayList<>();
        obj.add(program);
        obj.addAll(programSteps.values());
        return obj;
    }

    public static List<Object> parseAndListen(String expr, String id, String programName) {
        CodePointCharStream stream = CharStreams.fromString(expr);
        VtlLexer lexer = new VtlLexer(stream);
        VtlParser parser = new VtlParser(new CommonTokenStream(lexer));

        ProvenanceListener provenanceListener = new ProvenanceListener(id, programName);
        ParseTreeWalker.DEFAULT.walk(provenanceListener, parser.start());
        return provenanceListener.getObjects();
    }

}
