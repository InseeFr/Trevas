package fr.insee.vtl.prov;

import fr.insee.vtl.parser.VtlBaseListener;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;

import java.util.LinkedHashMap;
import java.util.Map;

public class ProvenanceListener extends VtlBaseListener {

    public LinkedHashMap<String, Node> variables = new LinkedHashMap<>();
    public Node currentNode;

    public static class Node {
        public String name;
        public String expression;
        public Map<String, Node> parents = new LinkedHashMap<>();
    }

    public void addVariable(String name) {
        Node node = new Node();
        node.name = name;
        variables.put(name, node);
    }

    @Override
    public void enterTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
        currentNode = new Node();
        currentNode.expression = getText(ctx.expr());
        currentNode.name = ctx.varID().getText();
    }

    @Override
    public void exitTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
        variables.put(currentNode.name, currentNode);
        currentNode = null;
    }

    @Override
    public void enterPersistAssignment(VtlParser.PersistAssignmentContext ctx) {
        currentNode = new Node();
        currentNode.expression = getText(ctx.expr());
        currentNode.name = ctx.varID().getText();
    }

    private static String getText(ParserRuleContext ctx) {
        Token start = ctx.getStart();
        Token stop = ctx.getStop();
        return start.getInputStream().getText(new Interval(start.getStartIndex(), stop.getStopIndex()));
    }

    @Override
    public void exitPersistAssignment(VtlParser.PersistAssignmentContext ctx) {
        variables.put(currentNode.name, currentNode);
        currentNode = null;
    }


    @Override
    public void enterVarID(VtlParser.VarIDContext ctx) {
        String varID = ctx.getText();
        varID = varID.split("\\.")[0];
        if (currentNode != null) {

            if (variables.containsKey(varID)) {
                currentNode.parents.put(varID, variables.get(varID));
            } else {
                Node n = new Node();
                n.name = varID;
                variables.put(varID, n);
            }
        }
    }

}
