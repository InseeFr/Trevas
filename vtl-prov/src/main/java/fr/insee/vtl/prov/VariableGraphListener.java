package fr.insee.vtl.prov;

import fr.insee.vtl.parser.VtlBaseListener;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ANTLR Listener that create a directed graph for a given VTL script.
 */
public class VariableGraphListener extends VtlBaseListener {

    private Variable current;
    private final Map<String, Variable> seen = new LinkedHashMap<>();
    private final DefaultDirectedGraph<Variable, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);

    private void setCurrent(ParserRuleContext id, ParserRuleContext expression) {
        assert current == null;
        current = new Variable(id, expression);
        if (seen.containsKey(current.getName())) {
            current = seen.get(current.getName()).newVersion();
        }
    }

    private void resetCurrent() {
        assert current != null;
        seen.put(current.getName(), current);
        current = null;
    }

    @Override
    public void enterTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
        setCurrent(ctx.varID(), ctx.expr());
    }

    @Override
    public void exitTemporaryAssignment(VtlParser.TemporaryAssignmentContext ctx) {
        resetCurrent();
    }

    @Override
    public void enterPersistAssignment(VtlParser.PersistAssignmentContext ctx) {
        setCurrent(ctx.varID(), ctx.expr());
    }

    @Override
    public void exitPersistAssignment(VtlParser.PersistAssignmentContext ctx) {
        resetCurrent();
    }

    @Override
    public void enterVarID(VtlParser.VarIDContext ctx) {
        assert current != null;

        Variable newVariable = new Variable(ctx);

        // Ignore if token is the same.
        if (newVariable.isSame(current)) {
            return;
        }

        // Get the current version if it exists
        if (seen.containsKey(newVariable.getName())) {
            newVariable = seen.get(newVariable.getName());
        } else {
            seen.put(newVariable.getName(), newVariable);
        }

        graph.addVertex(newVariable);
        graph.addVertex(current);
        graph.addEdge(newVariable, current);

    }

    public DefaultDirectedGraph<Variable, DefaultEdge> getGraph() {
        Set<DefaultEdge> selfLoops = graph.edgeSet().stream()
                .filter(e -> graph.getEdgeSource(e).equals(graph.getEdgeTarget(e)))
                .collect(Collectors.toSet());
        graph.removeAllEdges(selfLoops);
        return graph;
    }

    /**
     * Returns the last variables (sink vertex) of the graph.
     */
    public Set<Variable> getVariables() {
        return graph.vertexSet().stream()
                .filter(v -> graph.outgoingEdgesOf(v).isEmpty())
                .collect(Collectors.toSet());
    }

}
