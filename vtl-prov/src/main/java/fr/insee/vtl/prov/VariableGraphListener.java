package fr.insee.vtl.prov;

import fr.insee.vtl.parser.VtlBaseListener;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ANTLR Listener that create a directed graph for a given VTL script.
 */
public class VariableGraphListener extends VtlBaseListener {

    /**
     * Returns the text of a context with empty space (aka. all channels).
     */
    private static String getText(ParserRuleContext ctx) {
        Token start = ctx.getStart();
        Token stop = ctx.getStop();
        return start.getInputStream().getText(new Interval(start.getStartIndex(), stop.getStopIndex()));
    }

    private static class Variable {
        private final ParserRuleContext name;
        private final ParserRuleContext expression;
        private Integer version = 0;

        private Variable(ParserRuleContext name, ParserRuleContext expression) {
            this.name = name;
            this.expression = expression;
        }

        public Variable(ParserRuleContext name) {
            this.name = name;
            this.expression = null;
        }

        public String getName() {
            return getText(this.name);
        }

        public String getVersion() {
            return getName() + "_" + this.version;
        }

        public Optional<String> getExpression() {
            return Optional.ofNullable(expression)
                    .map(VariableGraphListener::getText);
        }

        public Variable newVersion() {
            Variable copy = new Variable(this.name, this.expression);
            copy.version = this.version + 1;
            return copy;
        }

        @Override
        public String toString() {
            return "Variable{" +
                    "name=" + getName() +
                    "(_" + version +  ")" +
                    ", expression=" + getExpression() +
                    '}';
        }

        @Override
        public final boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Variable)) return false;

            Variable variable = (Variable) o;
            return name.equals(variable.name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    private Variable current;
    private final Map<String, Variable> seen = new LinkedHashMap<>();
    private final DefaultDirectedGraph<String, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);

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
        if (newVariable.equals(current)) {
            return;
        }

        // Get the current version if it exists
        if (seen.containsKey(newVariable.getName())) {
            newVariable = seen.get(newVariable.getName());
        } else {
            seen.put(newVariable.getName(), newVariable);
        }

        graph.addVertex(newVariable.getVersion());
        graph.addVertex(current.getVersion());
        graph.addEdge(newVariable.getVersion(), current.getVersion());

    }

    public DefaultDirectedGraph<String, DefaultEdge> getGraph() {
        Set<DefaultEdge> selfLoops = graph.edgeSet().stream()
                .filter(e -> graph.getEdgeSource(e).equals(graph.getEdgeTarget(e)))
                .collect(Collectors.toSet());
        graph.removeAllEdges(selfLoops);
        return graph;
    }

    /**
     * Returns the last variables (sink vertex) of the graph.
     */
    public Set<String> getVariables() {
        return graph.vertexSet().stream()
                .filter(v -> graph.outgoingEdgesOf(v).isEmpty())
                .collect(Collectors.toSet());
    }

}
