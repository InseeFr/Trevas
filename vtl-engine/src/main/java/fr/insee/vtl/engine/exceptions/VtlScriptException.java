package fr.insee.vtl.engine.exceptions;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.script.ScriptException;

/**
 * The <code>VtlScriptException</code> is the base class for all VTL exceptions.
 */
public class VtlScriptException extends ScriptException {

    private final ParseTree tree;

    /**
     * Constructor taking the exception message and the parsing context.
     *
     * @param msg  The message for the exception.
     * @param tree The parsing context where the exception is thrown.
     */
    public VtlScriptException(String msg, ParseTree tree) {
        super(msg);
        this.tree = tree;
    }

    /**
     * Constructor taking the mother exception and the parsing context.
     *
     * @param mother The mother exception.
     * @param tree   The parsing context where the exception is thrown.
     */
    public VtlScriptException(Exception mother, ParseTree tree) {
        super(mother);
        this.tree = tree;
    }

    /**
     * Returns the position in a VTL expression corresponding to a parsing context.
     *
     * @param tree The parsing context whose corresponding position is looked up.
     * @return The position in the VTL expression, as an <code>Position</code> instance.
     */
    public static Position positionOf(ParseTree tree) {
        if (tree instanceof ParserRuleContext) {
            ParserRuleContext parserRuleContext = (ParserRuleContext) tree;
            return new Position(parserRuleContext.getStart(), parserRuleContext.getStop());
        }
        if (tree instanceof TerminalNode) {
            var terminalNode = ((TerminalNode) tree);
            return new Position(terminalNode.getSymbol());
        }
        throw new IllegalStateException();
    }

    /**
     * Returns the parsing context where the exception was raised.
     *
     * @return The parsing context where the exception was raised.
     */
    public ParseTree getTree() {
        return tree;
    }

    /**
     * Returns the position in a VTL expressions that caused the exception.
     *
     * @return The position in the VTL expression, as an <code>Position</code> instance.
     */
    public Position getPosition() {
        return positionOf(getTree());
    }

    /**
     * The <code>Position</code> class represents a section of code in a VTL expression.
     */
    public static class Position {

        private final Integer startLine;
        private final Integer endLine;
        private final Integer startColumn;
        private final Integer endColumn;


        public Position(Token from, Token to) {
            this.startLine = from.getLine() - 1;
            this.endLine = to.getLine() - 1;
            this.startColumn = from.getCharPositionInLine();
            this.endColumn = to.getCharPositionInLine() + (to.getStopIndex() - to.getStartIndex() + 1);
        }

        public Position(Token token) {
            this.startLine = token.getLine() - 1;
            this.endLine = token.getLine() - 1;
            this.startColumn = token.getCharPositionInLine();
            this.endColumn = token.getCharPositionInLine() + (token.getStopIndex() - token.getStartIndex() + 1);
        }

//        public Position(Integer startLine, Integer endLine, Integer startColumn, Integer endColumn) {
//            this.startLine = startLine - 1;
//            this.endLine = endLine - 1;
//            this.startColumn = startColumn;
//            this.endColumn = startLine + ( - token.getStartIndex() + 1);
//        }

        /**
         * Returns the number of the line where the <code>Position</code> starts.
         *
         * @return The starting line number (first line is 0).
         */
        public Integer getStartLine() {
            return startLine;
        }

        /**
         * Returns the number of the line where the <code>Position</code> ends.
         *
         * @return The ending line number (first line is 0).
         */
        public Integer getEndLine() {
            return endLine;
        }

        /**
         * Returns the number of the column where the <code>Position</code> starts.
         *
         * @return The starting column number (first column is 0).
         */
        public Integer getStartColumn() {
            return startColumn;
        }

        /**
         * Returns the number of the column where the <code>Position</code> ends.
         *
         * @return The ending column number (first column is 0).
         */
        public Integer getEndColumn() {
            return endColumn;
        }
    }
}
