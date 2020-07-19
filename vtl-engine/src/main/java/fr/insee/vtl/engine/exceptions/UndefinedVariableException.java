package fr.insee.vtl.engine.exceptions;

import org.antlr.v4.runtime.tree.ParseTree;

/**
 * The <code>UndefinedVariableException</code> indicates that a variable used in an expression has not been defined in the scope of this expression.
 */
public class UndefinedVariableException extends VtlScriptException {

    /**
     * Constructor taking the parsing context.
     *
     * @param tree The parsing context where the exception is thrown.
     */
    public UndefinedVariableException(ParseTree tree) {
        super(String.format("undefined variable %s", tree.getText()), tree);
    }
}
