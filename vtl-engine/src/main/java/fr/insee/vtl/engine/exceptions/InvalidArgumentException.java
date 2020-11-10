package fr.insee.vtl.engine.exceptions;

import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * The <code>InvalidArgumentException</code> indicates that an argument used in an expression is invalid.
 */
public class InvalidArgumentException extends VtlScriptException {

    /**
     * Constructor taking the parsing context and the message.
     *
     * @param message The exception message.
     * @param tree The parsing context where the exception is thrown.
     */
    public InvalidArgumentException(String message, ParseTree tree) {
        super(message, tree);
    }

}
