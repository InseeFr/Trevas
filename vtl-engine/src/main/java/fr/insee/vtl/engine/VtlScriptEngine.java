package fr.insee.vtl.engine;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.exceptions.VtlScriptException;
import fr.insee.vtl.engine.exceptions.VtlSyntaxException;
import fr.insee.vtl.engine.visitors.AssignmentVisitor;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ProcessingEngineFactory;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.*;

import javax.script.*;
import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.stream.Collectors;

/**
 * <code>VtlScriptEngine</code> provides base methods for the VTL script engine.
 */
public class VtlScriptEngine extends AbstractScriptEngine {

    public static final String PROCESSING_ENGINE_NAMES = "$vtl.engine.processing_engine_names";

    private final ScriptEngineFactory factory;

    /**
     * Constructor taking a script engine factory.
     *
     * @param factory The script engine factory associated to the script engine to create.
     */
    public VtlScriptEngine(ScriptEngineFactory factory) {
        this.factory = factory;
    }

    private List<String> getProcessingEngineNames() {
        Object o = Optional.ofNullable(get(PROCESSING_ENGINE_NAMES))
                .orElse("memory");
        if (o instanceof String) {
            return Arrays.asList(((String) o).split(","));
        } else {
            throw new IllegalArgumentException(PROCESSING_ENGINE_NAMES +
                    " must be a comma separated list of names");
        }
    }

    public ProcessingEngine getProcessingEngine() {
        List<String> names = getProcessingEngineNames();
        List<ProcessingEngineFactory> factories = ServiceLoader.load(ProcessingEngineFactory.class)
                .stream()
                .map(ServiceLoader.Provider::get)
                .filter(f -> names.contains(f.getName()))
                .collect(Collectors.toList());
        // TODO: Handle multiple factories.
        return factories.get(0).getProcessingEngine(this);
    }

    /**
     * Base method for the evaluation of a script expression in a given context.
     *
     * @param stream  The script to evaluate represented as a stream of Unicode code points.
     * @param context The evaluation context (for example: data bindings).
     * @return The result of the evaluation of the script in the given context.
     * @throws VtlScriptException In case of error during the evaluation.
     */
    private Object evalStream(CodePointCharStream stream, ScriptContext context) throws VtlScriptException {
        try {
            VtlLexer lexer = new VtlLexer(stream);

            Deque<VtlScriptException> errors = new ArrayDeque<>();
            BaseErrorListener baseErrorListener = new BaseErrorListener() {
                @Override
                public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int startLine, int startColumn, String msg, RecognitionException e) {
                    if (e != null && e.getCtx() != null) {
                        errors.add(new VtlScriptException(msg, e.getCtx()));
                    } else {
                        if (offendingSymbol instanceof Token) {
                            errors.add(new VtlSyntaxException(msg, (Token) offendingSymbol));
                        } else {
                            throw new Error("offendingSymbol was not a Token");
                        }
                    }
                }

            };

            lexer.removeErrorListeners();
            lexer.addErrorListener(baseErrorListener);

            VtlParser parser = new VtlParser(new CommonTokenStream(lexer));
            parser.removeErrorListeners();
            parser.addErrorListener(baseErrorListener);

            // Note that we need to call this method to trigger the
            // error listener.
            var start = parser.start();

            if (!errors.isEmpty()) {
                var first = errors.removeFirst();
                for (VtlScriptException suppressed : errors) {
                    first.addSuppressed(suppressed);
                }
                throw first;
            }

            AssignmentVisitor assignmentVisitor = new AssignmentVisitor(context, getProcessingEngine());
            Object lastValue = null;
            for (VtlParser.StatementContext stmt : start.statement()) {
                lastValue = assignmentVisitor.visit(stmt);
            }
            return lastValue;
        } catch (VtlRuntimeException vre) {
            throw vre.getCause();
        }
    }

    /**
     * Evaluation of a script expression (represented as a string) in a given context.
     *
     * @param script  The script to evaluate represented as a string.
     * @param context The evaluation context (for example: data bindings).
     * @return The result of the evaluation of the script in the given context.
     * @throws VtlScriptException In case of error during the evaluation.
     */
    @Override
    public Object eval(String script, ScriptContext context) throws VtlScriptException {
        CodePointCharStream stream = CharStreams.fromString(script);
        return evalStream(stream, context);
    }

    /**
     * Evaluation of a script expression (read in a <code>Reader</code>) in a given context.
     *
     * @param reader  The <code>Reader</code> containing the script to evaluate.
     * @param context The evaluation context (for example: data bindings).
     * @return The result of the evaluation of the script in the given context.
     * @throws ScriptException In case of error during the evaluation.
     */
    @Override
    public Object eval(Reader reader, ScriptContext context) throws ScriptException {
        try {
            CodePointCharStream stream = CharStreams.fromReader(reader);
            return evalStream(stream, context);
        } catch (IOException e) {
            throw new ScriptException(e);
        }
    }

    /**
     * Returns an new instance of script context bindings.
     *
     * @return A new instance of bindings (<code>SimpleBindings</code> object).
     */
    @Override
    public Bindings createBindings() {
        return new SimpleBindings();
    }

    /**
     * Returns the script engine factory associated to the script engine.
     *
     * @return The script engine factory associated to the script engine.
     */
    @Override
    public ScriptEngineFactory getFactory() {
        return factory;
    }
}
