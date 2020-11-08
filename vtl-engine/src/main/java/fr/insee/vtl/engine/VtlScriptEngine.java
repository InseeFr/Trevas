package fr.insee.vtl.engine;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.exceptions.VtlScriptException;
import fr.insee.vtl.engine.processors.InMemoryProcessingEngine;
import fr.insee.vtl.engine.visitors.AssignmentVisitor;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;

import javax.script.*;
import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * <code>VtlScriptEngine</code> provides base methods for the VTL script engine.
 */
public class VtlScriptEngine extends AbstractScriptEngine {

    private final ScriptEngineFactory factory;

    public List<ProcessingEngine> findProcessingEngines() {
        ServiceLoader<ProcessingEngine> loader = ServiceLoader.load(ProcessingEngine.class);
        return loader.stream().map(ServiceLoader.Provider::get).collect(Collectors.toList());
    }

    public ProcessingEngine getProcessingEngine() {
        return processingEngine;
    }

    public void setProcessingEngine(ProcessingEngine processingEngine) {
        this.processingEngine = processingEngine;
    }

    private ProcessingEngine processingEngine = new InMemoryProcessingEngine();

    /**
     * Constructor taking a script engine factory.
     *
     * @param factory The script engine factory associated to the script engine to create.
     */
    public VtlScriptEngine(ScriptEngineFactory factory) {
        this.factory = factory;
    }

    /**
     * Base method for the evaluation of a script expression in a given context.
     *
     * @param stream The script to evaluate represented as a stream of Unicode code points.
     * @param context The evaluation context (for example: data bindings).
     * @return The result of the evaluation of the script in the given context.
     * @throws VtlScriptException In case of error during the evaluation.
     */
    private Object evalStream(CodePointCharStream stream, ScriptContext context) throws VtlScriptException {
        try {
            VtlLexer lexer = new VtlLexer(stream);
            VtlParser parser = new VtlParser(new CommonTokenStream(lexer));

            AssignmentVisitor assignmentVisitor = new AssignmentVisitor(context, processingEngine);
            Object lastValue = null;
            for (VtlParser.StatementContext stmt : parser.start().statement()) {
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
     * @param script The script to evaluate represented as a string.
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
     * @param reader The <code>Reader</code> containing the script to evaluate.
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
