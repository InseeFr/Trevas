package fr.insee.vtl.engine;

import fr.insee.vtl.engine.visitors.AssignmentVisitor;
import fr.insee.vtl.engine.visitors.VtlRuntimeException;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;

import javax.script.*;
import java.io.IOException;
import java.io.Reader;

public class VtlScriptEngine extends AbstractScriptEngine {

    private final ScriptEngineFactory factory;

    public VtlScriptEngine(ScriptEngineFactory factory) {
        this.factory = factory;
    }

    private Object evalStream(CodePointCharStream stream, ScriptContext context) {
        VtlLexer lexer = new VtlLexer(stream);
        VtlParser parser = new VtlParser(new CommonTokenStream(lexer));

        AssignmentVisitor assignmentVisitor = new AssignmentVisitor(context);
        Object lastValue = null;
        for (VtlParser.StatementContext stmt : parser.start().statement()) {
            lastValue = assignmentVisitor.visit(stmt);
        }
        return lastValue;
    }

    @Override
    public Object eval(String script, ScriptContext context) {
        CodePointCharStream stream = CharStreams.fromString(script);
        return evalStream(stream, context);
    }

    @Override
    public Object eval(Reader reader, ScriptContext context) throws ScriptException {
        try {
            CodePointCharStream stream = CharStreams.fromReader(reader);
            return evalStream(stream, context);
        } catch (VtlRuntimeException vre) {
            throw vre.getCause();
        } catch (IOException e) {
            throw new ScriptException(e);
        }
    }

    @Override
    public Bindings createBindings() {
        return new SimpleBindings();
    }

    @Override
    public ScriptEngineFactory getFactory() {
        return factory;
    }
}
