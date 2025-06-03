package fr.insee.vtl.engine;

import static fr.insee.vtl.engine.VtlNativeMethods.NATIVE_METHODS;

import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.engine.exceptions.VtlSyntaxException;
import fr.insee.vtl.engine.visitors.AssignmentVisitor;
import fr.insee.vtl.model.FunctionProvider;
import fr.insee.vtl.model.Positioned;
import fr.insee.vtl.model.ProcessingEngine;
import fr.insee.vtl.model.ProcessingEngineFactory;
import fr.insee.vtl.model.VtlMethod;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import fr.insee.vtl.parser.VtlLexer;
import fr.insee.vtl.parser.VtlParser;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.script.AbstractScriptEngine;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * The {@link ScriptEngine} implementation for VTL.
 *
 * <p>To get an instance of the engine use the {@link ScriptEngineManager}:
 *
 * <pre><code>
 * ScriptEngineManager manager = new ScriptEngineManager();
 * ScriptEngine engine = manager.getEngineByName("vtl");
 * </code></pre>
 *
 * <p>VTL expressions can be evaluated using the methods: {@link #eval(Reader)}, {@link
 * #eval(Reader, ScriptContext)}, {@link #eval(String)} and {@link #eval(String, ScriptContext)}
 */
public class VtlScriptEngine extends AbstractScriptEngine {

  /** Script engine property giving the (comma-separated) list of engine names. */
  public static final String PROCESSING_ENGINE_NAMES = "$vtl.engine.processing_engine_names";

  private final ScriptEngineFactory factory;
  private Map<String, Method> methodCache;

  private Map<String, Method> globalMethodCache;

  /**
   * Constructor taking a script engine factory.
   *
   * @param factory The script engine factory associated to the script engine to create.
   */
  public VtlScriptEngine(ScriptEngineFactory factory) {
    this.factory = factory;
  }

  public static Positioned fromToken(Token token) {
    Positioned.Position position =
        new Positioned.Position(
            token.getLine() - 1,
            token.getLine() - 1,
            token.getCharPositionInLine(),
            token.getCharPositionInLine() + (token.getStopIndex() - token.getStartIndex() + 1));
    return () -> position;
  }

  public static Positioned fromContext(ParseTree tree) {
    if (tree instanceof ParserRuleContext parserRuleContext) {
      return fromTokens(parserRuleContext.getStart(), parserRuleContext.getStop());
    }
    if (tree instanceof TerminalNode treeNode) {
      return fromToken(treeNode.getSymbol());
    }
    throw new IllegalStateException();
  }

  public static Positioned fromTokens(Token from, Token to) {
    if (to == null) {
      to = from;
    }
    var position =
        new Positioned.Position(
            from.getLine() - 1,
            to.getLine() - 1,
            from.getCharPositionInLine(),
            to.getCharPositionInLine() + (to.getStopIndex() - to.getStartIndex() + 1));
    return () -> position;
  }

  static boolean matchParameters(Method method, Class<?>... classes) {
    Type[] genericParameterTypes = method.getGenericParameterTypes();
    Class<?>[] parameterTypes = method.getParameterTypes();

    if (classes.length != parameterTypes.length) {
      return false;
    }

    Map<TypeVariable<?>, Class<?>> typeArguments = new HashMap<>();

    for (int i = 0; i < parameterTypes.length; i++) {
      if (!isAssignableTo(classes[i], parameterTypes[i], genericParameterTypes[i], typeArguments)) {
        return false;
      }
    }

    return true;
  }

  static boolean isAssignableTo(
      Class<?> clazz,
      Class<?> target,
      Type genericTarget,
      Map<TypeVariable<?>, Class<?>> typeArguments) {
    if (target.isAssignableFrom(clazz)) {
      if (genericTarget instanceof TypeVariable<?> typeVariable) {
        Class<?> existingTypeArgument = typeArguments.get(typeVariable);
        if (existingTypeArgument == null) {
          typeArguments.put(typeVariable, clazz);
        } else return existingTypeArgument.equals(clazz);
      }
      return true;
    }

    if (genericTarget instanceof ParameterizedType parameterizedType) {
      Type[] typeArgumentsArray = parameterizedType.getActualTypeArguments();

      if (typeArgumentsArray.length != 1) {
        return false;
      }

      Type typeArgument = typeArgumentsArray[0];

      if (typeArgument instanceof TypeVariable<?> typeVariable) {
        Class<?> existingTypeArgument = typeArguments.get(typeVariable);
        if (existingTypeArgument == null) {
          typeArguments.put(typeVariable, clazz);
        } else return existingTypeArgument.equals(clazz);
        return true;
      } else if (typeArgument instanceof Class<?> classArgument) {
        return classArgument.isAssignableFrom(clazz);
      }
    }

    return false;
  }

  /**
   * Returns the name of the engine to use.
   *
   * @return The names of the engine to use.
   */
  private String getProcessingEngineName() {
    Object engineName = Optional.ofNullable(get(PROCESSING_ENGINE_NAMES)).orElse("memory");
    if (engineName instanceof String engineNameString) {
      return engineNameString;
    } else {
      throw new IllegalArgumentException(PROCESSING_ENGINE_NAMES + " must be a string");
    }
  }

  /**
   * Returns an instance of the processing engine for the script engine.
   *
   * @return an instance of the processing engine for the script engine.
   */
  public ProcessingEngine getProcessingEngine() {
    String name = getProcessingEngineName();
    Optional<ProcessingEngineFactory> factory =
        ServiceLoader.load(ProcessingEngineFactory.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(f -> f.getName().equals(name))
            .findFirst();
    return factory.orElseThrow().getProcessingEngine(this);
  }

  /**
   * Base method for the evaluation of a script expression in a given context.
   *
   * @param stream The script to evaluate represented as a stream of Unicode code points.
   * @param context The evaluation context (for example: data bindings).
   * @return The result of the evaluation of the script in the given context.
   * @throws VtlScriptException In case of error during the evaluation.
   */
  private Object evalStream(CodePointCharStream stream, ScriptContext context)
      throws VtlScriptException {
    try {
      VtlLexer lexer = new VtlLexer(stream);

      Deque<VtlScriptException> errors = new ArrayDeque<>();
      BaseErrorListener baseErrorListener =
          new BaseErrorListener() {
            @Override
            public void syntaxError(
                Recognizer<?, ?> recognizer,
                Object offendingSymbol,
                int startLine,
                int startColumn,
                String msg,
                RecognitionException e) {
              if (e != null && e.getCtx() != null) {
                errors.add(new VtlScriptException(msg, fromContext(e.getCtx())));
              } else {
                if (offendingSymbol instanceof Token offendingSymbolToken) {
                  errors.add(new VtlSyntaxException(msg, fromToken(offendingSymbolToken)));
                } else {
                  var pos =
                      new Positioned.Position(startLine, startLine, startColumn, startColumn + 1);
                  errors.add(new VtlScriptException(msg, () -> pos));
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

      AssignmentVisitor assignmentVisitor = new AssignmentVisitor(this, getProcessingEngine());
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
   * Returns a new instance of script context bindings.
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

  public VtlMethod findMethod(String name, Collection<Class> types) throws NoSuchMethodException {
    Set<Method> customMethods =
        methodCache == null ? Set.of() : new HashSet<>(methodCache.values());
    Set<Method> methods =
        Stream.concat(NATIVE_METHODS.stream(), customMethods.stream()).collect(Collectors.toSet());

    List<Method> candidates =
        methods.stream()
            .filter(method -> method.getName().equals(name))
            .filter(method -> matchParameters(method, types.toArray(Class[]::new)))
            .collect(Collectors.toList());
    if (candidates.size() == 1) {
      return new VtlMethod(candidates.get(0));
    }
    // TODO: Handle parameter resolution.
    for (Method method : methods) {
      if (method.getName().equals(name)
          && types.equals(Arrays.asList(method.getParameterTypes()))) {
        return new VtlMethod(method);
      }
    }
    throw new NoSuchMethodException(methodToString(name, types));
  }

  public VtlMethod findGlobalMethod(String name, Collection<Class> types)
      throws NoSuchMethodException {
    if (globalMethodCache == null) return null;
    Set<Method> methods = new HashSet<>(globalMethodCache.values());

    List<Method> candidates =
        methods.stream()
            .filter(method -> method.getName().equals(name))
            .filter(method -> matchParameters(method, types.toArray(Class[]::new)))
            .collect(Collectors.toList());

    if (candidates.size() == 0) {
      // It's not a global method
      return null;
    }

    if (candidates.size() == 1) {
      return new VtlMethod(candidates.get(0));
    }
    // TODO: Handle parameter resolution.
    for (Method method : methods) {
      if (method.getName().equals(name)
          && types.equals(Arrays.asList(method.getParameterTypes()))) {
        return new VtlMethod(method);
      }
    }
    throw new NoSuchMethodException(methodToString(name, types));
  }

  private String methodToString(String name, Collection<Class> argTypes) {
    StringJoiner sj = new StringJoiner(", ", name + "(", ")");
    if (argTypes != null) {
      for (Class<?> c : argTypes) {
        sj.add(c == null ? "null" : c.getSimpleName());
      }
    }
    return sj.toString();
  }

  public Method registerMethod(String name, Method method) {
    if (methodCache == null) {
      loadMethods();
    }
    return methodCache.put(name, method);
  }

  public Method registerGlobalMethod(String name, Method method) {
    if (globalMethodCache == null) {
      globalMethodCache = new LinkedHashMap<>();
    }
    return globalMethodCache.put(name, method);
  }

  private void loadMethods() {
    methodCache = new LinkedHashMap<>();
    ServiceLoader<FunctionProvider> providers = ServiceLoader.load(FunctionProvider.class);
    for (FunctionProvider provider : providers) {
      Map<String, Method> functions = provider.getFunctions(this);
      // TODO: rename function name with 'name' instead of java name
      for (String name : functions.keySet()) {
        methodCache.put(name, functions.get(name));
      }
    }
  }
}
