package fr.insee.vtl.parser;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.antlr.v4.runtime.*;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

public class TaskForceTest {

  @TestFactory
  public Stream<DynamicTest> testVTLPositive() {
    return split(normalize(loadPositive()))
        .map(
            block -> {
              String[] lines = block.split("\n", 2);
              String name = lines[0];
              String code = lines[1];
              return parse(
                  name,
                  code,
                  false,
                  VtlLexer::new,
                  VtlParser::new,
                  parser -> ((VtlParser) parser).start());
            });
  }

  @TestFactory
  public Stream<DynamicTest> testVTLNegative() {
    return split(normalize(loadNegative()))
        .map(
            block -> {
              String[] lines = block.split("\n", 2);
              String name = lines[0];
              String code = lines[1];
              return parse(
                  name,
                  code,
                  true,
                  VtlLexer::new,
                  VtlParser::new,
                  parser -> ((VtlParser) parser).start());
            });
  }

  @TestFactory
  public Stream<DynamicTest> testVTLSimplePositive() {
    return split(normalize(loadPositive()))
        .map(
            block -> {
              String[] lines = block.split("\n", 2);
              String name = lines[0];
              String code = lines[1];
              return parse(
                  name,
                  code,
                  false,
                  VtlSimpleLexer::new,
                  VtlSimpleParser::new,
                  parser -> ((VtlSimpleParser) parser).start());
            });
  }

  @TestFactory
  public Stream<DynamicTest> testVTLSimpleNegative() {
    return split(normalize(loadNegative()))
        .map(
            block -> {
              String[] lines = block.split("\n", 2);
              String name = lines[0];
              String code = lines[1];
              return parse(
                  name,
                  code,
                  true,
                  VtlSimpleLexer::new,
                  VtlSimpleParser::new,
                  parser -> ((VtlSimpleParser) parser).start());
            });
  }

  String readResourceFile(String fileName) {
    try {
      URL resource = getClass().getResource(fileName);
      if (resource == null) {
        throw new IOException("Resource not found: " + fileName);
      }
      Path path = Paths.get(resource.toURI());
      return Files.readString(path);
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException("Failed to read resource file: " + fileName, e);
    }
  }

  String normalize(String input) {
    // Positive test use ## instead of // for some reason.
    return input.replaceAll("(?m)^##", "//");
  }

  Stream<String> split(String input) {
    // TODO: Fix last line
    return Pattern.compile("((?m)^//)").splitAsStream(input).filter(test -> !test.isEmpty());
  }

  DynamicTest parse(
      String name,
      String input,
      boolean shouldFail,
      Function<CharStream, Lexer> lexerFactory,
      Function<TokenStream, Parser> parserFactory,
      Consumer<Parser> parseRuleInvoker) {
    System.out.println("Test case: " + name + " \n" + input);

    return DynamicTest.dynamicTest(
        name,
        () -> {
          SyntaxErrorListener listener = new SyntaxErrorListener();
          var charStream = CharStreams.fromString(input);

          var lexer = lexerFactory.apply(charStream);
          lexer.removeErrorListeners();
          lexer.addErrorListener(listener);

          CommonTokenStream tokens = new CommonTokenStream(lexer);
          Parser parser = parserFactory.apply(tokens);
          parser.removeErrorListeners();
          parser.addErrorListener(listener);

          parseRuleInvoker.accept(parser);

          List<SyntaxError> errors = listener.getSyntaxErrors();
          System.out.println(input);
          System.out.println(errors);
          if (shouldFail) {
            assertThat(errors).isNotEmpty();
          } else {
            assertThat(errors).isEmpty();
          }
        });
  }

  String loadPositive() {
    return readResourceFile("/PositiveTests.vtl");
  }

  String loadNegative() {
    return readResourceFile("/NegativeTests.vtl");
  }
}
