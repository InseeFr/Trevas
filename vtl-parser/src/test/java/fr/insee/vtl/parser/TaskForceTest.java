package fr.insee.vtl.parser;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

public class TaskForceTest {

  @TestFactory
  public Stream<DynamicTest> testPositive() {
    return split(normalize(loadPositive()))
        .map(
            block -> {
              String[] lines = block.split("\n", 2);
              String name = lines[0];
              String code = lines[1];
              return parse(name, code);
            });
  }

  @TestFactory
  public Stream<DynamicTest> testNegative() {
    return split(normalize(loadNegative()))
        .map(
            block -> {
              String[] lines = block.split("\n", 2);
              String name = lines[0];
              String code = lines[1];
              return parse(name, code);
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

  DynamicTest parse(String name, String input) {
    System.out.println("Test case: " + name + " \n" + input);

    return DynamicTest.dynamicTest(
        name,
        () -> {
          SyntaxErrorListener listener = new SyntaxErrorListener();
          var charStream = CharStreams.fromString(input);
          var lexer = new VtlSimpleLexer(charStream);
          var tokens = new CommonTokenStream(lexer);
          var parser = new VtlSimpleParser(tokens);

          parser.removeErrorListeners();

          lexer.addErrorListener(listener);
          parser.addErrorListener(listener);

          VtlSimpleParser.StartContext start = parser.start();
          List<SyntaxError> errors = listener.getSyntaxErrors();
          System.out.println(start);
          System.out.println(errors);
        });
  }

  String loadPositive() {
    return readResourceFile("/PositiveTests.vtl");
  }

  String loadNegative() {
    return readResourceFile("/NegativeTests.vtl");
  }
}
