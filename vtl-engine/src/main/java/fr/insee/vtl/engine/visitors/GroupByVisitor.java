package fr.insee.vtl.engine.visitors;

import static fr.insee.vtl.engine.VtlScriptEngine.fromContext;

import fr.insee.vtl.engine.exceptions.InvalidArgumentException;
import fr.insee.vtl.engine.exceptions.VtlRuntimeException;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.parser.VtlBaseVisitor;
import fr.insee.vtl.parser.VtlParser;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Produce a normalized group by. Group except are inverted. */
public class GroupByVisitor extends VtlBaseVisitor<List<String>> {

  private final Structured.DataStructure dataStructure;

  public GroupByVisitor(Structured.DataStructure dataStructure) {
    this.dataStructure = dataStructure;
  }

  private String getName(VtlParser.ComponentIDContext ctx) {
    String text = ctx.getText();
    if (text.startsWith("'") && text.endsWith("'")) {
      text = text.substring(1, text.length() - 1);
    }
    return text;
  }

  @Override
  protected List<String> defaultResult() {
    return List.of();
  }

  @Override
  public List<String> visitGroupByOrExcept(VtlParser.GroupByOrExceptContext ctx) {
    List<String> componentNames = new ArrayList<>(ctx.componentID().size());
    for (var component : ctx.componentID()) {
      var componentName = getName(component);
      if (!dataStructure.containsKey(componentName)) {
        throw new VtlRuntimeException(
            new InvalidArgumentException(
                "unknown component %s".formatted(componentName), fromContext(component)));
      }
      componentNames.add(componentName);
    }
    if (ctx.BY() != null) {
      return componentNames;
    } else if (ctx.EXCEPT() != null) {
      // Except is kind of random since the order of identifiers is not really known...
      // But it's specified.
      return dataStructure.keySet().stream()
          .filter(componentNames::contains)
          .collect(Collectors.toList());
    } else {
      throw new UnsupportedOperationException();
    }
  }
}
