package fr.insee.vtl.engine.visitors.expression.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import java.util.Arrays;
import java.util.List;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end script tests for {@code DS # component} (visitor → {@link
 * fr.insee.vtl.engine.semantics.membership.MembershipExecutor}).
 */
class MembershipFunctionsTest {

  private ScriptEngine engine;

  @BeforeEach
  void setUp() {
    engine = new ScriptEngineManager().getEngineByName("vtl");
  }

  @Test
  void measureMembershipViaScript() throws ScriptException {
    var ds =
        new InMemoryDataset(
            List.of(
                new Structured.Component("Id_1", Long.class, Dataset.Role.IDENTIFIER),
                new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE)),
            Arrays.asList(1L, 10L));
    engine.getContext().setAttribute("ds", ds, ScriptContext.ENGINE_SCOPE);

    engine.eval("res := ds#Me_1;");

    var res = (Dataset) engine.getContext().getAttribute("res");
    assertThat(res.getColumnNames()).containsExactly("Id_1", "Me_1");
    assertThat(res.getDataAsMap().get(0)).containsEntry("Me_1", 10L);
  }

  @Test
  void unknownComponentFails() {
    var ds =
        new InMemoryDataset(
            List.of(new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE)),
            List.of(1L));
    engine.getContext().setAttribute("ds", ds, ScriptContext.ENGINE_SCOPE);

    assertThatThrownBy(() -> engine.eval("res := ds#missing;"))
        .hasMessageContaining("column missing not found");
  }
}
