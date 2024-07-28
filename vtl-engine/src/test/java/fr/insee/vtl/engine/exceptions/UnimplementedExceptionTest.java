package fr.insee.vtl.engine.exceptions;

import fr.insee.vtl.model.utils.Java8Helpers;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.SimpleBindings;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class UnimplementedExceptionTest {

    @Test
    public void testSimple() {
        String vtlExpression = "a := first_value(ds over());";
        Dataset ds = new InMemoryDataset(
                Java8Helpers.listOf(Java8Helpers.listOf(1L, 31L)),
                Java8Helpers.listOf(new Structured.Component("ID", Long.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("FOO", Long.class, Dataset.Role.MEASURE))
        );
        SimpleBindings bindings = new SimpleBindings();
        bindings.put("ds", ds);

        ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");
        engine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);

        assertThatThrownBy(() -> {
            engine.eval(vtlExpression);
        }).isInstanceOf(UnsupportedOperationException.class);
    }
}
