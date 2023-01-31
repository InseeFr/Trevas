package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ValidationFunctionsTest {

    private final InMemoryDataset dataset = new InMemoryDataset(
            List.of(
                    List.of("2011", "I", "CREDIT", 10L),
                    List.of("2011", "I", "DEBIT", -2L),
                    List.of("2012", "I", "CREDIT", 10L),
                    List.of("2012", "I", "DEBIT", 2L)
            ),
            List.of(
                    new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_3", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE)
            )
    );
    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testValidateExceptions() throws ScriptException {
        ScriptContext context = engine.getContext();
        context.setAttribute("DS_1", dataset, ScriptContext.ENGINE_SCOPE);

        assertThatThrownBy(() -> engine.eval("define datapoint ruleset dpr1 (variable unvalid_var) is " +
                "when Id_3 = \"CREDIT\" then Me_1 >= 0 errorcode \"Bad credit\"; " +
                "when Id_3 = \"DEBIT\" then Me_1 >= 0 errorcode \"Bad debit\" " +
                "end datapoint ruleset; " +
                "DS_r := check_datapoint(DS_1, dpr1);"))
                .hasMessageContaining("Variable unvalid_var not contained in DS_1");
        assertThatThrownBy(() -> engine.eval("DS_r := check_datapoint(DS_1, dpr1111);"))
                .hasMessageContaining("undefined variable dpr1111");
    }
}
