package fr.insee.vtl.engine.visitors.expression.functions;

import fr.insee.vtl.model.utils.Java8Helpers;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ValidationFunctionsTest {

    private final Dataset dataset = new InMemoryDataset(
            Java8Helpers.listOf(
                    Java8Helpers.listOf("2011", "I", "CREDIT", 10L),
                    Java8Helpers.listOf("2011", "I", "DEBIT", -2L),
                    Java8Helpers.listOf("2012", "I", "CREDIT", 10L),
                    Java8Helpers.listOf("2012", "I", "DEBIT", 2L)
            ),
            Java8Helpers.listOf(
                    new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_3", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Me_1", Long.class, Dataset.Role.MEASURE)
            )
    );

    private final Dataset dsExprOk = new InMemoryDataset(
            Java8Helpers.listOf(
                    Java8Helpers.listOf("2011", "I", "CREDIT", true),
                    Java8Helpers.listOf("2011", "I", "DEBIT", false),
                    Java8Helpers.listOf("2012", "I", "CREDIT", false),
                    Java8Helpers.listOf("2012", "I", "DEBIT", true)
            ),
            Java8Helpers.listOf(
                    new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_3", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("bool_var", Boolean.class, Dataset.Role.MEASURE)
            )
    );
    private final Dataset dsExprKo1 = new InMemoryDataset(
            Java8Helpers.listOf(
                    Java8Helpers.listOf("2011", "I", "CREDIT", true, true),
                    Java8Helpers.listOf("2011", "I", "DEBIT", false, true),
                    Java8Helpers.listOf("2012", "I", "CREDIT", false, true),
                    Java8Helpers.listOf("2012", "I", "DEBIT", true, true)
            ),
            Java8Helpers.listOf(
                    new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_3", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("toto", Boolean.class, Dataset.Role.MEASURE),
                    new Structured.Component("toto2", Boolean.class, Dataset.Role.MEASURE)
            )
    );
    private final Dataset dsExprKo2 = new InMemoryDataset(
            Java8Helpers.listOf(
                    Java8Helpers.listOf("2011", "I", "CREDIT", 1L),
                    Java8Helpers.listOf("2011", "I", "DEBIT", 1L),
                    Java8Helpers.listOf("2012", "I", "CREDIT", 1L),
                    Java8Helpers.listOf("2012", "I", "DEBIT", 1L)
            ),
            Java8Helpers.listOf(
                    new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_3", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("toto", Long.class, Dataset.Role.MEASURE)
            )
    );
    private final Dataset dsImbalanceKo = new InMemoryDataset(
            Java8Helpers.listOf(
                    Java8Helpers.listOf("2011", "I", "CREDIT", 1L, 1L),
                    Java8Helpers.listOf("2011", "I", "DEBIT", 2L, 1L),
                    Java8Helpers.listOf("2012", "I", "CREDIT", 2L, 1L),
                    Java8Helpers.listOf("2012", "I", "DEBIT", 3L, 1L)
            ),
            Java8Helpers.listOf(
                    new Structured.Component("Id_1", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_2", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("Id_3", String.class, Dataset.Role.IDENTIFIER),
                    new Structured.Component("toto", Long.class, Dataset.Role.MEASURE),
                    new Structured.Component("toto2", Long.class, Dataset.Role.MEASURE)
            )
    );
    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testValidateExceptions() {
        ScriptContext context = engine.getContext();
        context.setAttribute("DS_1", dataset, ScriptContext.ENGINE_SCOPE);

        assertThatThrownBy(() -> engine.eval("define datapoint ruleset dpr1 (variable invalid_var) is " +
                "when Id_3 = \"CREDIT\" then Me_1 >= 0 errorcode \"Bad credit\"; " +
                "when Id_3 = \"DEBIT\" then Me_1 >= 0 errorcode \"Bad debit\" " +
                "end datapoint ruleset; " +
                "DS_r := check_datapoint(DS_1, dpr1);"))
                .hasMessageContaining("Variable invalid_var not contained in DS_1");
        assertThatThrownBy(() -> engine.eval("define datapoint ruleset dpr1 (valuedomain bad_vd) is " +
                "bad_vd = \"AA\" errorcode \"Bad\" " +
                "end datapoint ruleset; " +
                "DS_r := check_datapoint(DS_1, dpr1);"))
                .hasMessageContaining("Valuedomain bad_vd not used in DS_1 components");
        assertThatThrownBy(() -> engine.eval("DS_r := check_datapoint(DS_1, dpr1111);"))
                .hasMessageContaining("undefined variable dpr1111");
        assertThatThrownBy(() -> engine.eval("define datapoint ruleset dpr1 (variable Id_3 as Id_1, Me_1) is " +
                "when Id_1 = \"CREDIT\" then Me_1 >= 0 errorcode \"Bad credit\"; " +
                "when Id_1 = \"DEBIT\" then Me_1 >= 0 errorcode \"Bad debit\" " +
                "end datapoint ruleset; " +
                "DS_r := check_datapoint(DS_1, dpr1);"))
                .hasMessageContaining("Alias Id_1 from dpr1 ruleset already defined in DS_1");
    }

    @Test
    public void testValidationSimpleException() {

        ScriptContext context = engine.getContext();
        context.setAttribute("dsExprOk", dsExprOk, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("dsExprKo1", dsExprKo1, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("dsExprKo2", dsExprKo2, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("dsImbalanceKo", dsImbalanceKo, ScriptContext.ENGINE_SCOPE);

        assertThatThrownBy(() -> engine.eval("DS_1 := check(dsExprKo1);"))
                .hasMessageContaining("Check operand dataset contains several measures");
        assertThatThrownBy(() -> engine.eval("DS_2 := check(dsExprKo2);"))
                .hasMessageContaining("Check operand dataset measure has to be boolean");
        assertThatThrownBy(() -> engine.eval("DS_3 := check(dsExprOk imbalance dsImbalanceKo);"))
                .hasMessageContaining("Check imbalance dataset contains several measures");
        assertThatThrownBy(() -> engine.eval("DS_4 := check(dsExprOk imbalance dsExprOk);"))
                .hasMessageContaining("Check imbalance dataset measure has to be numeric");
    }
}
