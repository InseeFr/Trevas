package fr.insee.vtl.engine.visitors.expression;

import fr.insee.vtl.engine.samples.DatasetSamples;
import fr.insee.vtl.model.utils.Java8Helpers;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.exceptions.VtlScriptException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ComparisonExprTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testNull() throws ScriptException {

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);

        List<String> operators = Java8Helpers.listOf("=", "<>", "<", ">", "<=", ">=");
        List<List<String>> values = Java8Helpers.listOf(
                Java8Helpers.listOf("\"string\"", "string"),
                Java8Helpers.listOf("1.1", "number"),
                Java8Helpers.listOf("1", "integer"),
                Java8Helpers.listOf("cast(null, number)", "number")
        );

        for (String operator : operators) {
            // Left is null
            for (List<String> value : values) {
                engine.eval("bool := cast(null, " + value.get(1) + ") " + operator + " " + value.get(0) + " ;");
                assertThat((Boolean) context.getAttribute("bool")).isNull();
            }

            // Right is null
            for (List<String> value : values) {
                engine.eval("bool := " + value.get(0) + " " + operator + " cast(null, " + value.get(1) + ");");
                assertThat((Boolean) context.getAttribute("bool")).isNull();
            }
        }
    }

    @Test
    public void testComparisonExpr() throws ScriptException {
        ScriptContext context = engine.getContext();
        // EQ
        engine.eval("bool := true = true;");
        assertThat((Boolean) context.getAttribute("bool")).isTrue();
        engine.eval("long := 6 = (3*2);");
        assertThat((Boolean) context.getAttribute("long")).isTrue();
        engine.eval("mix := 6 = (3*2.0);");
        assertThat((Boolean) context.getAttribute("mix")).isTrue();

        context.setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
        engine.eval("equal := ds1[keep id, long1] = ds2[keep id, long1];");
        Object equal = engine.getContext().getAttribute("equal");
        assertThat(((Dataset) equal).getDataAsMap()).containsExactlyInAnyOrder(
                Java8Helpers.mapOf("id", "Hadrien", "bool_var", false),
                Java8Helpers.mapOf("id", "Nico", "bool_var", true),
                Java8Helpers.mapOf("id", "Franck", "bool_var", true)
        );
        assertThat(((Dataset) equal).getDataStructure().get("bool_var").getType()).isEqualTo(Boolean.class);

        // NEQ
        engine.eval("bool := true <> true;");
        assertThat((Boolean) context.getAttribute("bool")).isFalse();
        engine.eval("long := 6 <> (3*20);");
        assertThat((Boolean) context.getAttribute("long")).isTrue();
        engine.eval("mix := 6 <> (3*20.0);");
        assertThat((Boolean) context.getAttribute("mix")).isTrue();
        context.setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
        engine.eval("notEqual := ds1[keep id, long1] <> ds2[keep id, long1];");
        Object notEqual = engine.getContext().getAttribute("notEqual");
        assertThat(((Dataset) notEqual).getDataAsMap()).containsExactlyInAnyOrder(
                Java8Helpers.mapOf("id", "Hadrien", "bool_var", true),
                Java8Helpers.mapOf("id", "Nico", "bool_var", false),
                Java8Helpers.mapOf("id", "Franck", "bool_var", false)
        );
        assertThat(((Dataset) equal).getDataStructure().get("bool_var").getType()).isEqualTo(Boolean.class);
        // LT
        engine.eval("lt := 2 < 3;");
        assertThat((Boolean) context.getAttribute("lt")).isTrue();
        engine.eval("lt1 := 2.1 < 1.1;");
        assertThat((Boolean) context.getAttribute("lt1")).isFalse();
        engine.eval("mix := 6 < 6.1;");
        assertThat((Boolean) context.getAttribute("mix")).isTrue();
        context.setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
        engine.eval("lt := ds1[keep id, long1] < ds2[keep id, long1];");
        Object lt = engine.getContext().getAttribute("lt");
        assertThat(((Dataset) lt).getDataAsMap()).containsExactlyInAnyOrder(
                Java8Helpers.mapOf("id", "Hadrien", "bool_var", true),
                Java8Helpers.mapOf("id", "Nico", "bool_var", false),
                Java8Helpers.mapOf("id", "Franck", "bool_var", false)
        );
        assertThat(((Dataset) equal).getDataStructure().get("bool_var").getType()).isEqualTo(Boolean.class);
        // MT
        engine.eval("lt := 2 > 3;");
        assertThat((Boolean) context.getAttribute("lt")).isFalse();
        engine.eval("lt1 := 2.1 > 1.1;");
        assertThat((Boolean) context.getAttribute("lt1")).isTrue();
        engine.eval("mix := 6 > 6.1;");
        assertThat((Boolean) context.getAttribute("mix")).isFalse();
        context.setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
        engine.eval("mt := ds1[keep id, long1] > ds2[keep id, long1];");
        Object mt = engine.getContext().getAttribute("mt");
        assertThat(((Dataset) mt).getDataAsMap()).containsExactlyInAnyOrder(
                Java8Helpers.mapOf("id", "Hadrien", "bool_var", false),
                Java8Helpers.mapOf("id", "Nico", "bool_var", false),
                Java8Helpers.mapOf("id", "Franck", "bool_var", false)
        );
        assertThat(((Dataset) equal).getDataStructure().get("bool_var").getType()).isEqualTo(Boolean.class);
        // LE
        engine.eval("lt := 3 <= 3;");
        assertThat((Boolean) context.getAttribute("lt")).isTrue();
        engine.eval("lt1 := 2.1 <= 1.1;");
        assertThat((Boolean) context.getAttribute("lt1")).isFalse();
        engine.eval("mix := 6 <= 6.1;");
        assertThat((Boolean) context.getAttribute("mix")).isTrue();
        context.setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
        engine.eval("le := ds1[keep id, long1] <= ds2[keep id, long1];");
        Object le = engine.getContext().getAttribute("le");
        assertThat(((Dataset) le).getDataAsMap()).containsExactlyInAnyOrder(
                Java8Helpers.mapOf("id", "Hadrien", "bool_var", true),
                Java8Helpers.mapOf("id", "Nico", "bool_var", true),
                Java8Helpers.mapOf("id", "Franck", "bool_var", true)
        );
        assertThat(((Dataset) equal).getDataStructure().get("bool_var").getType()).isEqualTo(Boolean.class);
        // ME
        engine.eval("me := 2 >= 3;");
        assertThat((Boolean) context.getAttribute("me")).isFalse();
        engine.eval("me1 := 2.1 >= 1.1;");
        assertThat((Boolean) context.getAttribute("me1")).isTrue();
        engine.eval("mix := 6 >= 6.1;");
        assertThat((Boolean) context.getAttribute("mix")).isFalse();

        context.setAttribute("ds1", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
        context.setAttribute("ds2", DatasetSamples.ds2, ScriptContext.ENGINE_SCOPE);
        engine.eval("me := ds1[keep id, long1] >= ds2[keep id, long1];");
        Object me = engine.getContext().getAttribute("me");
        assertThat(((Dataset) me).getDataAsMap()).containsExactlyInAnyOrder(
                Java8Helpers.mapOf("id", "Hadrien", "bool_var", false),
                Java8Helpers.mapOf("id", "Nico", "bool_var", true),
                Java8Helpers.mapOf("id", "Franck", "bool_var", true)
        );
        assertThat(((Dataset) equal).getDataStructure().get("bool_var").getType()).isEqualTo(Boolean.class);
    }

    @Test
    public void testComparisonExceptions() {
        assertThatThrownBy(() -> {
            engine.eval("s := \"ok\" <> true;");
        }).isInstanceOf(VtlScriptException.class);
        // TODO: refine message
//                .hasMessage("invalid type Boolean, expected String");
    }

    @Test
    public void testInNotIn() throws ScriptException {

        engine.eval("res := null in {1, 2, 3, 123};");
        assertThat((Boolean) engine.getContext().getAttribute("res")).isNull();

        engine.eval("res := null not_in {1, 2, 3, 123};");
        assertThat((Boolean) engine.getContext().getAttribute("res")).isNull();

        engine.eval("res := \"string\" in {\"a\",\"list\",\"with\",\"string\"};");
        assertThat((Boolean) engine.getContext().getAttribute("res")).isTrue();

        engine.eval("res := \"string\" in {\"a\",\"list\",\"with\",\"out string\"};");
        assertThat((Boolean) engine.getContext().getAttribute("res")).isFalse();

        engine.getContext().setAttribute("var", 123L, ScriptContext.ENGINE_SCOPE);
        engine.eval("res := var in {1, 2, 3, 123};");


        engine.getContext().setAttribute("ds", DatasetSamples.ds1, ScriptContext.ENGINE_SCOPE);
        engine.eval("me := ds[keep id, long1, string1] in {\"toto\", \"franck\"};");
        Object in = engine.getContext().getAttribute("me");
        assertThat(((Dataset) in).getDataAsMap()).containsExactlyInAnyOrder(
                Java8Helpers.mapOf("id", "Toto", "long1", false, "string1", true),
                Java8Helpers.mapOf("id", "Hadrien", "long1", false, "string1", false),
                Java8Helpers.mapOf("id", "Nico", "long1", false, "string1", false),
                Java8Helpers.mapOf("id", "Franck", "long1", false, "string1", true)
        );
        assertThat(((Dataset) in).getDataStructure().get("string1").getType()).isEqualTo(Boolean.class);

        assertThat((Boolean) engine.getContext().getAttribute("res")).isTrue();

        assertThatThrownBy(() -> {
            engine.eval("res := var in {1, 2, 3, \"string is not number\"};");
        });

        // TODO: improve type checking
//        assertThatThrownBy(() -> {
//            engine.eval("res := \"string is not number\" in {1, 2, 3};");
//        });


    }
}
