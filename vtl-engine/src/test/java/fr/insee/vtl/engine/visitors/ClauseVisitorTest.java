package fr.insee.vtl.engine.visitors;

import fr.insee.vtl.engine.exceptions.VtlScriptException;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static fr.insee.vtl.engine.VtlScriptEngineTest.atPosition;
import static fr.insee.vtl.model.Dataset.Role;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ClauseVisitorTest {

    private ScriptEngine engine;


    @BeforeEach
    public void setUp() {
        engine = new ScriptEngineManager().getEngineByName("vtl");
    }

    @Test
    public void testFilterClause() throws ScriptException {

        var dataset = new InMemoryDataset(
                List.of(
                        new Structured.Component("name", String.class, Role.IDENTIFIER),
                        new Structured.Component("age", Long.class, Role.MEASURE),
                        new Structured.Component("weight", Long.class, Role.MEASURE)
                ),
                Arrays.asList("Toto", null, 100L),
                Arrays.asList("Hadrien", 10L, 11L),
                Arrays.asList("Nico", 11L, 10L),
                Arrays.asList("Franck", 12L, 9L)
        );

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds := ds1[filter age > 10 and age < 12];");

        assertThat(engine.getContext().getAttribute("ds")).isInstanceOf(Dataset.class);
        assertThat(((Dataset) engine.getContext().getAttribute("ds")).getDataAsMap()).isEqualTo(List.of(
                Map.of("name", "Nico", "age", 11L, "weight", 10L)
        ));

        engine.eval("ds := ds1[filter age > 10 or age < 12];");

        assertThat(((Dataset) engine.getContext().getAttribute("ds")).getDataAsMap()).isEqualTo(List.of(
                Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                Map.of("name", "Nico", "age", 11L, "weight", 10L),
                Map.of("name", "Franck", "age", 12L, "weight", 9L)
        ));
    }

    @Test
    public void testManyCalc() throws ScriptException {
        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                        Map.of("name", "Nico", "age", 11L, "weight", 10L),
                        Map.of("name", "Franck", "age", 12L, "weight", 9L)
                ),
                Map.of("name", String.class, "age", Long.class, "weight", Long.class),
                Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE)
        );

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds := ds1[rename age to wisdom][calc wisdom := wisdom * 2];");

        var ds = (Dataset) engine.getContext().getAttribute("ds");
        assertThat(ds.getDataAsMap()).contains(
                Map.of("name", "Hadrien", "weight", 11L, "wisdom", 20L),
                Map.of("name", "Nico", "weight", 10L, "wisdom", 22L),
                Map.of("name", "Franck", "weight", 9L, "wisdom", 24L)
        );
    }

    @Test
    public void testCalcRoleModifier() throws ScriptException {
        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                        Map.of("name", "Nico", "age", 11L, "weight", 10L),
                        Map.of("name", "Franck", "age", 12L, "weight", 9L)
                ),
                Map.of("name", String.class, "age", Long.class, "weight", Long.class),
                Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE)
        );

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds := ds1[calc new_age := age + 1, identifier id := name, attribute 'unit' := \"year\"];");

        Dataset ds = (Dataset) context.getAttribute("ds");
        Dataset.Component idComponent = ds.getDataStructure().values().stream().filter(component ->
                component.getName().equals("id")
        ).findFirst().orElse(null);
        Dataset.Component ageComponent = ds.getDataStructure().values().stream().filter(component ->
                component.getName().equals("new_age")
        ).findFirst().orElse(null);
        Dataset.Component unitComponent = ds.getDataStructure().values().stream().filter(component ->
                component.getName().equals("unit")
        ).findFirst().orElse(null);

        assertThat(ageComponent.getRole()).isEqualTo(Role.MEASURE);
        assertThat(idComponent.getRole()).isEqualTo(Role.IDENTIFIER);
        assertThat(unitComponent.getRole()).isEqualTo(Role.ATTRIBUTE);
    }

    @Test
    public void testRenameClause() throws ScriptException {
        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                        Map.of("name", "Nico", "age", 11L, "weight", 10L),
                        Map.of("name", "Franck", "age", 12L, "weight", 9L)
                ),
                Map.of("name", String.class, "age", Long.class, "weight", Long.class),
                Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE)
        );

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds := ds1[rename age to weight, weight to age, name to pseudo];");

        assertThat(engine.getContext().getAttribute("ds")).isInstanceOf(Dataset.class);
        assertThat(((Dataset) engine.getContext().getAttribute("ds")).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("pseudo", "Hadrien", "weight", 10L, "age", 11L),
                Map.of("pseudo", "Nico", "weight", 11L, "age", 10L),
                Map.of("pseudo", "Franck", "weight", 12L, "age", 9L)
        );

        assertThatThrownBy(() -> engine.eval("ds := ds1[rename age to weight, weight to age, name to age];"))
                .isInstanceOf(VtlScriptException.class)
                .hasMessage("duplicate column: age")
                .is(atPosition(0, 47, 58));
    }

    @Test
    public void testCalcClause() throws ScriptException {

        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                        Map.of("name", "Nico", "age", 11L, "weight", 10L),
                        Map.of("name", "Franck", "age", 12L, "weight", 9L)
                ),
                Map.of("name", String.class, "age", Long.class, "weight", Long.class),
                Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE)
        );

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds := ds1[calc res := age + weight / 2];");

        assertThat(engine.getContext().getAttribute("ds")).isInstanceOf(Dataset.class);
        assertThat(((Dataset) engine.getContext().getAttribute("ds")).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("name", "Hadrien", "res", 15.5, "age", 10L, "weight", 11L),
                Map.of("name", "Nico", "res", 16.0, "age", 11L, "weight", 10L),
                Map.of("name", "Franck", "res", 16.5, "age", 12L, "weight", 9L)
        );

    }

    @Test
    public void testKeepDropClause() throws ScriptException {
        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "age", 10L, "weight", 11L),
                        Map.of("name", "Nico", "age", 11L, "weight", 10L),
                        Map.of("name", "Franck", "age", 12L, "weight", 9L)
                ),
                Map.of("name", String.class, "age", Long.class, "weight", Long.class),
                Map.of("name", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE)
        );

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

        engine.eval("ds := ds1[keep name, age];");

        assertThat(engine.getContext().getAttribute("ds")).isInstanceOf(Dataset.class);
        assertThat(((Dataset) engine.getContext().getAttribute("ds")).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("name", "Hadrien", "age", 10L),
                Map.of("name", "Nico", "age", 11L),
                Map.of("name", "Franck", "age", 12L)
        );

        engine.eval("ds := ds1[drop weight];");

        assertThat(engine.getContext().getAttribute("ds")).isInstanceOf(Dataset.class);
        assertThat(((Dataset) engine.getContext().getAttribute("ds")).getDataAsMap()).containsExactlyInAnyOrder(
                Map.of("name", "Hadrien", "age", 10L),
                Map.of("name", "Nico", "age", 11L),
                Map.of("name", "Franck", "age", 12L)
        );
    }

    @Test
    public void testAggregate() throws ScriptException {

        InMemoryDataset dataset = new InMemoryDataset(
                List.of(
                        Map.of("name", "Hadrien", "country", "norway", "age", 10L, "weight", 11D),
                        Map.of("name", "Nico", "country", "france", "age", 11L, "weight", 10D),
                        Map.of("name", "Franck", "country", "france", "age", 12L, "weight", 9D)
                ),
                Map.of("name", String.class, "country", String.class, "age", Long.class, "weight", Double.class),
                Map.of("name", Role.IDENTIFIER, "country", Role.IDENTIFIER, "age", Role.MEASURE, "weight", Role.MEASURE)
        );

        ScriptContext context = engine.getContext();
        context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

        // test := ds1[aggr sumAge := sum(age) group by country];
        // test := ds1[aggr sumAge := sum(age group by country)];
        // test := ds1[aggr sumAge := sum(age group by country), totalWeight := sum(weight group by country)];
        // test := ds1[aggr sumAge := sum(age), totalWeight := sum(weight) group by country];

        engine.eval("res := ds1[aggr " +
                    "sumAge := sum(age)," +
                    "avgWeight := avg(age)," +
                    "countVal := count(null)" +
                    " group by country];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("country", "france", "sumAge", 23L, "avgWeight", 11.5, "countVal", 2L),
                Map.of("country", "norway", "sumAge", 10L, "avgWeight", 10.0, "countVal", 1L)
        );

        engine.eval("res := ds1[aggr " +
                "maxAge := max(age)," +
                "maxWeight := max(weight)" +
                " group by country];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("country", "france", "maxAge", 12L, "maxWeight", 10D),
                Map.of("country", "norway", "maxAge", 10L, "maxWeight", 11D)
        );

        engine.eval("res := ds1[aggr " +
                "minAge := min(age)," +
                "minWeight := min(weight)" +
                " group by country];");
        assertThat(engine.getContext().getAttribute("res")).isInstanceOf(Dataset.class);
        assertThat(((Dataset) engine.getContext().getAttribute("res")).getDataAsMap()).containsExactly(
                Map.of("country", "france", "minAge", 11L, "minWeight", 9D),
                Map.of("country", "norway", "minAge", 10L, "minWeight", 11D)
        );
    }
}
