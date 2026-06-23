package fr.insee.vtl.spark.processing.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SubspaceTest {

  private final InMemoryDataset multiIdDataset =
      new InMemoryDataset(
          List.of(
              Map.of("Id_1", 1L, "Id_2", "A", "Id_3", "X", "Me_1", 100L, "At_1", "a"),
              Map.of("Id_1", 1L, "Id_2", "A", "Id_3", "Y", "Me_1", 200L, "At_1", "b"),
              Map.of("Id_1", 1L, "Id_2", "B", "Id_3", "Z", "Me_1", 300L, "At_1", "c"),
              Map.of("Id_1", 2L, "Id_2", "A", "Id_3", "W", "Me_1", 400L, "At_1", "d")),
          Map.of(
              "Id_1", Long.class,
              "Id_2", String.class,
              "Id_3", String.class,
              "Me_1", Long.class,
              "At_1", String.class),
          Map.of(
              "Id_1", Dataset.Role.IDENTIFIER,
              "Id_2", Dataset.Role.IDENTIFIER,
              "Id_3", Dataset.Role.IDENTIFIER,
              "Me_1", Dataset.Role.MEASURE,
              "At_1", Dataset.Role.ATTRIBUTE));

  private SparkSession spark;
  private ScriptEngine engine;
  private ScriptContext context;

  @BeforeEach
  public void setUp() {
    ScriptEngineManager mgr = new ScriptEngineManager();
    engine = mgr.getEngineByExtension("vtl");
    spark = SparkSession.builder().appName("test").master("local").getOrCreate();
    SparkSession.setActiveSession(spark);
    engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    context = engine.getContext();
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  public void testSubspaceClause() throws ScriptException {
    context.setAttribute("ds1", multiIdDataset, ScriptContext.ENGINE_SCOPE);

    engine.eval("ds_r := ds1[sub Id_1 = 1, Id_2 = \"A\"];");

    Dataset dsR = (Dataset) context.getAttribute("ds_r");
    assertThat(dsR.getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("Id_3", "X", "Me_1", 100L, "At_1", "a"),
            Map.of("Id_3", "Y", "Me_1", 200L, "At_1", "b"));
    assertThat(dsR.getDataStructure().getIdentifiers())
        .extracting(Structured.Component::getName)
        .containsExactly("Id_3");
    assertThat(dsR.getDataStructure().getMeasures())
        .extracting(Structured.Component::getName)
        .containsExactly("Me_1");
    assertThat(dsR.getDataStructure().getAttributes())
        .extracting(Structured.Component::getName)
        .containsExactly("At_1");
  }

  @Test
  public void testSubspaceClause_singleIdentifier() throws ScriptException {
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(
                Map.of("country", "france", "name", "Nico", "age", 11L),
                Map.of("country", "france", "name", "Hadrien", "age", 10L),
                Map.of("country", "norway", "name", "Franck", "age", 12L)),
            Map.of("country", String.class, "name", String.class, "age", Long.class),
            Map.of(
                "country", Dataset.Role.IDENTIFIER,
                "name", Dataset.Role.IDENTIFIER,
                "age", Dataset.Role.MEASURE));

    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);
    engine.eval("ds2 := ds1[sub country = \"france\"];");

    Dataset ds2 = (Dataset) context.getAttribute("ds2");
    assertThat(ds2.getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("name", "Nico", "age", 11L), Map.of("name", "Hadrien", "age", 10L));
    assertThat(ds2.getDataStructure().getIdentifiers())
        .extracting(Structured.Component::getName)
        .containsExactly("name");
  }

  @Test
  public void testSubspaceClause_chainedWithCalc() throws ScriptException {
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(
                Map.of("region", "EU", "name", "Nico", "value", 10L),
                Map.of("region", "EU", "name", "Hadrien", "value", 20L)),
            Map.of("region", String.class, "name", String.class, "value", Long.class),
            Map.of(
                "region", Dataset.Role.IDENTIFIER,
                "name", Dataset.Role.IDENTIFIER,
                "value", Dataset.Role.MEASURE));

    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);
    engine.eval("ds2 := ds1[sub region = \"EU\"][calc doubled := value * 2];");

    Dataset ds2 = (Dataset) context.getAttribute("ds2");
    assertThat(ds2.getDataAsMap())
        .containsExactlyInAnyOrder(
            Map.of("name", "Nico", "value", 10L, "doubled", 20L),
            Map.of("name", "Hadrien", "value", 20L, "doubled", 40L));
  }

  @Test
  public void testSubspaceClause_emptyResult() throws ScriptException {
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(Map.of("country", "france", "name", "Nico")),
            Map.of("country", String.class, "name", String.class),
            Map.of("country", Dataset.Role.IDENTIFIER, "name", Dataset.Role.IDENTIFIER));

    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);
    engine.eval("ds2 := ds1[sub country = \"norway\"];");

    Dataset ds2 = (Dataset) context.getAttribute("ds2");
    assertThat(ds2.getDataAsMap()).isEmpty();
    assertThat(ds2.getDataStructure().getIdentifiers())
        .extracting(Structured.Component::getName)
        .containsExactly("name");
  }

  @Test
  public void testSubspaceClause_unknownIdentifier() {
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(Map.of("name", "Nico", "age", 11L)),
            Map.of("name", String.class, "age", Long.class),
            Map.of("name", Dataset.Role.IDENTIFIER, "age", Dataset.Role.MEASURE));

    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    assertThatThrownBy(() -> engine.eval("ds := ds1[sub missing = 1];"))
        .hasMessageContaining("undefined variable 'missing'");
  }

  @Test
  public void testSubspaceClause_duplicateIdentifier() {
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(Map.of("country", "france", "name", "Nico")),
            Map.of("country", String.class, "name", String.class),
            Map.of("country", Dataset.Role.IDENTIFIER, "name", Dataset.Role.IDENTIFIER));

    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    assertThatThrownBy(
            () -> engine.eval("ds := ds1[sub country = \"france\", country = \"norway\"];"))
        .hasMessageContaining("duplicate identifier 'country'");
  }

  @Test
  public void testSubspaceClause_notAnIdentifier() {
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(Map.of("name", "Nico", "age", 11L)),
            Map.of("name", String.class, "age", Long.class),
            Map.of("name", Dataset.Role.IDENTIFIER, "age", Dataset.Role.MEASURE));

    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    assertThatThrownBy(() -> engine.eval("ds := ds1[sub age = 11];"))
        .hasMessageContaining("sub can only fix identifier components");
  }

  @Test
  public void testSubspaceClause_typeMismatch() {
    InMemoryDataset dataset =
        new InMemoryDataset(
            List.of(Map.of("name", "Nico", "code", 1L)),
            Map.of("name", String.class, "code", Long.class),
            Map.of("name", Dataset.Role.IDENTIFIER, "code", Dataset.Role.IDENTIFIER));

    context.setAttribute("ds1", dataset, ScriptContext.ENGINE_SCOPE);

    assertThatThrownBy(() -> engine.eval("ds := ds1[sub name = 1];"))
        .hasMessageContaining("invalid type");
  }
}
