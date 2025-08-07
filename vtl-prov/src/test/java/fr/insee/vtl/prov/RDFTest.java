package fr.insee.vtl.prov;

import static org.assertj.core.api.Assertions.assertThat;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.prov.prov.Program;
import fr.insee.vtl.prov.utils.PropertiesLoader;
import fr.insee.vtl.prov.utils.RDFUtils;
import fr.insee.vtl.sdmx.TrevasSDMXUtils;
import fr.insee.vtl.spark.SparkDataset;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.jena.rdf.model.Model;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RDFTest {

  private SparkSession spark;
  private ScriptEngine engine;

  static Properties conf;

  static {
    try {
      conf = PropertiesLoader.loadProperties();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static String sparqlEndpoint =
      conf.getProperty("sparql-endpoint-url")
          + conf.getProperty("trevas-provenance-repository-path");
  static String sparqlEndpointUser = conf.getProperty("sparql-endpoint-user");
  static String sparlqEndpointPassword = conf.getProperty("sparql-endpoint-password");

  String BLUEPRINT_CLASS_PATH = "docs/configuration/provenance/blueprint-class-metadata.ttl";

  String BLUEPRINT_DETAILS_PATH = "docs/configuration/provenance/blueprint-detail-metadata.ttl";

  String BLUEPRINT_LINK_PATH = "docs/configuration/provenance/blueprint-link-metadata.ttl";

  @BeforeEach
  void loadBlueprintConfig() {
    Model modelClass = RDFUtils.initModel(BLUEPRINT_CLASS_PATH);
    Model modelDetail = RDFUtils.initModel(BLUEPRINT_DETAILS_PATH);
    Model modelLink = RDFUtils.initModel(BLUEPRINT_LINK_PATH);
    Model model = modelClass.add(modelDetail).add(modelLink);
    RDFUtils.loadModelWithCredentials(
        model, sparqlEndpoint, sparqlEndpointUser, sparlqEndpointPassword);

    spark = SparkSession.builder().appName("test").master("local").getOrCreate();

    ScriptEngineManager mgr = new ScriptEngineManager();
    engine = mgr.getEngineByExtension("vtl");
    engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    engine.put("$vtl.spark.session", spark);
  }

  @BeforeEach
  void createFolderIfNotExists() {
    String path = "src/test/resources/output";
    File folder = new File(path);
    if (!folder.exists()) {
      folder.mkdirs();
    }
  }

  @Test
  public void simpleTest() throws IOException, ScriptException {

    String script =
        """
                        ds_sum := ds1 + ds2;
                        ds_mul := ds_sum * 3;\s
                        ds_res <- ds_mul   [filter mod(var1, 2) = 0]\
                                           [calc var_sum := var1 + var2];\
                        """;
    Map<String, Class<?>> types =
        Map.of("id", String.class, "var1", Long.class, "var2", Long.class);
    Map<String, Dataset.Role> roles =
        Map.of(
            "id",
            Dataset.Role.IDENTIFIER,
            "var1",
            Dataset.Role.MEASURE,
            "var2",
            Dataset.Role.MEASURE);
    InMemoryDataset ds1 =
        new InMemoryDataset(
            List.of(
                Map.of("id", "1", "var1", 10L, "var2", 11L),
                Map.of("id", "2", "var1", 11L, "var2", 10L),
                Map.of("id", "3", "var1", 12L, "var2", 9L)),
            types,
            roles);
    InMemoryDataset ds2 =
        new InMemoryDataset(
            List.of(
                Map.of("id", "1", "var1", 20L, "var2", 110L),
                Map.of("id", "2", "var1", -1L, "var2", 10L),
                Map.of("id", "3", "var1", 0L, "var2", 9L)),
            types,
            roles);

    ScriptContext context = engine.getContext();
    context.setAttribute("ds1", ds1, ScriptContext.ENGINE_SCOPE);
    context.setAttribute("ds2", ds2, ScriptContext.ENGINE_SCOPE);

    Program program =
        ProvenanceListener.run(
            engine, script, "trevas-simple-test", "Simple test from Trevas tests");
    Model model = RDFUtils.buildModel(program);
    String content = RDFUtils.serialize(model, "JSON-LD");
    assertThat(content).isNotEmpty();
    RDFUtils.loadModelWithCredentials(
        model, sparqlEndpoint, sparqlEndpointUser, sparlqEndpointPassword);
    RDFUtils.writeJsonLdToFile(model, "src/test/resources/output/test-simple.json");
    assertThat(program.getProgramSteps()).hasSize(3);
  }

  @Test
  public void bpeTest() throws IOException, ScriptException {

    String bpeScript =
        """
                        // Define UNIQUE_MUNICIPALITY ruleset
                        define datapoint ruleset UNIQUE_MUNICIPALITY (variable DEPCOM) is
                          MUNICIPALITY_FORMAT_RULE : match_characters(DEPCOM, "[0-9]{5}|2[A-B][0-9]{3}") errorcode "Municipality code is not in the correct format"
                        end datapoint ruleset;

                        // Validation of municipality code in input file
                        CHECK_MUNICIPALITY := check_datapoint(BPE_DETAIL_VTL, UNIQUE_MUNICIPALITY invalid);

                        // Clean BPE input database
                        BPE_DETAIL_CLEAN := BPE_DETAIL_VTL  [drop LAMBERT_X, LAMBERT_Y]
                                                    [rename ID_EQUIPEMENT to id, TYPEQU to facility_type, DEPCOM to municipality, REF_YEAR to year];

                        // BPE aggregation by municipality, type and year
                        BPE_MUNICIPALITY <- BPE_DETAIL_CLEAN    [aggr nb := count(id) group by municipality, year, facility_type];

                        // BPE aggregation by NUTS 3, type and year
                        BPE_NUTS3 <- BPE_MUNICIPALITY    [calc nuts3 := if substr(municipality,1,2) = "97" then substr(municipality,1,3) else substr(municipality,1,2)]
                                                            [aggr nb := count(nb) group by year, nuts3, facility_type];

                        // Define ruleset
                        define datapoint ruleset NUTS3_TYPES (variable facility_type, nb) is
                         BOWLING_ALLEY_RULE : when facility_type = "F102" then nb > 10 errorcode "Not enough bowling alleys"
                        end datapoint ruleset;

                        // BPE validation of facility types by NUTS 3
                        CHECK_NUTS3_TYPES := check_datapoint(BPE_NUTS3, NUTS3_TYPES invalid);

                        // Prepare 2021 census dataset by NUTS 3
                        CENSUS_NUTS3_2021 := LEGAL_POP   [rename REF_AREA to nuts3, TIME_PERIOD to year, POP_TOT to pop]
                                                            [filter year = "2021"]
                                                            [calc pop := cast(pop, integer)]
                                                            [drop year, NB_COM, POP_MUNI];

                        // Extract dataset on general practitioners from BPE by NUTS 3 in 2021
                        GENERAL_PRACT_NUTS3_2021 := BPE_NUTS3   [filter facility_type = "D201" and year = "2021"]
                                                    [drop facility_type, year];

                        // Merge practitioners and legal population datasets by NUTS 3 in 2021 and compute an indicator
                        BPE_CENSUS_NUTS3_2021 <- inner_join(GENERAL_PRACT_NUTS3_2021, CENSUS_NUTS3_2021)
                                                [calc pract_per_10000_inhabitants := nb / pop * 10000]
                                                [drop nb, pop];\
                        """;
    Structured.DataStructure bpeStructure =
        TrevasSDMXUtils.buildStructureFromSDMX3(
            "src/test/resources/input/DSD_BPE_CENSUS.xml", "BPE_DETAIL_VTL");
    Structured.DataStructure censusStructure =
        TrevasSDMXUtils.buildStructureFromSDMX3(
            "src/test/resources/input/DSD_BPE_CENSUS.xml", "LEGAL_POP");

    SparkDataset bpeDetailDs =
        new SparkDataset(
            spark
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .option("quote", "\"")
                .csv("src/test/resources/input/BPE_DETAIL_SAMPLE.csv"),
            bpeStructure);

    SparkDataset censusNuts =
        new SparkDataset(
            spark
                .read()
                .option("header", "true")
                .option("delimiter", ";")
                .option("quote", "\"")
                .csv("src/test/resources/input/LEGAL_POP_NUTS3.csv"),
            censusStructure);

    ScriptContext context = engine.getContext();
    context.setAttribute("BPE_DETAIL_VTL", bpeDetailDs, ScriptContext.ENGINE_SCOPE);
    context.setAttribute("LEGAL_POP", censusNuts, ScriptContext.ENGINE_SCOPE);

    Program program =
        ProvenanceListener.run(engine, bpeScript, "trevas-bpe-test", "BPE from Trevas tests");
    Model model = RDFUtils.buildModel(program);
    RDFUtils.loadModelWithCredentials(
        model, sparqlEndpoint, sparqlEndpointUser, sparlqEndpointPassword);
    RDFUtils.writeJsonLdToFile(model, "src/test/resources/output/test-bpe.json");
    assertThat(program.getProgramSteps()).hasSize(8);
  }
}
