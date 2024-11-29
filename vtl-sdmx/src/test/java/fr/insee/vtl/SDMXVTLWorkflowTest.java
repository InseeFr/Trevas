package fr.insee.vtl;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.PersistentDataset;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.model.utils.Java8Helpers;
import fr.insee.vtl.sdmx.SDMXVTLWorkflow;
import io.sdmx.api.io.ReadableDataLocation;
import io.sdmx.utils.core.io.ReadableDataLocationTmp;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SDMXVTLWorkflowTest {

    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {

        ScriptEngineManager mgr = new ScriptEngineManager();
        engine = mgr.getEngineByExtension("vtl");
    }

    @Disabled
    @Test
    void testRefFromRepo() {
        // Works partially, the transformation does not pull in the ruleset. Maybe the transformation is wrong and does not
        // reference ruleset?
        String url = "https://registry.sdmx.io/sdmx/v2/structure/transformationscheme/FR1/BPE_CENSUS/+/?format=sdmx-3.0&references=all";
        ReadableDataLocation rdl = new ReadableDataLocationTmp(url);
        SDMXVTLWorkflow sdmxVtlWorkflow = new SDMXVTLWorkflow(engine, rdl, Java8Helpers.mapOf());
        System.out.println(sdmxVtlWorkflow.getEmptyDatasets());

        System.out.println(sdmxVtlWorkflow.getTransformationsVTL());

        // Invalid step definition for:CHECK_MUNICIPALITY
        // - Caused by: fr.insee.vtl.engine.exceptions.UndefinedVariableException: undefined variable UNIQUE_MUNICIPALITY
        engine.getBindings(ScriptContext.ENGINE_SCOPE).putAll(sdmxVtlWorkflow.getEmptyDatasets());
        Map<String, PersistentDataset> result = sdmxVtlWorkflow.run();
        assertThat(result).containsKeys(
                "BPE_CENSUS_NUTS3_2021", "BPE_MUNICIPALITY", "BPE_NUTS3"
        );
    }

    @Test
    void testGetEmptyDataset() {

        SparkSession.builder()
                .appName("test")
                .master("local")
                .getOrCreate();

        ScriptEngineManager mgr = new ScriptEngineManager();
        ScriptEngine engine = mgr.getEngineByExtension("vtl");
        engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");

        ReadableDataLocation rdl = new ReadableDataLocationTmp("src/test/resources/DSD_BPE_CENSUS.xml");
        SDMXVTLWorkflow sdmxVtlWorkflow = new SDMXVTLWorkflow(engine, rdl, Java8Helpers.mapOf());
        Map<String, Dataset> emptyDatasets = sdmxVtlWorkflow.getEmptyDatasets();

        engine.getBindings(ScriptContext.ENGINE_SCOPE).putAll(emptyDatasets);
        Map<String, PersistentDataset> result = sdmxVtlWorkflow.run();
        assertThat(result).containsKeys(
                "BPE_CENSUS_NUTS3_2021", "BPE_MUNICIPALITY", "BPE_NUTS3"
        );

        assertThat(result.get("BPE_CENSUS_NUTS3_2021").getDataStructure()).isEqualTo(
                new Structured.DataStructure(Java8Helpers.listOf(
                        new Structured.Component("nuts3", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("pract_per_10000_inhabitants", Double.class, Dataset.Role.MEASURE)
                ))
        );
        assertThat(result.get("BPE_MUNICIPALITY").getDataStructure()).isEqualTo(
                new Structured.DataStructure(Java8Helpers.listOf(
                        new Structured.Component("facility_type", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("municipality", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("year", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("nb", Long.class, Dataset.Role.MEASURE)
                ))
        );
        assertThat(result.get("BPE_NUTS3").getDataStructure()).isEqualTo(
                new Structured.DataStructure(Java8Helpers.listOf(
                        new Structured.Component("year", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("facility_type", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("nuts3", String.class, Dataset.Role.IDENTIFIER),
                        new Structured.Component("nb", Long.class, Dataset.Role.MEASURE)
                ))
        );
    }

    @Test
    public void testGetRulesetsVTL() {
        ReadableDataLocation rdl = new ReadableDataLocationTmp("src/test/resources/DSD_BPE_CENSUS.xml");
        SDMXVTLWorkflow sdmxVtlWorkflow = new SDMXVTLWorkflow(engine, rdl, Java8Helpers.mapOf());
        assertThat(sdmxVtlWorkflow.getRulesetsVTL()).isEqualTo(
                "define datapoint ruleset UNIQUE_MUNICIPALITY (valuedomain CL_DEPCOM) is\n" +
                        "                    MUNICIPALITY_FORMAT_RULE : match_characters(CL_DEPCOM, \"[0-9]{5}|2[A-B][0-9]{3}\") errorcode \"Municipality code is not in the correct format\"\n" +
                        "                end datapoint ruleset;\n" +
                        "\n" +
                        "define datapoint ruleset NUTS3_TYPES (variable facility_type, nb) is\n" +
                        "                    BOWLING_ALLEY_RULE : when facility_type = \"F102\" then nb > 10 errorcode \"Not enough bowling alleys\"\n" +
                        "                end datapoint ruleset;"
        );
    }

    @Test
    public void testGetTransformationsVTL() {
        ReadableDataLocation rdl = new ReadableDataLocationTmp("src/test/resources/DSD_BPE_CENSUS.xml");
        SDMXVTLWorkflow sdmxVtlWorkflow = new SDMXVTLWorkflow(engine, rdl, Java8Helpers.mapOf());
        assertThat(sdmxVtlWorkflow.getTransformationsVTL()).isEqualTo(
                "// Validation of municipality code in input file\n" +
                        "CHECK_MUNICIPALITY := check_datapoint(BPE_DETAIL_VTL, UNIQUE_MUNICIPALITY invalid);\n" +
                        "\n" +
                        "// Clean BPE input database\n" +
                        "BPE_DETAIL_CLEAN := BPE_DETAIL_VTL  [drop LAMBERT_X, LAMBERT_Y]\n" +
                        "                            [rename ID_EQUIPEMENT to id, TYPEQU to facility_type, DEPCOM to municipality, REF_YEAR to year];\n" +
                        "\n" +
                        "// BPE aggregation by municipality, type and year\n" +
                        "BPE_MUNICIPALITY <- BPE_DETAIL_CLEAN    [aggr nb := count(id) group by municipality, year, facility_type];\n" +
                        "\n" +
                        "// BPE aggregation by NUTS 3, type and year\n" +
                        "BPE_NUTS3 <- BPE_MUNICIPALITY    [calc nuts3 := if substr(municipality,1,2) = \"97\" then substr(municipality,1,3) else substr(municipality,1,2)]\n" +
                        "                                    [aggr nb := count(nb) group by year, nuts3, facility_type];\n" +
                        "\n" +
                        "// BPE validation of facility types by NUTS 3\n" +
                        "CHECK_NUTS3_TYPES := check_datapoint(BPE_NUTS3, NUTS3_TYPES invalid);\n" +
                        "\n" +
                        "// Prepare 2021 census dataset by NUTS 3\n" +
                        "CENSUS_NUTS3_2021 := LEGAL_POP   [rename REF_AREA to nuts3, TIME_PERIOD to year, POP_TOT to pop]\n" +
                        "                                    [filter year = \"2021\"]\n" +
                        "                                    [calc pop := cast(pop, integer)]\n" +
                        "                                    [drop year, NB_COM, POP_MUNI];\n" +
                        "\n" +
                        "// Extract dataset on general practitioners from BPE by NUTS 3 in 2021\n" +
                        "GENERAL_PRACT_NUTS3_2021 := BPE_NUTS3   [filter facility_type = \"D201\" and year = \"2021\"]\n" +
                        "                            [drop facility_type, year];\n" +
                        "\n" +
                        "// Merge practitioners and legal population datasets by NUTS 3 in 2021 and compute an indicator\n" +
                        "BPE_CENSUS_NUTS3_2021 <- inner_join(GENERAL_PRACT_NUTS3_2021, CENSUS_NUTS3_2021)\n" +
                        "                        [calc pract_per_10000_inhabitants := nb / pop * 10000]\n" +
                        "                        [drop nb, pop];"
        );
    }
}