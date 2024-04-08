package fr.insee.vtl.sdmx;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.model.Dataset;
import fr.insee.vtl.model.PersistentDataset;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.spark.SparkDataset;
import io.sdmx.api.io.ReadableDataLocation;
import io.sdmx.utils.core.io.ReadableDataLocationTmp;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class BPETest {

    private SparkSession spark;
    private ScriptEngine engine;

    @BeforeEach
    public void setUp() {
        spark = SparkSession.builder()
                .appName("test")
                .master("local")
                .getOrCreate();

        ScriptEngineManager mgr = new ScriptEngineManager();
        engine = mgr.getEngineByExtension("vtl");
        engine.put(VtlScriptEngine.PROCESSING_ENGINE_NAMES, "spark");
    }

    @Test
    public void bpeV1() throws ScriptException {
        Structured.DataStructure bpeStructure = TrevasSDMXUtils.buildStructureFromSDMX3("src/test/resources/DSD_BPE_CENSUS.xml", "BPE_DETAIL");

        SparkDataset bpeDetailDs = new SparkDataset(
                spark.read()
                        .option("header", "true")
                        .option("delimiter", ";")
                        .option("quote", "\"")
                        .csv("src/test/resources/BPE_DETAIL_SAMPLE.csv"),
                bpeStructure
        );

        assertThat(bpeDetailDs.getDataStructure().size()).isEqualTo(6);

        ScriptContext context = engine.getContext();
        context.setAttribute("BPE_DETAIL", bpeDetailDs, ScriptContext.ENGINE_SCOPE);

        // Step 1
        engine.eval("" +
                "define datapoint ruleset UNIQUE_MUNICIPALITY (variable DEPCOM) is\n" +
                "    MUNICIPALITY_FORMAT_RULE : match_characters(DEPCOM, \"[0-9]{5}|2[A-B][0-9]{3}\") errorcode \"Municipality code is not in the correct format\"\n" +
                "end datapoint ruleset;\n" +
                "\n" +
                "CHECK_MUNICIPALITY := check_datapoint(BPE_DETAIL, UNIQUE_MUNICIPALITY invalid);");

        Dataset checkMunicipality = (Dataset) engine.getContext().getAttribute("CHECK_MUNICIPALITY");

        assertThat(checkMunicipality.getDataPoints()).isEmpty();

        // Step 2
        engine.eval("BPE_DETAIL_CLEAN := BPE_DETAIL" +
                "   [drop LAMBERT_X, LAMBERT_Y]\n" +
                "   [rename ID_EQUIPEMENT to id, TYPEQU to facility_type, DEPCOM to municipality, REF_YEAR to year];");

        Dataset bpeDetailClean = (Dataset) engine.getContext().getAttribute("BPE_DETAIL_CLEAN");
        Structured.DataStructure bpeDetailCleanStructure = bpeDetailClean.getDataStructure();

        assertThat(bpeDetailCleanStructure.get("id").getType()).isEqualTo(String.class);
        assertThat(bpeDetailCleanStructure.get("id").getRole()).isEqualTo(Dataset.Role.IDENTIFIER);

        assertThat(bpeDetailCleanStructure.get("facility_type").getType()).isEqualTo(String.class);
        assertThat(bpeDetailCleanStructure.get("facility_type").getRole()).isEqualTo(Dataset.Role.ATTRIBUTE);

        assertThat(bpeDetailCleanStructure.get("municipality").getType()).isEqualTo(String.class);
        assertThat(bpeDetailCleanStructure.get("municipality").getRole()).isEqualTo(Dataset.Role.ATTRIBUTE);

        assertThat(bpeDetailCleanStructure.get("year").getType()).isEqualTo(String.class);
        assertThat(bpeDetailCleanStructure.get("year").getRole()).isEqualTo(Dataset.Role.ATTRIBUTE);

        // Step 3
        engine.eval("BPE_MUNICIPALITY <- BPE_DETAIL_CLEAN" +
                "   [aggr nb := count(id) group by municipality, year, facility_type];");

        Dataset bpeMunicipality = (Dataset) engine.getContext().getAttribute("BPE_MUNICIPALITY");
        Structured.DataStructure bpeMunicipalityStructure = bpeMunicipality.getDataStructure();

        assertThat(bpeMunicipalityStructure.get("municipality").getType()).isEqualTo(String.class);
        assertThat(bpeMunicipalityStructure.get("municipality").getRole()).isEqualTo(Dataset.Role.IDENTIFIER);

        assertThat(bpeMunicipalityStructure.get("facility_type").getType()).isEqualTo(String.class);
        assertThat(bpeMunicipalityStructure.get("facility_type").getRole()).isEqualTo(Dataset.Role.IDENTIFIER);

        assertThat(bpeMunicipalityStructure.get("year").getType()).isEqualTo(String.class);
        assertThat(bpeMunicipalityStructure.get("year").getRole()).isEqualTo(Dataset.Role.IDENTIFIER);


        assertThat(bpeMunicipalityStructure.get("nb").getType()).isEqualTo(Long.class);
        assertThat(bpeMunicipalityStructure.get("nb").getRole()).isEqualTo(Dataset.Role.MEASURE);

        // Step 4
        engine.eval("BPE_NUTS3 <- BPE_MUNICIPALITY" +
                "   [calc nuts3 := if substr(municipality,1,2) = \"97\" then substr(municipality,1,3) else substr(municipality,1,2)]    \n" +
                "   [aggr nb := count(nb) group by year, nuts3, facility_type];");

        Dataset bpeNuts = (Dataset) engine.getContext().getAttribute("BPE_NUTS3");
        Structured.DataStructure bpeNutsStructure = bpeNuts.getDataStructure();

        assertThat(bpeNutsStructure.get("nuts3").getType()).isEqualTo(String.class);
        assertThat(bpeNutsStructure.get("nuts3").getRole()).isEqualTo(Dataset.Role.IDENTIFIER);

        assertThat(bpeNutsStructure.get("facility_type").getType()).isEqualTo(String.class);
        assertThat(bpeNutsStructure.get("facility_type").getRole()).isEqualTo(Dataset.Role.IDENTIFIER);

        assertThat(bpeNutsStructure.get("year").getType()).isEqualTo(String.class);
        assertThat(bpeNutsStructure.get("year").getRole()).isEqualTo(Dataset.Role.IDENTIFIER);


        assertThat(bpeNutsStructure.get("nb").getType()).isEqualTo(Long.class);
        assertThat(bpeNutsStructure.get("nb").getRole()).isEqualTo(Dataset.Role.MEASURE);

        // Step 5
        engine.eval("" +
                "define datapoint ruleset NUTS3_TYPES (variable facility_type, nb) is\n" +
                "    BOWLING_ALLEY_RULE : when facility_type = \"F102\" then nb > 10 errorcode \"Not enough bowling alleys\"\n" +
                "end datapoint ruleset;\n" +
                "\n" +
                "CHECK_NUTS3_TYPES := check_datapoint(BPE_NUTS3, NUTS3_TYPES invalid);");

        Dataset checkNutsTypes = (Dataset) engine.getContext().getAttribute("CHECK_NUTS3_TYPES");

        // size is 2 with full dataset instead of sample
        assertThat(checkNutsTypes.getDataPoints()).isEmpty();

        // Step 6
        Structured.DataStructure censusStructure = TrevasSDMXUtils.buildStructureFromSDMX3("src/test/resources/DSD_BPE_CENSUS.xml", "LEGAL_POP");

        SparkDataset censusNuts = new SparkDataset(
                spark.read()
                        .option("header", "true")
                        .option("delimiter", ";")
                        .option("quote", "\"")
                        .csv("src/test/resources/LEGAL_POP_NUTS3.csv"),
                censusStructure
        );

        context.setAttribute("CENSUS_NUTS3_2021", censusNuts, ScriptContext.ENGINE_SCOPE);

        engine.eval("CENSUS_NUTS3_2021 := CENSUS_NUTS3_2021   \n" +
                "   [rename REF_AREA to nuts3, TIME_PERIOD to year, POP_TOT to pop]\n" +
                "   [filter year = \"2021\"]\n" +
                "   [calc pop := cast(pop, integer)]" +
                "   [drop year, NB_COM, POP_MUNI];");

        Dataset censusNuts2021 = (Dataset) engine.getContext().getAttribute("CENSUS_NUTS3_2021");
        Structured.DataStructure censusNuts2021Structure = censusNuts2021.getDataStructure();

        assertThat(censusNuts2021Structure.get("nuts3").getType()).isEqualTo(String.class);
        assertThat(censusNuts2021Structure.get("nuts3").getRole()).isEqualTo(Dataset.Role.IDENTIFIER);

        assertThat(censusNuts2021Structure.get("pop").getType()).isEqualTo(Long.class);
        assertThat(censusNuts2021Structure.get("pop").getRole()).isEqualTo(Dataset.Role.MEASURE);

        // Step 7
        engine.eval("GENERAL_PRACT_NUTS3_2021 := BPE_NUTS3" +
                "   [filter facility_type = \"D201\" and year = \"2021\"]\n" +
                "   [drop facility_type, year];");

        Dataset generalNuts = (Dataset) engine.getContext().getAttribute("GENERAL_PRACT_NUTS3_2021");
        Structured.DataStructure generalNutsStructure = generalNuts.getDataStructure();

        assertThat(generalNutsStructure.get("nuts3").getType()).isEqualTo(String.class);
        assertThat(generalNutsStructure.get("nuts3").getRole()).isEqualTo(Dataset.Role.IDENTIFIER);

        assertThat(generalNutsStructure.get("nb").getType()).isEqualTo(Long.class);
        assertThat(generalNutsStructure.get("nb").getRole()).isEqualTo(Dataset.Role.MEASURE);

        // Step 8
        engine.eval("BPE_CENSUS_NUTS3_2021 <- inner_join(GENERAL_PRACT_NUTS3_2021, CENSUS_NUTS3_2021)\n" +
                "   [calc pract_per_10000_inhabitants := nb / pop * 10000]\n" +
                "   [drop nb, pop];");

        Dataset bpeCensus = (Dataset) engine.getContext().getAttribute("BPE_CENSUS_NUTS3_2021");
        Structured.DataStructure bpeCensusStructure = bpeCensus.getDataStructure();

        assertThat(bpeCensusStructure.get("nuts3").getType()).isEqualTo(String.class);
        assertThat(bpeCensusStructure.get("nuts3").getRole()).isEqualTo(Dataset.Role.IDENTIFIER);

        assertThat(bpeCensusStructure.get("pract_per_10000_inhabitants").getType()).isEqualTo(Double.class);
        assertThat(bpeCensusStructure.get("pract_per_10000_inhabitants").getRole()).isEqualTo(Dataset.Role.MEASURE);
    }

    @Test
    public void bpeV2() {
        Structured.DataStructure bpeStructure = TrevasSDMXUtils.buildStructureFromSDMX3("src/test/resources/DSD_BPE_CENSUS.xml", "BPE_DETAIL");

        SparkDataset bpeDetailDs = new SparkDataset(
                spark.read()
                        .option("header", "true")
                        .option("delimiter", ";")
                        .option("quote", "\"")
                        .csv("src/test/resources/BPE_DETAIL_SAMPLE.csv"),
                bpeStructure
        );

        Structured.DataStructure censusStructure = TrevasSDMXUtils.buildStructureFromSDMX3("src/test/resources/DSD_BPE_CENSUS.xml", "LEGAL_POP");

        SparkDataset censusNuts = new SparkDataset(
                spark.read()
                        .option("header", "true")
                        .option("delimiter", ";")
                        .option("quote", "\"")
                        .csv("src/test/resources/LEGAL_POP_NUTS3.csv"),
                censusStructure
        );
        Map<String, Dataset> inputs = Map.of("BPE_DETAIL", bpeDetailDs, "LEGAL_POP", censusNuts);
        ReadableDataLocation rdl = new ReadableDataLocationTmp("src/test/resources/DSD_BPE_CENSUS.xml");
        SDMXVTLWorkflow sdmxVtlWorkflow = new SDMXVTLWorkflow(engine, rdl, inputs);
        Map<String, PersistentDataset> bindings = sdmxVtlWorkflow.run();
        assertThat(bindings.size()).isEqualTo(3);
    }
}
