package fr.insee.vtl.prov;

import fr.insee.vtl.prov.prov.Program;
import fr.insee.vtl.prov.utils.PropertiesLoader;
import fr.insee.vtl.prov.utils.RDFUtils;
import org.apache.jena.rdf.model.Model;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class RDFTest {

    static Properties conf;

    static {
        try {
            conf = PropertiesLoader.loadProperties();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static String sparqlEndpoint = conf.getProperty("sparql-endpoint-url") + conf.getProperty("trevas-provenance-repository-path");
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
        RDFUtils.loadModelWithCredentials(model, sparqlEndpoint, sparqlEndpointUser, sparlqEndpointPassword);
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
    public void simpleTest() throws IOException {


        String script = "ds_sum := ds1 + ds2;\n" +
                "ds_mul := ds_sum * 3; \n" +
                "ds_res <- ds_mul   [filter mod(var1, 2) = 0]" +
                "                   [calc var_sum := var1 + var2];";

        Program program = ProvenanceListener.run(script, "trevas-simple-test", "Simple test from Trevas tests");
        Model model = RDFUtils.buildModel(program);
        String content = RDFUtils.serialize(model, "JSON-LD");
        assertThat(content).isNotEmpty();
        RDFUtils.loadModelWithCredentials(model, sparqlEndpoint, sparqlEndpointUser, sparlqEndpointPassword);
        RDFUtils.writeJsonLdToFile(model, "src/test/resources/output/test-simple.json");
        assertThat(program.getProgramSteps()).hasSize(3);
    }

    @Test
    public void simpleTestWithBindings() throws IOException {


        String script = "ds1 := ds1 + ds2;\n" +
                "ds_mul := ds_sum * 3; \n" +
                "ds_res <- ds_mul   [filter mod(var1, 2) = 0]" +
                "                   [calc var_sum := var1 + var2];";

        Program program = ProvenanceListener.runWithBindings(script, "trevas-simple-test", "Simple test from Trevas tests");
        Model model = RDFUtils.buildModel(program);
        String content = RDFUtils.serialize(model, "JSON-LD");
        assertThat(content).isNotEmpty();
        RDFUtils.loadModelWithCredentials(model, sparqlEndpoint, sparqlEndpointUser, sparlqEndpointPassword);
        RDFUtils.writeJsonLdToFile(model, "src/test/resources/output/test-simple-with-bindings.json");
        assertThat(program.getProgramSteps()).hasSize(3);
    }

    @Test
    public void bpeTest() throws IOException {


        String bpeScript = "// Validation of municipality code in input file\n" +
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
                "                        [drop nb, pop];";

        Program program = ProvenanceListener.run(bpeScript, "trevas-bpe-test", "BPE from Trevas tests");
        Model model = RDFUtils.buildModel(program);
        RDFUtils.loadModelWithCredentials(model, sparqlEndpoint, sparqlEndpointUser, sparlqEndpointPassword);
        RDFUtils.writeJsonLdToFile(model, "src/test/resources/output/test-bpe.json");
        assertThat(program.getProgramSteps()).hasSize(8);
    }
}
