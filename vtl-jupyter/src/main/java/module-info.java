module fr.insee.vtl.kernel {
    requires java.logging;
    requires java.scripting;
    requires jupyter.jvm.basekernel;

    exports fr.insee.vtl.jupyter;
}