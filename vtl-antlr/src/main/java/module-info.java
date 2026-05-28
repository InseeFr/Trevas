/**
 * Vendored copy of the ANTLR 4 runtime, relocated from {@code org.antlr.v4.*} into {@code
 * fr.insee.vtl.antlr.*} so this module does not collide with the ANTLR runtime that Apache Spark
 * bundles. Source files under {@code fr/insee/vtl/antlr/} were extracted from {@code
 * org.antlr:antlr4-runtime:4.9.3-sources} and the package was rewritten in-tree — per-file ANTLR
 * Project BSD-3-Clause copyright headers are preserved verbatim.
 *
 * <p>To upgrade: bump {@code antlr4.version} in {@code pom.xml}, then run {@code ./mvnw -pl
 * vtl-antlr -Pvendor-antlr generate-sources} and commit the diff under {@code src/main/java/fr/}.
 * The vendored source is committed deliberately so the IDE sees this module as a plain Java project
 * with no special build-time plumbing.
 */
module fr.insee.vtl.antlr {
  exports fr.insee.vtl.antlr.runtime;
  exports fr.insee.vtl.antlr.runtime.atn;
  exports fr.insee.vtl.antlr.runtime.dfa;
  exports fr.insee.vtl.antlr.runtime.misc;
  exports fr.insee.vtl.antlr.runtime.tree;
  exports fr.insee.vtl.antlr.runtime.tree.pattern;
  exports fr.insee.vtl.antlr.runtime.tree.xpath;
}
