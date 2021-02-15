# Démarrage

Trevas est un projet multi-modules.

## Modules

### VTL-Engine

Moteur d'exécution.

### VTL-Model

Modèle permettant de définir les objets utilisés au sein du moteur.

### VTL-Parser

Parser VTL auto-généré par [Antlr](https://www.antlr.org/) à partir de la [description formelle de VTL 2.0](https://github.com/InseeFr/Trevas/tree/master/vtl-parser/src/main/antlr4/fr/insee/vtl/parser).

### VTL-Spark

Module permettant la résolution de transformations VTL par [Spark](https://spark.apache.org/).

### VTL-Jackson

Module de sérialisation / déserialisation json de Dataset.
