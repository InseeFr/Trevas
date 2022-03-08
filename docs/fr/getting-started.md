# Démarrage

Trevas est un projet multi-modules. Les modules suivants sont définis.

## Modules

### VTL-Engine

Moteur d'exécution et son implémentation Java basique.

### VTL-Model

Modèle définissant les objets utilisés dans le moteur.

### VTL-Parser

Parseur généré par [Antlr](https://www.antlr.org/) à partir de la [grammaire formelle de VTL 2.0](https://github.com/InseeFr/Trevas/tree/master/vtl-parser/src/main/antlr4/fr/insee/vtl/parser).

### VTL-Spark

Module permettant l'exécution de transformations VTL par [Spark](https://spark.apache.org/).

### VTL-Jackson

Module de sérialisation / désérialisation JSON de jeux de données.

### VTL-JDBC

Outils pour l'utilisation de sources de données SQL.
