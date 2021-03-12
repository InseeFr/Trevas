# Introduction

Trevas enable statistical transformation and validation, thanks to [VTL 2.0](https://sdmx.org/?page_id=5096) language.

## Description technique

[Trevas](https://github.com/InseeFr/Trevas) provide in particular a Java 11 engine.

Engine is based on JSR 223, respecting the Java Scripting API.

## Modules

### VTL-Engine

Engine.

### VTL-Model

Model defining objects used by the engine.

### VTL-Parser

Auto-generated VTL parser, built by [Antlr](https://www.antlr.org/) thanks to [formal description of VTL 2.0](https://github.com/InseeFr/Trevas/tree/master/vtl-parser/src/main/antlr4/fr/insee/vtl/parser).

### VTL-Spark

Module provided to enable [Spark](https://spark.apache.org/) resolution of VTL treatments.

### VTL-Jackson

Serialisation / deserialisation module from / to json for Dataset.
