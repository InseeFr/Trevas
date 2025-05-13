# Trevas

Transformation engine and validator for statistics.

[![Build Status](https://github.com/InseeFr/Trevas/actions/workflows/ci.yml/badge.svg)](https://github.com/InseeFr/Trevas/actions/workflows/ci.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=InseeFr_Trevas&metric=alert_status)](https://sonarcloud.io/dashboard?id=InseeFr_Trevas)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=InseeFr_Trevas&metric=coverage)](https://sonarcloud.io/dashboard?id=InseeFr_Trevas)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/fr.insee.trevas/trevas-parent/badge.svg)](https://maven-badges.herokuapp.com/maven-central/fr.insee.trevas/trevas-parent)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Mentioned in Awesome Official Statistics ](https://awesome.re/mentioned-badge.svg)](http://www.awesomeofficialstatistics.org)

Trevas is a Java engine for the Validation and Transformation Language (VTL), an [SDMX standard](https://sdmx.org/?page_id=5096) that allows the formal definition of algorithms to validate statistical data and calculate derived data. VTL is user oriented and provides a technology-neutral and standard view of statistical processes at the business level. Trevas supports the latest VTL version (v2.1, July 2024).

For actual execution, VTL expressions need to be translated to the target runtime environment. Trevas provides this step for the Java platform, by using the VTL formal grammar and the [Antlr](https://www.antlr.org/) tool. For a given execution, Trevas receives the VTL expression and the data bindings that associate variable names in the expression to actual data sets. The execution results can then be retrieved from the bindings for further treatments.

Trevas provides an abstract definition of a Java VTL engine, as well as two concrete implementations:

- an in-memory engine for relatively small data, for example at design time when developing and testing VTL expressions on data samples
- an [Apache Spark](https://spark.apache.org/) engine for Big Data production environments

Other implementations can be easily developed for different contexts.

## Documentation

The documentation can be found in the [docs](https://github.com/InseeFr/Trevas/tree/master/docs) folder and [browsed online](https://inseefr.github.io/Trevas).

If you want to contribute, see this [guide](docs/CONTRIBUTING.md).

## Requirements

Open JDK 17 + is required.

## References

<p align="center">
  <img width="100px" src="./docs/static/img/sdmx-logo.svg" />
</p>

Trevas is listed among the [SDMX](https://sdmx.org/?page_id=4500) tools.

<p align="center">
  <img width="100px" src="./docs/static/img/sdmx-io-logo.svg" />
</p>

Trevas is part of the [sdmx.io](https://www.sdmx.io/) ecosystem.

<p align="center">
    <img  src="https://awesome.re/mentioned-badge.svg" />
</p>

Trevas is referenced by [_Awesome official statistics software_](https://github.com/SNStatComp/awesome-official-statistics-software)
