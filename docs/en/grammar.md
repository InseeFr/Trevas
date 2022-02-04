# Use of the VTL grammar

## The VTL grammar

Trevas relies on the formal VTL grammar specified as [EBNF](https://en.wikipedia.org/wiki/Extended_Backus%E2%80%93Naur_form). The reference is the upgrade of version 2.0 published in July 2020 [on the SDMX web site](https://sdmx.org/wp-content/uploads/VTL-2.0-EBNF-Grammar-2020-07.zip).

The grammar consists in two files ready to be processed by the [Antlr](https://www.antlr.org/) parser generator:

- [VtlTokens.g4](https://github.com/InseeFr/Trevas/blob/master/vtl-parser/src/main/antlr4/fr/insee/vtl/parser/VtlTokens.g4) contains the list of valid VTL terms.

- [Vtl.g4](https://github.com/InseeFr/Trevas/blob/master/vtl-parser/src/main/antlr4/fr/insee/vtl/parser/Vtl.g4) contains the rules that create valid VTL expressions.

Antlr uses those files to produce a lexer, which creates a list of vocabulary symbols from an input stream of characters, and a parser, which creates the grammar structure corresponding to the list of symbols. Antlr can generate parsers usable in different target languages. Trevas uses the parser for Java, which is exposed in the [`vtl-parser`](https://github.com/InseeFr/Trevas/tree/master/vtl-parser) module.

## Grammar adaptations

In order to improve performances and fonctionalities, minor modifications have been made to the VTL grammar used in Trevas.

### Simplification of the grammar tree

As documented [here](https://github.com/VTL-Community/VTL-Community/issues/5) and [here](https://github.com/InseeFr/Trevas-JS/issues/40), the `expr` and `exprComp` branches in the grammar tree are almost identical. In order to avoid implementing the same logic twice, the `exprComp` branch was commented out in commit [498c1f8](https://github.com/InseeFr/Trevas/commit/498c1f8be39702fbcfc89a3144ac1842d7771d93). It was later found that this modification wrongly invalidated the `COUNT()` expression, so the corresponding rule was reintroduced in the grammar in commit [54f86f2] (https://github.com/InseeFr/Trevas/commit/54f86f27d2e8fdd57df1439d74ed56d225064a7d).


### Addition of distance operators

Distance operators like Levenshtein or Jaro-Winkler are commonly used in tests on strings. In order to allow them in VTL expressions, commit [036dc60](https://github.com/InseeFr/Trevas/commit/036dc6055240a38c19be7afd1d3067e370353f9f) added a `distanceOperators` section containing a `LEVENSHTEIN` rule in the grammar, as well as a `LEVENSHTEIN` symbol in the lexer file.
