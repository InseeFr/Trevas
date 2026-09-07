# RDF / SDTH model — compatibility path and next improvements

> Companion to [`20260728_02`](./20260728_02_provenance-output-and-tests.md) §1.3
> and [`docs/model-v1.md`](../docs/model-v1.md).
>
> Goal: improve the Trevas RDF export so it better matches the SDTH vocabulary
> for data-entity lineage (not only program-step wiring), while keeping the IR
> (`ProvGraph`) as the single source of truth.

## Status

Phase A (compatibility) is done: IR → `SdthProgramView` → `RDFUtils`, public
entry `Provenance.run`. PR-38 added an experimental `RichRdfView` (IR→Model
directly) that is not yet on the production path.

Phase B (this document from §8) is the planned SDTH RDF improvement work.

## 1. Production branch (keep for consumers of step-shaped RDF)

```
VTL script
  → ProvenanceExtractor → ProvGraph
  → SdthProgramView.toProgram(graph, meta)
  → RDFUtils.buildModel(program)
  → Jena Model (JSON-LD / Turtle / …)
```

`RDFUtils` maps:

| Java (`fr.insee.vtl.prov.prov`) | RDF |
|---|---|
| `Program` | `sdth:Program` + `rdfs:label` + `sdth:hasProgramStep` (no `hasSourceCode` on Program) |
| `ProgramStep` | `sdth:ProgramStep` + label `"Step {index}"` + `hasSourceCode` |
| step → produced DF | `sdth:producesDataframe` |
| step → consumed DFs | `sdth:consumesDataframe` |
| step → used vars | `sdth:usesVariable` |
| step → assigned vars | `sdth:assignsVariable` |
| `DataframeInstance` | `sdth:DataframeInstance` + `rdfs:label` + `sdth:hasName` + `sdth:hasVarInstance` |
| `VariableInstance` | `sdth:VariableInstance` + label + `hasName`; optional Trevas `hasRole` / `hasType` |
| Entity lineage | `sdth:wasDerivedFrom` / `sdth:elaborationOf` on DF and variables; roots → `FileInstance` |

URI pattern: `http://trevas/{program\|program-step\|dataset\|variable\|file}/{id}` with
`@` encoded as `__` in the URI local name.

Namespace emitted today: `http://rdf-vocabulary.ddialliance.org/sdth#`.

## 2. How the compatibility adapter was built

Do not rewrite `RDFUtils` first. Project the IR onto the existing `Program`
tree, then reuse the serializer:

```
ProvGraph → SdthProgramView.toProgram → RDFUtils.buildModel
```

Optional parallel path (landed, not wired to `Provenance.run`):

```
ProvGraph → RichRdfView.buildModel
```

`RichRdfView` emits IR kinds (dataset / variable / expression / scalar) and maps
value `dependsOn` → `sdth:wasDerivedFrom`, condition `dependsOn` → `prov:used`.
Tests: `RichRdfViewTest`. It does not emit program steps, `hasName`,
`elaborationOf`, or `FileInstance`.

## 3. What `SdthProgramView` rebuilds

The IR is finer (expression nodes, anonymous intermediates, `role=condition`).
Legacy RDF is coarser (statement steps + dataset/variable instances). The view
rolls up:

| Legacy field | How to derive from ProvGraph |
|---|---|
| `Program.id` / `label` / `sourceCode` | Run metadata (caller-supplied) |
| One `ProgramStep` per top-level assignment | Assignment boundaries (`:=` / `<-`); index = statement order |
| `step.sourceCode` | Statement source fragment |
| `producedDataframe` | Named LHS dataset (not anonymous `#s1.1`) |
| `consumedDataframes` | Dataset roots of value `dependsOn` for that step |
| `assignedVariables` | Variables created/overwritten on the produced DF |
| `usedVariables` | Variables referenced by value or condition deps |
| `DataframeInstance.hasVariableInstances` | Variables with `dataset=<that df id>` |
| `VariableInstance.role` / `type` | Structure oracle |
| Instance ids | IR deterministic ids |

Dropped from the step view: expression nodes, anonymous intermediates, edge
`op`/`role` as first-class RDF (they stay in IR / DOT).

Condition edges: fold into `sdth:usesVariable` on the step (no extra predicate).

## 4. Acceptance — compatibility path

1. Existing `RDFTest` / BPE path stays green.
2. Isomorphic step-shaped triples for the same scripts (types, predicates,
   labels, structural links), allowing id-policy differences.
3. Entity-lineage predicates are out of scope for the compatibility acceptance
   (covered by Phase B below).

## 5. Why this wiring

- Unblocked IR + DOT without breaking step-shaped RDF consumers.
- Keeps `RDFUtils` as one SDTH spelling site for the Program tree.
- Lets Phase B deepen the model without another hand-written extractor.

## 6. Resolved open points (Phase A)

- Multi-clause statements (`filter`+`calc`): one assignment = one step;
  anonymous intermediates folded.
- Rulesets: populated on `ProgramStep` from IR annotations; still not
  serialized by `RDFUtils`.
- Id policy: IR ids (`ds2@1`, `ds2@1.var1`, `step-ds2@1`).

## 7. Landed (Phase A)

- `SdthProgramView` + `SdthProgramViewTest`
- `Provenance.run` (PR-16) + BPE RDF (PR-37)
- `RichRdfView` + `RichRdfViewTest` (PR-38, experimental)

## 7b. Landed (Phase B / Wave F)

Production path (`SdthProgramView` → `RDFUtils` → `Provenance.run`) now emits:

- `sdth:hasName` on dataframe / variable / file instances
- `sdth:hasVarInstance` (replaces `hasVariableInstance`)
- `sdth:hasSourceCode` only on `ProgramStep`
- dataframe `wasDerivedFrom` / `elaborationOf` (identity → elaboration)
- root `FileInstance` + dataframe `wasDerivedFrom` file
- variable `wasDerivedFrom` / `elaborationOf`
- JSON-LD-safe URI ids (`@` → `__`)

Tests: `SdthEntityLineageTest`.

## 8. Phase B — improve the SDTH RDF model

We want the export to support SDTH-style **entity lineage**: follow a
`DataframeInstance` or `VariableInstance` backward through the program, not only
via `ProgramStep` consumes/produces. That means emitting the SDTH predicates
meant for data entities, aligning property names with the vocabulary, and
keeping step wiring as the complementary process view.

Two complementary graphs in one model:

- Process: `Program` / `ProgramStep` + consumes / produces / uses / assigns
  (already there).
- Entity: `wasDerivedFrom` / `elaborationOf` (+ `FileInstance` for external
  loads) so queries can walk entities without re-deriving lineage from steps.

Implementation preference: extend the production serializer path
(`SdthProgramView` fields + `RDFUtils`, and/or wire a merged view from IR)
rather than leave lineage only in disconnected `RichRdfView`. DOT goldens stay
the IR contract; RDF tests assert the SDTH shape separately.

### 8.1 Vocabulary and metadata (cheap, unblocks SHACL-shaped checks)

| Step | Change | Notes |
|------|--------|--------|
| B1 | Emit `sdth:hasName` on dataframe / variable / file instances | Mirror the VTL binding or component name; keep `rdfs:label` |
| B2 | Prefer `sdth:hasVarInstance` over `hasVariableInstance` | Align with SDTH spelling used by shapes/examples |
| B3 | Restrict `sdth:hasSourceCode` to `ProgramStep` | Drop it from `Program` and `VariableInstance` in the export |
| B4 | Emit `sdth:FileInstance` for external / catalogue roots | Link loaded dataframes with entity lineage (see B5) |

### 8.2 Entity lineage predicates

| Step | Change | Mapping from IR / run |
|------|--------|------------------------|
| B5 | `sdth:wasDerivedFrom` on produced dataframes | Each produced DF → consumed DF(s) of that assignment (from step roll-up or value `dependsOn` between datasets) |
| B6 | `sdth:wasDerivedFrom` from root dataframes to `FileInstance` | Bindings that are inputs (no producer step), when a file/catalogue source is known or synthesized |
| B7 | `sdth:wasDerivedFrom` on variables | Value lineage: assigned / derived variable → source variable(s) (IR variable/expression `dependsOn`, rolled to variables) |
| B8 | `sdth:elaborationOf` where appropriate | Same logical entity, new version or identity-style pass-through (e.g. pure rename of a dataset binding, version bump without value rewrite). Do not use it as a synonym for every transform — transforms stay `wasDerivedFrom` |

`elaborationOf` vs `wasDerivedFrom` (working rule, refine with fixtures):

- Transform / combine / filter / calc / join → `wasDerivedFrom`
- Identity, re-bind, or “same entity elaborated” without changing value meaning → `elaborationOf`
- External load → dataframe `wasDerivedFrom` file (not elaboration)

### 8.3 Namespace and instance ids

| Step | Change | Notes |
|------|--------|--------|
| B9 | Document and stick to one SDTH base URI in Trevas | Production today: `http://rdf-vocabulary.ddialliance.org/sdth#`. Validation fixtures that still use `http://DDI/SDTH/` need an explicit remap layer, not silent dual emission |
| B10 | Avoid `@` inside IRI local names when serializing JSON-LD | Versioned IR ids like `ds@1` break some JSON-LD tooling; encode as `ds__1` / `ds/v/1` in RDF URIs while keeping IR ids unchanged in DOT |

### 8.4 Suggested delivery order

1. B1–B3 (names, `hasVarInstance`, `hasSourceCode` placement) + tests on `RDFUtils` / compact JSON-LD.
2. B5 then B6 (dataframe lineage + files).
3. B7–B8 (variable lineage + elaboration).
4. B9–B10 (NS policy + JSON-LD-safe URIs) as soon as Desktop validation is in the loop.

Wire into `Provenance.run` once the step-shaped model remains green and entity
triples are covered by unit tests (extend `SdthProgramViewTest` / `RDFTest` or
add a focused SDTH lineage suite). Keep `RichRdfView` either merged into that
path or clearly marked experimental until then.

### 8.5 Acceptance — Phase B

- Every named dataframe that is an input or a step output has a clear entity
  lineage story: `wasDerivedFrom` and/or `elaborationOf` (and file link for
  external roots).
- Variable-level lineage is queryable for calc/rename-style assignments (not
  only step `uses`/`assigns`).
- `hasName` present on dataframe and variable instances; dataframe→variable
  links use `hasVarInstance`.
- `hasSourceCode` only on program steps in the export.
- Existing step predicates remain; BPE / `RDFTest` stay green.
- Optional: validate a remapped copy of a Desktop export against the project
  SDTH shapes fixture (see local provenance validation notes under `docs/`)
  without treating “empty report” as success when namespaces do not match.

## 9. Non-goals (for now)

- Replacing DOT goldens with RDF goldens.
- Emitting full PROV Activities for every clause (unless a later view needs it).
- Requiring byte-identical JSON-LD with historical UUID churn.
- Changing the IR `dependsOn` model — RDF is a view over it.
