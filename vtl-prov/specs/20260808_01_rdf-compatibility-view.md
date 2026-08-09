# RDF compatibility view — new IR → same triples as today

> Companion to [`20260728_02`](./20260728_02_provenance-output-and-tests.md) §1.3 / build order step 6.  
> Constraint for this round: **emit the same RDF model as `RDFUtils.buildModel(Program)` today** (SDTH shape consumers already use). Richer IR→RDF (variable `wasDerivedFrom`, expression nodes, …) is a later view.

## 1. Current branch (keep it)

```
VTL script
  → ProvenanceListener → Program (+ ProgramStep, DataframeInstance, VariableInstance)
  → RDFUtils.buildModel(program)
  → Jena Model (JSON-LD / Turtle / …)
```

`RDFUtils` is the **only** RDF serializer in production path today. It maps:

| Java (`fr.insee.vtl.prov.prov`) | RDF |
|---|---|
| `Program` | `sdth:Program` + `rdfs:label` + `sdth:hasSourceCode` + `sdth:hasProgramStep` |
| `ProgramStep` | `sdth:ProgramStep` + label `"Step {index}"` + `hasSourceCode` |
| step → produced DF | `sdth:producesDataframe` |
| step → consumed DFs | `sdth:consumesDataframe` |
| step → used vars | `sdth:usesVariable` |
| step → assigned vars | `sdth:assignsVariable` |
| `DataframeInstance` | `sdth:DataframeInstance` + `rdfs:label` + `sdth:hasVariableInstance` |
| `VariableInstance` | `sdth:VariableInstance` + label; optional `hasRole` / `hasType` (Trevas URIs); optional `hasSourceCode` |

URI pattern: `http://trevas/{program\|program-step\|dataset\|variable}/{id}`.  
**Not emitted today** (even if sketched in `docs/model-v1.md`): `sdth:wasDerivedFrom`, PROV activity edges, etc. Compatibility = **this** triple set, not the aspirational TTL in model-v1.

## 2. Target branch (new IR, same RDF)

Do **not** rewrite `RDFUtils` first. Insert a **projection** from the new graph IR onto the existing `Program` tree, then reuse the serializer:

```
VTL script
  → extractor → ProvGraph (Node / Edge / dependsOn)     // new source of truth
  → SdthProgramView.toProgram(graph, meta)              // NEW — compatibility adapter
  → RDFUtils.buildModel(program)                        // unchanged
  → same Jena Model as before
```

Optional later:

```
ProvGraph → RdfView (richer) → Model   // only after golden RDF tests exist for the new shape
```

## 3. What `SdthProgramView` must rebuild

The IR is finer (expression nodes, anonymous intermediates, `role=condition`). Legacy RDF is coarser (statement steps + dataset/variable instances). The view **rolls up**:

| Legacy field | How to derive from ProvGraph |
|---|---|
| `Program.id` / `label` / `sourceCode` | Run metadata (same as today: caller-supplied), not from graph nodes |
| One `ProgramStep` per top-level assignment | Group edges by statement / `op` on assignment boundaries (`:=` / `<-`); index = statement order |
| `step.sourceCode` | Statement source fragment (already needed for DOT `src` / statement grouping) |
| `producedDataframe` | Dataset node that is the assignment LHS (named binding), not anonymous `#s1.1` |
| `consumedDataframes` | Dataset nodes that are **value** `dependsOn` roots of that step (drop pure expression nodes; fold conditions into vars — see below) |
| `assignedVariables` | Variable nodes created/overwritten on the produced DF in that step (`calc` targets, rename targets, …) |
| `usedVariables` | Variable nodes referenced by value **or** condition dependencies inside the step (today’s listener puts filter/calc refs here) |
| `DataframeInstance.hasVariableInstances` | Variable nodes with `dataset=<that df id>` |
| `VariableInstance.role` / `type` / `sourceCode` | Node properties from the structure oracle (same enrichment as today) |
| Instance `id`s | Prefer IR deterministic ids (stable URIs). If legacy UUIDs must be preserved for byte-identical JSON-LD, keep a compatibility id map — default goal is **isomorphic triples** (same types/predicates/labels), not byte-identical blank UUID churn |

**Drop from the RDF view (for now):** expression nodes, anonymous intermediate datasets, edge `op`/`role` annotations as first-class RDF. They stay in the IR / DOT goldens only.

**Condition edges:** do not invent new RDF predicates. Fold referenced variables into `sdth:usesVariable` on the step (matches current coarse behaviour).

## 4. Acceptance — “same model as before”

1. Keep existing `RDFTest` (or freeze JSON-LD fixtures from current `ProvenanceListener` + `RDFUtils`).
2. For the same scripts + inputs: `buildModel(SdthProgramView.toProgram(newGraph))` and `buildModel(oldProgram)` must be **graph-isomorphic** after normalizing resource ids if needed (compare by `rdf:type` + `rdfs:label` + structural links, or pin ids in the view).
3. Do **not** require emitting `model-v1` `wasDerivedFrom` until an explicit “RDF v2” view is specified.

## 5. Why this wiring

- Unblocks IR + DOT work without breaking RDF consumers.
- Leaves `RDFUtils` as the single place that knows SDTH URI/predicate spelling.
- Makes “same triples” a testable adapter, not a second hand-written RDF builder.
- When we want richer RDF later, add a second view; don’t overload the compatibility path.

## 6. Open points (short)

- Exact rule for multi-clause statements (`filter`+`calc`) → one step vs several (today: one assignment = one step).
- Whether rulesets stay as `ProgramStep.rulesets` only (not in `RDFUtils` today) or start appearing in RDF.
- Id policy: deterministic IR ids in `http://trevas/...` vs legacy random UUIDs.
