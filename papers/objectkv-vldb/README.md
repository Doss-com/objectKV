# objectKV PVLDB working paper

Status: `[EVALUATING]` working manuscript. This is not a submission-ready claim.

The paper uses the official VLDB 2027 and PVLDB Vol. 20 ACM-based template,
pinned at:

```text
https://github.com/vldbproceedings/VLDB-Template
39c95f5c6fcbe652a83be24e4eff8f2134cd3fbc
```

The repository does not vendor the template. `main.tex`, `references.bib`, and
the figure sources live here; the generated `objectkv-vldb.pdf` is the review
artifact. To reproduce the build, place the paper files into the pinned
template root or make `acmart.cls`, `pvldb.sty`, and
`ACM-Reference-Format.bst` available to Tectonic, then run:

```bash
rsvg-convert -f pdf -o figures/construction.pdf figures/construction.svg
rsvg-convert -f pdf -o figures/cell-services.pdf figures/cell-services.svg
rsvg-convert -f pdf -o figures/data-paths.pdf figures/data-paths.svg
rsvg-convert -f pdf -o figures/txlog-path.pdf figures/txlog-path.svg
rsvg-convert -f pdf -o figures/c5v2-layout.pdf figures/c5v2-layout.svg
rsvg-convert -f pdf -o figures/performance-balance.pdf figures/performance-balance.svg
rsvg-convert -f pdf -o figures/proof-ladder.pdf figures/proof-ladder.svg
tectonic main.tex
```

The proof status vocabulary is defined in
`../../docs/STATUS-TAXONOMY.md`. `[VERIFIED]` claims in the paper are limited to
the named suite, backend, topology, and receipt. The generated paper is rebuilt
after material architecture, evidence, or program-frontier changes.
