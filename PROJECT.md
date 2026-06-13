# Technical Challenge — Founding Engineer
## The Council of Ministers
*Madrid, Moncloa — the problem nobody can see.*

---

## The Problem

Every Tuesday the **Council of Ministers** meets. This year they made the country a promise: a simpler, cleaner statute book — *fewer laws, clearer ones*. A real legislative-simplification mandate, announced, with political capital on the line.

There is one problem. Nobody can actually *see* the statute book.

Spanish law is a corpus of consolidated norms published across two centuries, each one amending and citing the others in a tangle no human has ever held in their head. When a minister asks "which laws should we simplify first?", the honest answer today is a shrug and a binder. The Council governs the regulatory state half-blind.

> **Your job:** turn the Boletín Oficial del Estado into a knowledge graph, and make it answer the four questions the Council of Ministers will actually ask in that room.

---

## Your Corpus

Ingest the **complete BOE consolidated-legislation corpus** through the public API (`boe.es/datosabiertos`). The catalogue is large, but the API is paginated and uniform.

---

## A Minimal Guide

- The `analisis` block of each norm exposes the relationships you need — *amends*, *amended by*, *repeals*, *repealed by*, *cites*. It is the raw material for the four briefings.
- The in-force status (`estatus_derogacion`) tells you whether a norm is live or repealed. Read it carefully: there are nuances (total repeal, partial repeal, in force with amendments).
- Stack is your choice — database, framework and visualization library are yours to pick. What we value is judgment: what you prioritise, what you decide not to build, and why.

---

## The Four Briefings the Council Needs

### 01 — Diagnosis: which laws have become unreadable?

The Vice-Presidency wants the consolidation backlog: the laws amended so many times they are now incomprehensible even to lawyers. Give the **top 5**. These are the first candidates for a clean rewrite.

*(The 5 norms most amended by other norms.)*

---

### 02 — Root cause: who made the mess?

Show the Council *how* laws become unreadable. Find the "omnibus" laws — single acts that silently rewrote dozens of unrelated statutes at once. Name the **top 5 worst offenders**. The Council wants to see the pattern, not just the symptom.

*(The 5 norms that amend the most other norms.)*

---

### 03 — The rot: how much of the statute book rests on dead law?

Find every law still in force that cites a law already repealed. Quantify it: what fraction of live Spanish law rests on legal ground that no longer exists? Then surface the **top 5 most-cited ghosts** — the dead laws still propping up the most live statutes.

*(The percentage of in-force norms that cite at least one repealed norm, and the 5 repealed norms most cited by in-force norms.)*

---

### 04 — The scalpel: the unfinished repeal.

In 2015 the Council repealed **Ley 30/1992, the act on the Legal Regime of Public Administrations and Common Administrative Procedure**, and replaced it with Leyes 39/2015 and 40/2015. But the cleanup was never finished: dozens of laws still in force keep citing Ley 30/1992 as if it existed. The Council wants to close the operation — update those orphan references — and needs the worklist. Compute the **blast radius** of Ley 30/1992: the laws still in force that cite it directly.

*(The list of in-force norms that cite Ley 30/1992 directly.)*

---

## Deliverables · One Week

1. **Code** in a public repository. We expect Claude Code or similar in the loop.
2. **A web platform** with the **interactive graph** — navigable, with zoom, filters and search — and the **results of the four briefings** integrated into it. A minister should be able to open it and understand each answer without reading a single query.
3. **A 1-page design doc:** your schema, and why.
4. **A 5-minute video** showing the results and the platform. Remember: it is addressed to the Council of Ministers.

---

## Bonus — If You Have Time Left

Surprise us. Something we didn't ask for but that proves the problem, the tool, or your judgment.

---

*Make the Council see its own statute book for the first time.*

---

*Santiago, Íñigo and Tomás — Reversa AI · info@reversa.ai*
