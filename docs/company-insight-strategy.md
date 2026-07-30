# Company insight strategy

## Product goal

PortWatch exists to produce differentiated, decision-useful alternative-data insights for
Industrials equity research. The unit of research is a covered company, not a port or an HS code.
Port and trade observations remain valuable because they measure demand, sourcing, routing, and
operating conditions, but they are contextual evidence rather than proof of company activity.

The target output is:

```text
company x as_of_date x signal x evidence_vintage
```

Every output should answer: what changed, why it may matter to the company, what evidence supports
the claim, what could disprove it, and when the analyst could first have known it.

## What makes the insight differentiated

The public datasets are not unique by themselves. Differentiation comes from the research system:

- a reviewed entity graph connects subsidiaries, facilities, products, contracts, and exposures;
- company events are evaluated against their own historical baseline instead of a generic screen;
- independent company, facility, trade, and port sources corroborate or contradict one another;
- contribution analysis identifies which product, country, facility, or award drove the change;
- publication vintages recreate exactly what was knowable on a historical research date;
- every output is framed as a falsifiable thesis update with counterevidence.

## Evidence hierarchy

PortWatch assigns every input and claim to one of three layers.

| Layer | Examples | Permitted claim |
|---|---|---|
| Company-observed | SEC filing fact, named federal award, named facility inspection | The event or value is linked to the company or a reviewed subsidiary |
| Company-linked operational | Reviewed facility, customer, supplier, product, or geography mapping | The observation is relevant through a documented relationship |
| Contextual/inferred | Port/HS/country trade, TEU, dwell, vessel, or macro series | The environment is consistent or inconsistent with an exposure thesis |

A contextual signal cannot independently produce a company conclusion. A research-ready company
insight must contain at least one company-observed or company-linked operational input. Entity
matches must be reviewed and retain the source identifier, effective dates, and confidence.

## Company insight card contract

Each card will contain:

- ticker, legal entity, subsidiaries or facilities matched, and `as_of_date`;
- a concise falsifiable claim;
- current value, historical baseline, change, surprise, and direction;
- the mechanism linking the observation to revenue, cost, working capital, capacity, or risk;
- source records with publication and pipeline-availability timestamps;
- evidence class: observed, linked, or inferred;
- confidence, data freshness, limitations, and explicit counterevidence;
- related port/trade signals used as corroboration;
- analyst disposition: supports, contradicts, or does not change the thesis.

An LLM may summarize a completed card and retrieve its sources. It may not create entity links,
calculate the authoritative signal, or upgrade inferred evidence to observed evidence.

## Initial source plan

### 1. Company and entity foundation

The version 2 registry extends ticker-to-HS mappings into a dated entity graph containing:

- SEC CIK and legal registrant;
- subsidiaries and former names;
- federal-award recipient identifiers where relevant;
- reviewed facilities and operating locations;
- products, HS codes, countries, ports, customers, and suppliers disclosed by the company;
- citations, effective dates, confidence, and analyst review status for every relationship.

### 2. Company-specific collectors

Prioritize public sources whose natural grain names a company, subsidiary, or facility:

1. [SEC submissions and structured XBRL facts](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
   for reported fundamentals and filing events. This collector is implemented and resolves each
   response CIK through the reviewed entity graph before persistence.
2. [USAspending award transactions](https://api.usaspending.gov/) for Industrials companies with
   material government exposure.
3. [OSHA inspection/citation data](https://www.osha.gov/data) and
   [EPA ECHO facility events](https://echo.epa.gov/tools/web-services/facility-search-all-data)
   for operational and compliance changes.
4. Additional issuer-specific public data only when access is reproducible and its terms permit
   collection.

### 3. Contextual PortWatch collectors

Use Census port/HS/country flows, port TEU, dwell, vessel calls, and rail or blank-sailing data to:

- validate whether a company-linked demand signal is broad or isolated;
- distinguish value growth from physical-volume growth and product mix;
- measure sourcing concentration and disruption exposure;
- identify port substitution so routing changes are not mistaken for demand changes;
- provide counterevidence when operating data disagree with a company-specific signal.

The [BTS Port Performance Freight Statistics Program](https://www.bts.gov/ports) is a preferred
source for nationally consistent port throughput and operational comparisons.

## Pilot: Caterpillar

Caterpillar remains a useful first end-to-end pilot because the registry already documents its
HS 84 machinery and HS 86 railway-equipment exposures. The pilot should not claim that observed
cargo belongs to Caterpillar. Instead it should combine:

- company-observed SEC facts and disclosures;
- reviewed Caterpillar entities and facilities;
- company-linked facility or contract events when material;
- HS 84 and HS 86 value, weight, unit-value, country, and port signals;
- evidence for and against a demand, supply-chain, inventory, or operating-risk interpretation.

The pilot is successful when it can generate a reproducible monthly Caterpillar insight card that
an analyst can accept, reject, or annotate without reading application code.

## Acceptance criteria

A company signal is portfolio-grade only when:

1. its entity mapping and all source records are reproducible and cited;
2. at least one input is company-observed or company-linked;
3. calculations are deterministic and point-in-time safe;
4. contextual port data are labeled as corroboration, not shipment ownership;
5. the output includes counterevidence and a confidence level;
6. an analyst can trace the card back to raw payloads and revisions;
7. historical cards can be evaluated against later reported outcomes.
