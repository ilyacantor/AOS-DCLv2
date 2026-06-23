

**ContextOS**

CONTEXT INFRASTRUCTURE FOR THE AGENTIC ENTERPRISE

A Structured Debate in Four Rounds \+ Consensus Deliverables

*Product or Service? Build or Wait? Ship or Study?*

Prepared for AOS Executive Team  •  March 2026

**THE PANEL**

| Persona | Role | Perspective |
| :---- | :---- | :---- |
| Maya Chen | AOS CTO. Former VP Eng, data integration. AI systems architect. | Has to ship it. |
| Robert Okafor | Enterprise CISO, Fortune 500 manufacturing. 18 yrs enterprise security. | Has to approve it. |
| James Worthington | Enterprise Data Architect, Global 500 financial services. 15 yrs MDM & governance. | Has to live with it daily. |
| Sarah Delacroix | CDO, Fortune 500 healthcare/insurance. Built data governance programs at three companies. Owns Collibra, Alation deployments. | Has to champion or kill it internally. |
| Dr. Priya Subramaniam | Research scientist, knowledge representation & ontology. DARPA, healthcare, financial services consultant. Neutral moderator. | Holds the panel to intellectual honesty and prevents drift. |
| Marcus Webb (Round 3 guest) | SaaS pricing strategist. Former VP Pricing at two enterprise SaaS unicorns. Advisor to PE portfolio companies on packaging. | Knows what buyers will pay and how to not leave money on the table. |

# **ROUND 0 — Agree on Terms, Lock Them Down**

*Dr. Subramaniam requires the panel to align on working definitions with concrete examples before any product discussion begins. No one leaves until all six terms mean the same thing to everyone.*

**Dr. Priya Subramaniam (Moderator)**

Two words — ontology and semantics — get used interchangeably in enterprise software marketing and it creates confusion that derails product decisions. Before we debate whether ContextOS should exist, I need every person in this room using these words the same way. I'll propose definitions. I want challenges, extensions, or acceptance. Then I want concrete examples so we have anchors against drift.

**Proposed Definition: Semantics**

**Semantics** is the discipline of assigning and preserving meaning. In enterprise context, semantics answers: what does this data element mean to this business, in this context, for this user? It operates at the field level, the entity level, and the relationship level. Semantics does not require formal logic or machine reasoning. It requires accurate, contextualized, traceable meaning assignment with confidence and provenance.

**Proposed Definition: Ontology**

**Ontology** is a structured, explicit specification of a domain's concepts, their properties, and the relationships between them — including rules that constrain how those concepts relate. A semantic layer maps field A to meaning B. An ontology additionally says: meaning B is a subtype of C, must co-occur with D, and cannot exist without attribute E. Ontology encodes structure and constraint, not just translation.

**The Spectrum**

| Level | What It Is | Machine Reasoning? |
| :---- | :---- | :---- |
| 1\. Glossary | Terms with definitions | No |
| 2\. Taxonomy | Terms in hierarchies | Barely |
| 3\. Semantic Layer | Source-to-meaning mappings with context, confidence, provenance | Partial — resolves queries |
| 4\. Knowledge Graph | Entities \+ typed relationships \+ properties, queryable | Yes — traversal, pattern matching |
| 5\. Formal Ontology | Axiomatized model with inference rules and logical constraints | Yes — full inference, entailment |

## **Concrete Examples: Semantics vs. Ontology**

**Dr. Priya Subramaniam (Moderator)**

Let me give three examples of each. These are the reference anchors. If at any point in this debate someone uses the word incorrectly, I'll redirect to these examples.

**Three Examples of Semantics in Action**

| EXAMPLE: Semantics Example 1: Field Resolution SAP stores a field called KUNNR. Salesforce stores a field called AccountId. Workday stores a field called Worker\_ID. A semantic layer maps all three to the business concept 'Account' with confidence scores (KUNNR → Account at 95%, Worker\_ID → Account at 72% because it could also mean Employee). The semantic layer tells you what the field means. It does not tell you the rules governing accounts or how accounts relate to contracts. |
| :---- |

| EXAMPLE: Semantics Example 2: Persona-Aware Meaning The CFO asks 'how many customers do we have?' and gets 2,400 (paying accounts with signed contracts). The CRO asks the same question and gets 8,100 (all accounts in the pipeline including prospects). Both are correct. The semantic layer knows which definition applies to which persona and returns the contextually correct answer without the user specifying. This is semantics — meaning varies by context. |
| :---- |

| EXAMPLE: Semantics Example 3: Provenance and Confidence An agent queries 'Q3 revenue' and the semantic layer returns $14.2M with metadata: source is NetSuite GL, confidence 0.94, last refreshed 2 hours ago, recognized under ASC 606\. The agent now has a grounded, traceable answer instead of hallucinating a number. This is semantics providing meaning with provenance. |
| :---- |

**Three Examples of Ontology in Action**

| EXAMPLE: Ontology Example 1: Typed Relationships and Constraints The knowledge graph encodes: Account HAS Contracts. Contract GENERATES Revenue. Revenue RECOGNIZED\_IN Period. And the constraint: if Contract.type \= Subscription, then Revenue.recognition\_method \= Ratably. This is ontology — not just what things mean, but how they relate and what rules govern those relationships. A semantic layer tells you 'this field means revenue.' An ontology tells you revenue from subscriptions must be recognized ratably and will flag a violation if it's not. |
| :---- |

| EXAMPLE: Ontology Example 2: Entity Resolution Across Systems 'Acme Corp' in Salesforce, 'ACME CORPORATION' in SAP, and 'Acme (US) LLC' in the billing system are all the same entity. The ontology resolves them into a single golden record with a canonical identity, maps the variant names as aliases, and tracks which system record is authoritative for which attributes (Salesforce for contact info, SAP for financial data, billing for subscription status). This is ontology — structural knowledge about identity and authority, not just field mapping. |
| :---- |

| EXAMPLE: Ontology Example 3: Conflict Detection and Reconciliation Logic SAP says customer X has a $2M outstanding balance. Salesforce says $1.8M. The ontology detects this conflict, traces the root cause (SAP includes a disputed invoice that Salesforce excludes), and presents the reconciliation options to a human: accept SAP as authoritative, accept Salesforce, or escalate for manual review. The ontology doesn't just surface the discrepancy — it encodes the rules for how discrepancies should be resolved based on the dimension, the systems involved, and the organizational authority map. |
| :---- |

**Sarah Delacroix (CDO)**

I want to react to these examples because they directly map to tools I already own and gaps I already know about.

Our Collibra deployment handles Example 1 — field resolution and cataloging. It does a reasonable job mapping field names to business terms. What it cannot do is Examples 2 and 3\. It has no concept of persona-aware meaning. When the CFO and CRO ask the same question and get different answers, Collibra doesn't resolve that — my team gets a ticket and spends two days figuring out which definition was used. And it absolutely cannot do the Ontology examples. Entity resolution across systems? We have a $4M Informatica MDM project that's been running for two years and covers 60% of our customer entities. Conflict detection with reconciliation logic? That's a spreadsheet my data stewards maintain manually.

So I accept the definitions, and I'll add my qualifier: the gap between what data governance tools deliver today — which is roughly Level 2 to 3 on the spectrum — and what enterprises actually need — which is Level 4 — is where the market opportunity lives. If ContextOS can bridge that gap without requiring me to rip out Collibra, I'm interested. If it requires replacing what I have, I'm not.

**James Worthington (Enterprise Data Architect)**

Sarah's qualifier is critical. The data governance market has spent billions getting enterprises to Level 2-3. Nobody is going to throw that away. ContextOS has to sit on top of existing investments, not replace them. The pitch has to be: your data catalog tells you what fields exist. ContextOS tells you what they mean, how they relate, and where they disagree.

I also want to restate my position from the last round: for enterprise buyers, the value inflection point is not the formalism level. It's the ability to detect and help resolve contradictions between source systems. Your Ontology Example 3 — SAP says $2M, Salesforce says $1.8M, here's why, here's who decides — that's the product. Everything else is infrastructure to enable that moment.

**Maya Chen (CTO)**

I accept the definitions and the spectrum. For clarity on where we are and where we're building: DCL today delivers Level 3 — semantic mapping with 107 business concepts across 9 domains, persona-aware views, confidence scoring, and provenance. That's real. It ships. It works.

What we're proposing to build is Level 4 — entity resolution, typed relationships, conflict detection, reconciliation logic. That's the knowledge graph. We should not claim we deliver ontology today. We deliver sophisticated semantics today, with a knowledge graph as the build target. Honest positioning.

**Robert Okafor (CISO)**

Each example carries a different security profile. Semantics Example 1 — field mappings — is low-sensitivity metadata. Ontology Example 2 — entity resolution creating golden records — is medium sensitivity, because now you have a master record that aggregates identity across systems. Ontology Example 3 — conflict detection with financial data — is high sensitivity, potentially material nonpublic information for a public company. The product tiers have to match the security tiers.

| LOCKED: Semantics Assigning and preserving contextualized meaning across enterprise data sources. Field-level, entity-level, relationship-level. Requires accuracy, context-awareness, confidence, provenance. Does not require formal logic. AOS delivers this today via DCL (Level 3). Reference examples: field resolution, persona-aware meaning, provenance and confidence. |
| :---- |

| LOCKED: Ontology (as ContextOS builds it) A structured, queryable knowledge graph of the enterprise's concepts, entities, relationships, and constraints — Level 4 on the spectrum. Encodes structure, hierarchy, identity, and reconciliation rules. Not a Level 5 formal axiomatized ontology unless explicitly stated. Reference examples: typed relationships/constraints, entity resolution across systems, conflict detection with reconciliation logic. |
| :---- |

| LOCKED: Value Inflection Point Conflict detection and resolution. Mapping is table stakes. Reconciliation is the product. The moment ContextOS shows 'SAP says X, Salesforce says Y, here is why, here is who decides' — that is the demo that sells itself. |
| :---- |

| *Moderator rule: Any panelist who uses 'ontology' to mean 'field mapping' or 'semantics' to mean 'knowledge graph' will be corrected. These examples are our drift anchors.* |
| :---- |

# **ROUND 1 — The Onboarding Agent: Building the Ontology Blueprint**

*The panel focuses on the onboarding agent — the AI-assisted, FDE-supported process that creates the knowledge graph. What does it do, how does it work, what are the security considerations, and how does it handle the fact that enterprise context is dynamic and changes minute to minute?*

**Maya Chen (CTO)**

Let me describe the onboarding agent concretely, because this is the core differentiator. Nobody else has it.

The agent operates in three phases, which become three product modes.

**Phase 1 — Automated Discovery.** AOD crawls the environment — CMDB, network traffic, identity provider logs, finance contracts — and catalogs every system, every connection, every data flow. AAM extracts schemas from discovered systems. DCL runs semantic mapping against the base ontology library. Output: a first-draft knowledge graph with entities, relationships, and confidence scores. Everything the AI can figure out without talking to a human. This runs in hours, not days. It's the speed wedge.

**Phase 2 — Stakeholder Interviews.** A structured conversational agent conducts 45 to 60 minute sessions with functional leaders. The agent has already loaded the Phase 1 discovery data, so it doesn't ask stupid questions. It shows what it found and asks for confirmation, correction, or context. 'We found 14 cost centers in Workday. SAP has 11\. Here are the three that don't match — what's the story?' The output is what we call the Enterprise Contour Map: organizational hierarchy, system-of-record authority map, conflict register, management reporting overlay, vocabulary map, and priority queries. Every answer gets a confidence score and provenance tag — 'confirmed by CFO' versus 'inferred from system data.'

**Phase 3 — Living Ontology.** After initial deployment, the agent shifts to monitoring mode. It watches for schema changes, new systems coming online, entity drift, concept divergence. When it detects something — say, three new cost centers appeared in Workday that don't exist in NetSuite — it composes a targeted message to the relevant stakeholder: 'We detected new cost centers CC-4210, CC-4211, CC-4212 under Austin Engineering. Should we add them to the financial reporting hierarchy under Cloud division?' The stakeholder responds conversationally. The agent proposes an update. The FDE or data architect approves. This is the subscription engine — the ontology is alive, not static.

**Sarah Delacroix (CDO)**

Phase 2 is where I have the strongest reaction, both positive and skeptical.

**Positive:** The stakeholder interview is the single hardest part of any data governance initiative. I've run three of these programs at three companies. Every time, the bottleneck is getting functional leaders to sit down and tell you what their data means. They're busy. They don't think it's their job. They give you 20 minutes and then cancel the follow-up. If an AI agent can conduct a structured interview that respects their time, shows them what the system already knows, and only asks for the delta — that's a genuine breakthrough in the delivery model.

**Skeptical:** The interview subjects will give you their narrative, not the ground truth. The CFO will say 'our revenue recognition is clean and GAAP-compliant.' The system will show 40% of revenue is recognized in ways that don't match the stated policy. The agent needs to surface that gap diplomatically. That's a hard product design problem — you're essentially telling a C-suite executive that their own understanding of their business doesn't match reality. How does the agent handle that without damaging the relationship?

My recommendation: the agent should never contradict the stakeholder directly. It should surface the discrepancy as a 'we noticed something interesting' and flag it for the FDE to address in a separate, human-to-human conversation. The agent captures the conflict. The human resolves the politics.

**James Worthington (Enterprise Data Architect)**

Sarah's point is exactly right, and I'll extend it. The interview agent's job is not to build consensus. It's to build a complete inventory of what the organization believes, including the contradictions. The output — the Contour Map — should have a section called the Conflict Register that explicitly catalogs every instance where what a stakeholder said doesn't match what the systems show, or where two stakeholders disagree.

That register is gold. It's the thing no other product produces. Collibra doesn't produce it because it relies on manual, uncontested definitions. Informatica MDM doesn't produce it because it operates at the entity matching level, not the definitional level. The Conflict Register is ContextOS's unique deliverable.

On Phase 3 — the living ontology — this is where the product becomes indispensable or irrelevant. A static knowledge graph that was accurate on Day 1 is stale by Day 30\. Schemas change, orgs restructure, acquisitions add systems, people redefine terms. The monitoring agent has to detect structural changes — not just field additions, but conceptual shifts. If the company reorganizes from geographic segments to product segments, that's a fundamental ontology change that the agent needs to flag, not just a new field.

The minute-to-minute monitoring question is real but has a nuance. Schema changes happen infrequently — maybe weekly or monthly. But data value drift — the $2M vs. $1.8M problem — happens continuously. The agent needs two monitoring modes: structural monitoring (schema, hierarchy, relationship changes) that runs on a schedule, and value monitoring (conflict detection, threshold violations) that can run in near-real-time against active data flows.

**Robert Okafor (CISO)**

Let me walk through the security considerations for each phase, because they escalate significantly.

**Phase 1 — Discovery:** The agent needs read-only access to CMDB, network telemetry, and identity provider logs. These are medium-sensitivity data sources. The access should be scoped, time-limited, and use service account credentials that rotate after the crawl completes. The discovery phase also touches schema metadata from source systems — field names, types, cardinalities — which is low sensitivity. I can approve this in my standard third-party access review, probably two to three weeks.

**Phase 2 — Interviews:** This is where it gets complicated. The interview agent is recording conversations with executives about how the business operates, where numbers come from, and where discrepancies exist. For a public company, some of that is material nonpublic information. The transcript cannot leave the client's environment. The agent must run locally — in our VPC or on-prem — and the transcript must be stored, encrypted, under our control. AOS personnel who conduct HITL review on Phase 2 outputs need to be named, background-checked, and under NDA. For financial services, FINRA-screened. For healthcare, HIPAA-trained.

**Phase 3 — Monitoring:** The living ontology agent has persistent, ongoing access to detect changes. That's a standing connection to my systems. Standing connections require continuous monitoring, periodic re-authorization, and the ability for my team to revoke access instantly if something looks wrong. The agent's access scope cannot expand over time without explicit re-approval.

**The critical product requirement:** ContextOS needs an access dashboard that my security team can see at all times. What systems is the agent connected to? What data is it reading? When was the last access? What changes did it detect? If I can't answer those questions in real-time, I can't approve Phase 3\.

**Dr. Priya Subramaniam (Moderator)**

Three observations. First: the three-phase agent model is the right architecture, and the distinction between one-time discovery, periodic interviews, and continuous monitoring maps cleanly to different security postures and pricing models. Second: Sarah and James both converged on the Conflict Register as the unique deliverable — this should be the centerpiece of every demo. Third: Robert's escalating security model validates the two-tier product packaging we discussed in definitions — you can't deploy Phase 3 monitoring with Phase 1 security controls.

Before we move on, I want to name an assumption the panel is making: the onboarding agent requires the client to grant access. What about enterprises that won't grant any access to an external agent? Maya — is there a zero-access variant?

**Maya Chen (CTO)**

Yes, and it's actually simpler than you'd think. Phase 1 can operate on exported schema files — the client exports their data dictionaries, CMDB dumps, and org charts, and the agent processes them locally. Phase 2 interviews don't require system access at all — it's a conversation. Phase 3 monitoring is the only phase that requires standing access. For zero-access clients, you lose real-time monitoring but you can still deliver Phases 1 and 2 as a one-time engagement, then schedule periodic refresh cycles where the client exports updated schemas quarterly.

That actually creates a nice product packaging option: ContextOS Assessment (one-time, Phases 1-2, no standing access) versus ContextOS Live (ongoing, all three phases, standing access with continuous monitoring).

# **ROUND 2 — Future-Proofing: AI Shifts and Platform Relevance**

*Dr. Subramaniam requires the panel to address whether this product stays relevant as LLMs improve, agents proliferate, hyperscalers build context infrastructure, and the enterprise data stack evolves.*

**Dr. Priya Subramaniam (Moderator)**

Four structural shifts in the IT landscape could make ContextOS more valuable or obsolete within three years. I want the panel to take each seriously.

## **Shift 1: LLMs as Implicit Semantic Engines**

**Maya Chen (CTO)**

The question is whether GPT-5 or Claude's next generation will just understand enterprise data without needing an explicit semantic layer. The answer is no, for three structural reasons that won't change regardless of model capability.

First, organizational semantics are private and idiosyncratic. No foundation model will learn from public training data that your company repurposed the KUNNR field for vendor codes in one business unit three years ago. That's institutional knowledge that lives in people's heads and undocumented decisions.

Second, even if a model could infer the right semantics, the inference is not auditable. An enterprise can't tell its auditor 'the AI just knew what revenue means.' Regulatory frameworks — SOX, SR 11-7, GDPR — require documented, versioned, human-approved definitions. An explicit semantic layer provides the documentation trail. An implicit LLM inference does not.

Third, LLMs will make semantic layers faster to build, which helps us. LLM-assisted mapping is already our core approach. The risk is not obsolescence. The risk is commoditization — if building a semantic layer becomes trivially easy, our differentiation shifts entirely to the knowledge graph and the managed delivery model.

**Sarah Delacroix (CDO)**

Maya's second point is the one my compliance team would cite. I recently deployed a Copilot pilot for financial reporting. The first question from our Chief Compliance Officer was: 'When this AI says revenue is $14.2M, where did that number come from and who approved that definition?' Copilot couldn't answer that. An explicit semantic layer with provenance tracking can. That's not a theoretical advantage — it's a deal-blocking compliance requirement for any regulated enterprise.

## **Shift 2: Agentic AI Creates Exponential Demand**

**James Worthington (Enterprise Data Architect)**

This is the demand accelerant. Two years ago, my team managed semantic definitions for 15 dashboards and 30 reports. Today we're fielding requests from 8 different agent development teams, each building agents that need to understand customer entities, financial hierarchies, and organizational structure. Each team is hardcoding that context into their agent, which means we now have 8 slightly different versions of 'what does customer mean' running in production.

A centralized knowledge graph — what ContextOS proposes — is not just a data product in this world. It's a coordination mechanism. One source of truth that all agents read from. Without it, every new agent deployed multiplies the inconsistency problem. The demand curve for context infrastructure isn't linear with agent adoption — it's exponential, because each new agent creates new potential contradictions with every other agent.

**Robert Okafor (CISO)**

From a security lens, the centralized model is paradoxically both safer and riskier. Safer because I can govern one context layer with consistent access controls instead of auditing 8 bespoke context pipelines. Riskier because the knowledge graph becomes a high-value target — compromise it and you understand every agent's context, every business rule, every reconciliation decision. The security architecture has to treat the knowledge graph as crown-jewel infrastructure, on par with the data warehouse and identity provider.

## **Shift 3: Hyperscaler Competition**

**Maya Chen (CTO)**

The honest competitive assessment: if you're single-stack — all Microsoft, all Google, all AWS — the hyperscaler's native context tools will probably be good enough. Microsoft Fabric's semantic model is genuinely useful if you're in the Microsoft ecosystem. Snowflake Cortex is strong for Snowflake-native analytics.

Where they fall down permanently: multi-system, multi-cloud. Fabric doesn't understand your SAP data as well as your Azure SQL. Snowflake doesn't talk to Salesforce natively. No hyperscaler has incentive to build great cross-platform semantic resolution because their business model depends on stack consolidation. That's a structural misalignment that won't change.

ContextOS wins on the anti-consolidation thesis: we're the context layer for enterprises too complex to single-vendor. The Global 5000 minus the simplest 20% is our addressable market.

**Sarah Delacroix (CDO)**

I run in a multi-cloud, multi-vendor environment. I have Epic for clinical systems, SAP for financials, Workday for HR, Salesforce for provider relations, plus 20 specialty applications. Microsoft Fabric sees about 30% of my data estate. The other 70% is invisible to it. That 70% includes some of our most critical data — clinical outcomes, claims processing, regulatory reporting. If ContextOS can provide semantic resolution across all of those with equal depth, that's something no hyperscaler offers me.

## **Shift 4: Stack Consolidation — Myth vs. Reality**

**James Worthington (Enterprise Data Architect)**

Every three years someone predicts enterprise stack simplification. It never happens. Every acquisition adds systems. Every new regulation adds reporting requirements. Every cloud migration is partial. AI is adding new tool categories, not reducing existing ones. The complexity trend line goes up. AI increases the urgency to understand complexity, not the likelihood of reducing it. That's ContextOS's permanent tailwind.

| *Round 2 Consensus: All four shifts are net-positive for ContextOS. LLMs make semantic layers faster to build and more important to have. Agents create exponential demand. Hyperscalers leave the multi-system gap permanently. Stack complexity grows. The platform is future-proof if it executes on the delivery model.* |
| :---- |

# **ROUND 3 — Product or Service? Pricing and Competitive Positioning**

*The panel debates whether ContextOS is a product or a service, how to price it, and whether a startup can win. Marcus Webb joins for the pricing discussion.*

## **Product or Service?**

**James Worthington (Enterprise Data Architect)**

This is the most important strategic question in the entire debate, and I want to frame it precisely. What the customer buys with ContextOS is not complicated software. The technology — field mapping, entity resolution, conflict detection — is well-understood. What the customer buys is the outcome: a populated, accurate, governed knowledge graph delivered fast. The product is not the technology. The product is the time-to-value.

That means ContextOS is a product with a service wrapper, not a service with a product wrapper. The distinction matters enormously. A services company charges by the hour, has linear revenue growth tied to headcount, and trades at 2x revenue. A product company charges by the outcome, has scalable revenue, and trades at 10x+ revenue. AOS needs to be in the second category.

How to stay in the product category: the onboarding agent does 80% of the work autonomously. The FDE team handles the 20% the AI can't do. As the AI improves, the ratio shifts toward 90/10, then 95/5. The FDE cost per engagement drops over time. That's a product with improving unit economics, not a services business.

**Sarah Delacroix (CDO)**

James is right about the framing, but I'll challenge the 80/20 assumption. In my experience with data governance initiatives, the 'hard 20%' — the definitional disagreements, the political conflicts, the edge cases — is where 80% of the value lives. If the AI handles the easy stuff and humans handle the hard stuff, your cost model is inverted: the most valuable work is the most labor-intensive.

The solution is to make the hard 20% more efficient, not to eliminate it. The interview agent should reduce the time from 'identify the conflict' to 'present the conflict to a decision-maker' from weeks to hours. That's the productivity gain. The human still makes the decision, but the agent does all the preparation work. So yes, product with service wrapper — but the service is genuine expert time, not filler.

**Maya Chen (CTO)**

Architecturally, ContextOS is a platform product. The onboarding agent, the monitoring agent, the MCP server, the governance workflows — these are software. The FDE engagement is the delivery vehicle for the initial population, not the ongoing product. Once the knowledge graph is built and the monitoring agent is running, the client doesn't need FDE involvement for day-to-day operations. They need it for periodic reviews, new system integrations, and major organizational changes. That's a managed service add-on to a platform product, not a services business.

## **Pricing: Marcus Webb Joins**

**Marcus Webb (SaaS Pricing Strategist)**

I've been listening to this debate and I want to start with something blunt: you're overthinking the technology and underthinking the packaging. The buyer doesn't care about Level 3 versus Level 4\. The buyer cares about three things: how fast can I have this, what does it cost, and can I justify it to my CFO. Let me build a pricing model that addresses all three.

**Pricing Architecture**

Three layers. Each addresses a different buyer psychology and a different cost structure.

| Layer | What the Client Gets | Pricing Model | Indicative Range |
| :---- | :---- | :---- | :---- |
| ContextOS Discover (One-Time) | Automated crawl \+ schema extraction \+ first-draft semantic map. Delivered in days. The speed wedge. | Fixed project fee | $25K \- $75K based on \# of source systems (under 10 / 10-25 / 25+) |
| ContextOS Align (One-Time) | Stakeholder interviews \+ Contour Map \+ Conflict Register \+ reconciliation recommendations. Delivered in 4-8 weeks. | Fixed project fee | $75K \- $200K based on complexity (\# of functional leaders, \# of systems, regulatory overlay) |
| ContextOS Live (Subscription) | Monitoring agent, drift detection, change management, governance workflows, MCP/API access for agents. Ongoing. | Annual platform subscription \+ usage | $100K \- $300K/yr base \+ per-1000-MCP-calls overage |

**Marcus Webb (SaaS Pricing Strategist)**

Now let me explain why this structure works.

**Discover is the land.** $25K to $75K is an amount a VP-level buyer can often approve without board-level procurement. It's fast — days, not months. And it produces an immediate deliverable: a visual map of the enterprise's systems, connections, and semantic gaps. That's a demo that sells itself internally. The buyer shows their CIO or CDO and says 'look what we found in a week.' That internal champion moment is worth more than any sales pitch.

**Align is the expand.** Once the client has seen Discover, the conversation is: 'you found the gaps, now let's fill them.' The stakeholder interview process builds relationships across the C-suite. The Conflict Register becomes the executive talking point: 'we found 14 data conflicts, 3 critical, costing us an estimated $X in reconciliation effort.' That's the CFO justification for the next phase.

**Live is the embed.** This is the SaaS subscription. Recurring revenue. High switching cost. The monitoring agent and MCP API access create ongoing dependency — every agent the client deploys that reads from the knowledge graph is another reason not to churn. Usage-based MCP pricing means your revenue grows with their agent adoption, not just with your direct selling.

The critical insight: **the one-time fees fund the onboarding cost, and the subscription funds the platform.** You're not asking the client to pay $300K/year on day one with nothing to show. You're asking for $25K for a proof of value, $75K-$200K for the full build, and then an annual subscription that's justified by continuous value. Each step has its own ROI argument.

**Robert Okafor (CISO)**

The pricing tiers map nicely to security tiers. Discover is low-security — schema metadata, time-limited access, I can approve in my standard review. Align requires heavier security — interview transcripts, executive data, FDE access controls. Live requires the full security stack — standing connections, VPC deployment, continuous monitoring, incident response plan. Each price step comes with an appropriate security posture.

**Sarah Delacroix (CDO)**

On the usage-based MCP pricing — I want to flag a risk. If I'm paying per-MCP-call and I have 50 agents hitting the knowledge graph, my costs scale with agent adoption. That's great for AOS but it creates an incentive for me to build caching layers and reduce calls. You want to encourage usage, not penalize it. Consider a tiered usage model with generous included calls in the base subscription and overage only at high volumes. Don't make me optimize against your revenue model.

**Marcus Webb (SaaS Pricing Strategist)**

Sarah's right, and this is a common mistake in SaaS pricing. The fix is to include a meaningful call volume in the base subscription — say, 100K MCP calls per month — and price overages at a declining rate per tier. The first 100K are included. 100K to 500K is $X per thousand. 500K to 1M is $0.7X per thousand. Above 1M, we have a conversation about an enterprise agreement. This encourages adoption and rewards scale without creating a perverse optimization incentive.

## **Competitive Positioning**

**Maya Chen (CTO)**

| Competitor | Depth | Speed | Multi-System | Agent-Ready |
| :---- | :---- | :---- | :---- | :---- |
| Palantir Ontology | Level 4-5 | 6-18 months | Broad (requires ingestion) | Walled garden |
| Microsoft Fabric | Level 3-4 | Fast if Microsoft-only | Microsoft-centric | Copilot only |
| Snowflake Cortex | Level 3 | Fast if Snowflake-only | Snowflake-centric | Limited |
| Alation / Collibra | Level 2-3 | Months | Broad connectors | Not agent-native |
| Informatica MDM | Level 4 (entity only) | 12-24 months | Broad | Not agent-native |
| ContextOS (proposed) | Level 3→4 | Days to weeks | Multi-system native | MCP-native |

The whitespace: nobody offers fast \+ multi-system \+ agent-native \+ managed delivery. Palantir is deep but slow and expensive. Hyperscalers are fast but single-stack. Alation and Collibra are catalogs, not knowledge graphs. Informatica MDM is deep on entity matching but takes two years and costs $4M+. We win on the combination of speed, breadth, and the managed delivery model.

| *Round 3 Consensus: ContextOS is a product with a service delivery wrapper, not a services business. Pricing is three-layer: Discover (land, $25-75K one-time), Align (expand, $75-200K one-time), Live (embed, $100-300K/yr subscription \+ usage). The one-time fees fund onboarding cost; the subscription funds the platform. Each tier maps to a security posture and a buyer justification.* |
| :---- |

# **ROUND 4 — Final Positions and Consensus Deliverables**

*Each panelist delivers their final recommendation. Then the panel converges on three consensus deliverables: MVP feature specs, production-grade features, and security protocols. No hung jury — disagreements are noted, but a decision is reached on every point.*

**Maya Chen (CTO) — Final Position**

Build it. Call it ContextOS. Ship Discover in 4 months, Align in 8, Live in 12\. Architecture decisions: OWL/RDF-compatible output from day one. MCP server as primary consumption interface. VPC deployment option on the critical path. The onboarding agent is the product differentiator — nobody else has managed ontology delivery at this speed and price point.

**Robert Okafor (CISO) — Final Position**

Approve it if the security non-negotiables are met. The eight requirements from last round stand. If ContextOS meets them, the security story becomes competitive advantage, not a blocker. I'd champion it internally as AI governance infrastructure.

**James Worthington (Data Architect) — Final Position**

Champion it if time-to-value is real. Show me working cross-system context in two weeks. Show me the Conflict Register. Give me governance that my compliance team accepts. Build industry packs for financial services within 18 months. And the output must be open standard — OWL/RDF or I walk.

**Sarah Delacroix (CDO) — Final Position**

Champion it if it sits on top of my existing investments, not replacing them. ContextOS must integrate with Collibra and Alation, not compete with them. It fills the gap between what my data catalog provides — Level 2-3 — and what my agents need — Level 4\. The Conflict Register is the killer feature. The stakeholder interview agent is the delivery breakthrough. I'd need to see it work with real healthcare data complexity, not just generic enterprise concepts.

**Dr. Priya Subramaniam (Moderator) — Final Position**

The panel reaches consensus: build it. The disagreements are on timing and scope — not on the fundamental product thesis. The demand is real, the whitespace is genuine, and AOS has a credible head start. Two cautions: do not oversell formalism (call it what it is — a knowledge graph, not a formal ontology), and do not underfund the FDE team (the managed delivery model is the moat, and thin expertise will destroy credibility).

# **CONSENSUS DELIVERABLE 1: MVP Feature Specifications**

*What must ship first to prove the thesis and close the first five deals. Targeted at 4-month delivery. This is ContextOS Discover \+ a preview of Align.*

## **MVP Scope: ContextOS Discover**

| Feature | Description | Acceptance Criteria |
| :---- | :---- | :---- |
| Automated System Discovery | AOD crawls CMDB, network metadata, IdP logs, finance contracts to catalog all enterprise systems and connections | Discovers 90%+ of systems detectable via standard enterprise signals within 4 hours for environments with ≤25 systems |
| Schema Extraction | AAM extracts field names, types, cardinalities, and relationships from discovered systems via read-only schema metadata access | Covers top 30 enterprise systems (SAP, Salesforce, Workday, NetSuite, Oracle, ServiceNow, Jira, etc.) via connector library |
| Semantic Mapping (Level 3\) | DCL maps extracted fields to base ontology library using heuristic \+ LLM-assisted matching. Confidence scores on every mapping. | Base library of 200+ enterprise concepts across 10 domains. Mapping accuracy ≥85% for standard enterprise fields. All mappings include confidence score and provenance. |
| First-Draft Knowledge Graph | Auto-generated entity-relationship graph from discovery \+ schema \+ semantic mapping | Entities typed and linked. Relationship types include HAS, GENERATES, BELONGS\_TO, REPORTS\_TO at minimum. Visual graph explorer in UI. |
| Conflict Detection (Basic) | Flag instances where two systems describe the same entity with different values or structures | Detects field-level conflicts (same entity, different values) and structural conflicts (different hierarchies for same dimension). Displays conflict with source attribution. |
| Export (Open Standard) | Knowledge graph exportable in OWL/RDF-compatible format | Client can download the complete graph in Turtle or JSON-LD format. Validated against W3C spec. |
| MCP Server (Read) | Expose semantic catalog and knowledge graph via MCP for agent consumption | concept\_lookup, semantic\_search, entity\_resolve, conflict\_list tools functional. Documented API. Sub-200ms response for cached queries. |
| Security: Scoped Access | Time-limited, read-only, schema-metadata-only connection model | Credentials rotate after discovery window closes. No persistent connection. Access log exportable to client. |

## **MVP Preview: ContextOS Align (Onboarding Agent v1)**

| Feature | Description | Acceptance Criteria |
| :---- | :---- | :---- |
| Stakeholder Interview Agent | Structured conversational agent conducts 45-60 min sessions with functional leaders. Loads discovery data, asks for confirmation/correction/context. | Covers 5 interview sections: Business Overview, System Authority, Dimensional Walkthrough, Management Reporting, Pain Points. Shows discovered data inline. One question at a time. |
| Enterprise Contour Map | Structured output from interviews: org hierarchy, SOR authority map, conflict register, management overlay, vocabulary map, priority queries, follow-up tasks | JSON artifact with confidence scores and provenance on every element. Exportable. Renders visually in ContextOS UI. |
| Conflict Register | Comprehensive catalog of conflicts between stakeholder statements and system data, and between stakeholders | Every conflict includes: dimension, systems involved, stakeholder positions, system evidence, resolution status (resolved/unresolved/escalated), and recommended action. |
| HITL Review Workflow | FDE reviews agent output before it flows into production knowledge graph | Every interview output goes through human approval. FDE sees full context including transcript excerpts. Approve, reject, or modify individual elements. |
| Security: Interview Data | Interview transcripts stored in client environment only. No data exfiltration. | Transcripts encrypted at rest. AOS personnel access limited to abstracted conflict flags. Full transcript visible only to client-side roles. |

**MVP: What Is Explicitly NOT Included**

Entity resolution (golden records) — deferred to production. Value-level monitoring (real-time conflict detection on live data) — deferred to Live tier. Industry-specific ontology packs — deferred to 18-month roadmap. VPC/on-prem deployment — on critical path but may not ship with initial MVP if cloud-first clients are the first 5 deals. Approval workflow governance (multi-step, role-based) — deferred to production.

| *Panel dissent (James): 'Entity resolution should be in MVP. Without golden records, you cannot demonstrate Ontology Example 2 — the single most compelling demo.' Panel resolution: Entity resolution ships as beta in MVP, with explicit 'assisted mode' labeling (AI proposes matches, human confirms). Full production-grade entity resolution requires validated accuracy benchmarks against real data, which takes longer than 4 months.* |
| :---- |

# **CONSENSUS DELIVERABLE 2: Production-Grade Enterprise Features**

*What must exist for ContextOS to be deployed at a Fortune 500 with full compliance and governance requirements. Target: 12-18 months post-MVP.*

| Category | Feature | Description | Why It's Required |
| :---- | :---- | :---- | :---- |
| Knowledge Graph | Entity Resolution (Production) | AI-assisted matching with human confirmation. Golden records. Canonical identity with alias tracking. Merge/unmerge with full undo. | Without entity resolution, ContextOS is a semantic layer, not a knowledge graph. This is the Level 3→4 transition. |
| Knowledge Graph | Typed Relationship Engine | Encode HAS, GENERATES, BELONGS\_TO, REPORTS\_TO, RECOGNIZED\_IN and custom relationship types with constraints and cardinality rules | Enables machine traversal. Agents can ask 'what contracts generate revenue for this account' and get a structured answer. |
| Knowledge Graph | Reconciliation Logic Engine | Per-dimension, per-system-pair rules for how conflicts should be resolved. Supports: accept system A, accept system B, weighted average, escalate, manual review. | James's value inflection point. Without reconciliation logic, conflict detection is informational. With it, conflict detection is actionable. |
| Monitoring | Structural Drift Detection | Detects schema changes, hierarchy reorganizations, new systems, deprecated fields. Runs on configurable schedule. | Prevents knowledge graph from going stale. Alerts data architect to structural changes before they cause downstream failures. |
| Monitoring | Value Drift Detection | Near-real-time monitoring of entity values across systems. Flags new conflicts, resolved conflicts, and threshold violations. | This is the minute-to-minute monitoring capability. Essential for financial data reconciliation. |
| Monitoring | Change Proposal Agent | When drift is detected, agent composes a targeted message to the relevant stakeholder with proposed update. Stakeholder responds conversationally. Agent proposes graph modification. | The living ontology engine. Keeps the knowledge graph current without requiring periodic manual refresh. |
| Governance | Multi-Role Approval Workflow | Changes to production knowledge graph require approval from designated roles (Data Architect, Domain Owner, FDE). Configurable approval chains. | Enterprise governance requirement. No single person should be able to modify the production graph without review. |
| Governance | Full Version History | Every change to the knowledge graph is versioned. Full diff view. Rollback to any previous version. | Compliance and audit trail. Answers: who changed what, when, and why. |
| Governance | RBAC on Knowledge Graph | Role-based access control on graph elements. Data Architect sees everything. CFO sees finance domain. Agent X sees only the entities in its scope. | Security requirement. Prevents overprivileged access to sensitive business rules. |
| Integration | Data Catalog Connectors | Bidirectional sync with Collibra, Alation, and Informatica. Import existing business glossary. Export ContextOS enrichments back to catalog. | Sarah's requirement: ContextOS must sit on top of existing investments, not replace them. |
| Integration | MCP Server (Full) | Expanded MCP tools: concept\_lookup, semantic\_search, entity\_resolve, conflict\_query, reconciliation\_recommend, provenance\_trace. Write-capable for agent actions with approval workflow. | Full agent context infrastructure. Every enterprise agent reads from and potentially writes to the knowledge graph via MCP. |
| Scale | Multi-Tenant SaaS | Tenant isolation, per-tenant knowledge graphs, shared base ontology library with tenant-specific extensions | Enterprise SaaS requirement for scalable delivery. |
| Scale | VPC / On-Prem Deployment | Containerized deployment in client's VPC or on-premises infrastructure. Same feature set as cloud deployment. | Deal-gate for regulated industries. Robert's non-negotiable. |
| Vertical | Industry Ontology Packs | Pre-built concept libraries, relationship templates, and reconciliation rules for specific industries. Financial Services first (ISDA terms, GAAP/IFRS, regulatory hierarchies). | James's requirement: 200 generic concepts don't cover financial services. Industry packs bridge the gap. |

**Production Features: Disagreements Noted, Decisions Made**

**Entity resolution accuracy target:** James argues for 99%+ before production label. Maya argues 95% with assisted mode is production-viable since every match is human-confirmed. **Decision:** 95% AI accuracy with 100% human confirmation in V1. Automated approval (no human confirmation) requires 99%+ accuracy, validated on client-specific data.

**Industry pack scope:** Sarah wants healthcare as the first vertical. James wants financial services. **Decision:** Financial services first (larger market, higher ACV, Ilya's domain expertise). Healthcare second, within 6 months of financial services launch.

**VPC timeline:** Robert wants it at launch. Maya says it's 3-6 months of infrastructure work. **Decision:** VPC ships within 6 months of MVP launch. Cloud-first clients (non-regulated) are the first 5 deals. First regulated deal requires VPC, which gates the timeline.

# **CONSENSUS DELIVERABLE 3: Security Protocols**

*Robert's eight non-negotiables, expanded into implementable security protocols. The panel reached full consensus on all eight. No dissent.*

## **Security Protocol 1: Data Processing Agreement**

| Element | Requirement |
| :---- | :---- |
| Timing | Signed before any system connection. No exceptions. |
| Scope | Defines exactly what ContextOS accesses, processes, and retains at each tier (Discover, Align, Live). |
| Retention | Schema metadata retained for duration of engagement \+ 30 days. Interview transcripts retained only in client environment. Knowledge graph retained for contract duration. All data purged within 90 days of contract termination. |
| Sub-processors | Named list of any third-party services involved in processing. Updates require 30-day advance notice. |
| Breach notification | 72-hour notification window. Includes scope of breach, affected data, remediation steps. |

## **Security Protocol 2: Access Control — Discovery Phase**

| Element | Requirement |
| :---- | :---- |
| Credential type | Service account with read-only permissions. Scoped to schema metadata only (field names, types, relationships). No row data access. |
| Duration | Time-limited. Credentials expire when discovery window closes (configurable, default 72 hours). No auto-renewal. |
| Rotation | Credentials rotate on every engagement. No reuse across clients. |
| Audit | Complete log of every system touched, every query executed, every field read. Log exported to client at engagement completion. |
| Revocation | Client can revoke access instantly via single API call or dashboard button. |

## **Security Protocol 3: Interview Data Protection**

| Element | Requirement |
| :---- | :---- |
| Storage location | Client environment only. VPC or on-prem for regulated industries. Cloud clients: encrypted object storage in client's cloud account. |
| Transcript access | Full transcript visible only to client-side roles. AOS personnel see abstracted conflict flags and structured contour map elements only. |
| Encryption | AES-256 at rest. TLS 1.3 in transit. Client-managed keys where possible (BYOK). |
| Retention | Interview transcripts retained for engagement duration \+ as defined in DPA. Client controls deletion. |
| Regulatory overlay | For public companies: transcripts flagged as potentially MNPI. For healthcare: HIPAA BAA required. For financial services: FINRA-screened personnel. |

## **Security Protocol 4: Knowledge Graph Security**

| Element | Requirement |
| :---- | :---- |
| Classification | Knowledge graph classified as Medium sensitivity (Discover tier) to High sensitivity (Live tier with financial reconciliation data). Treatment matches classification. |
| Encryption | AES-256 at rest. TLS 1.3 in transit. Encrypted backups with separate key. |
| Access control | RBAC enforced at graph element level. Data Architect: full access. Domain Owner: domain-scoped. Agents: query-scoped, read-only unless write-approved. |
| Audit trail | Every read, write, and query logged. Immutable audit log exportable to client SIEM. Minimum 12-month retention. |
| Backup / DR | Automated daily backups. RTO 4 hours. RPO 1 hour. Tested quarterly. |

## **Security Protocol 5: VPC / On-Prem Deployment**

| Element | Requirement |
| :---- | :---- |
| Deployment model | Containerized (Docker/Kubernetes). Runs in client's VPC or on-prem infrastructure. No data leaves client perimeter. |
| Update mechanism | Signed, versioned container images pulled from AOS registry. Client controls update timing. Rollback supported. |
| Telemetry | Health metrics only (uptime, error counts, latency). No business data in telemetry. Telemetry can be disabled entirely. |
| Support access | AOS support accesses via client-controlled VPN or bastion host. Named individuals only. Access logged and time-limited. |
| Availability target | Available within 6 months of MVP launch. Required for any regulated industry deployment. |

## **Security Protocol 6: FDE Personnel Standards**

| Element | Requirement |
| :---- | :---- |
| Background checks | All FDE personnel pass criminal background check and credit check (for financial services clients). |
| Regulatory screening | Financial services clients: FINRA-eligible. Healthcare: HIPAA training certified. Defense: US person verification. |
| NDA | Individual NDA signed by each FDE assigned to the engagement. Covers all client data encountered. |
| Named individuals | Client receives a personnel list before engagement starts. Changes require advance notification. |
| Access scope | Documented per-FDE access scope. FDE sees only the elements they need for their review function. Principle of least privilege. |

## **Security Protocol 7: HITL Audit Trail**

| Element | Requirement |
| :---- | :---- |
| Scope | Every human decision in the HITL workflow logged: what was reviewed, what data was visible, what decision was made, rationale if provided. |
| Format | Structured JSON log. Queryable. Exportable to client GRC platform. |
| Access | Client has real-time access to HITL audit log. AOS has access for quality assurance. Log is immutable. |
| Retention | Minimum 7 years for regulated industries. Configurable per client. |

## **Security Protocol 8: Client Ownership and Portability**

| Element | Requirement |
| :---- | :---- |
| Ownership | Client owns the knowledge graph. Contractual guarantee. Not licensed — owned. |
| Export format | OWL/RDF (Turtle or JSON-LD). Complete graph including entities, relationships, constraints, confidence scores, provenance, and version history. |
| Export mechanism | One-click full export from ContextOS UI or API. No AOS involvement required. |
| Termination | Upon contract termination, client receives a complete export. AOS retains no copy after 90-day wind-down period. |
| Portability guarantee | Export is complete and self-contained. Client can load it into any OWL/RDF-compatible platform (Stardog, Neo4j, Apache Jena, etc.) without AOS tooling. |

| *Security consensus: All 8 protocols unanimous. No dissent. Robert: 'If these are met, ContextOS has a stronger security posture than 90% of the AI tools my team evaluates. That becomes a selling point, not a friction point.'* |
| :---- |

# **FINAL SYNTHESIS — The Case for ContextOS**

**Dr. Priya Subramaniam (Moderator)**

Four rounds. Six perspectives. The panel reached consensus on every substantive question. Here is the synthesis.

## **The One-Paragraph Case**

| *Every enterprise deploying AI agents needs a single, authoritative source of business context — what the data means, how entities relate, where systems disagree, and what rules govern reconciliation. ContextOS delivers this in weeks instead of months through an AI-assisted onboarding agent with expert human oversight. The hyperscalers won't build it because their incentive is stack consolidation. Palantir won't offer it at this speed or price. The data catalog vendors don't have the depth. The market is every enterprise too complex to single-vendor — roughly the Global 5000 — and the competitive window is now.* |
| :---- |

## **What ContextOS Is**

A context infrastructure platform for the agentic enterprise. It produces a populated, governed, living knowledge graph — Level 4 on the spectrum — through AI-assisted discovery, structured stakeholder interviews, and continuous monitoring. It is a product with a managed delivery wrapper, not a services business.

## **What ContextOS Is Not**

It is not a formal ontology (Level 5). It is not a data catalog replacement. It is not a data integration platform. It does not store or move business data. It sits on top of existing infrastructure and adds the context layer that catalogs, integration platforms, and AI agents all need and none currently provide at enterprise scale.

## **Open Questions for Leadership**

| Question | Impact | Who Decides |
| :---- | :---- | :---- |
| Is this the primary product bet for 12-18 months? | Half-commitment produces a demo, not a product | CEO |
| Hire FDEs with financial services domain expertise now? | First industry vertical requires domain depth | CEO \+ CTO |
| VPC: build in-house or partner? | 3-6 months infrastructure. Partnering (e.g., Replicated) may accelerate. | CTO |
| Data catalog integration: build or wait for demand? | Sarah's requirement. CDOs at target enterprises need this. | CTO |
| First 5 prospects: who specifically? | Shapes MVP priorities and industry focus | CEO \+ Sales |
| Patent strategy on managed ontology delivery model? | AI \+ FDE delivery model may be patentable | CEO \+ Legal |

## **Recommended Timeline**

| Milestone | Target | Dependencies |
| :---- | :---- | :---- |
| Architecture decisions locked (OWL/RDF, MCP, VPC roadmap) | Month 1 | CTO decision |
| ContextOS Discover MVP (internal) | Month 4 | Engineering build |
| Onboarding Agent v1 (internal) | Month 6 | Discover complete \+ FDE team hired |
| First external pilot (Discover \+ Align preview) | Month 7 | Pilot client identified |
| Entity Resolution beta | Month 8 | Farm test data validated |
| ContextOS Live v1 (monitoring agent) | Month 10 | Discover \+ Align stable |
| VPC deployment option | Month 10-12 | Build or partner decision |
| Financial Services ontology pack v1 | Month 12-14 | Domain FDE hired |
| Full production launch (all three tiers) | Month 14-16 | All features validated on pilot clients |

**Appendix: Persona for Round 5**

The panel recommends adding a **VP of AI / Head of ML Engineering** for a Round 5 discussion. This persona represents the agent builder — the internal team deploying AI agents that will consume the knowledge graph via MCP. Their concerns are different from the data architect's (governance) and the CDO's (catalog integration). They care about API latency, schema stability, and whether the MCP interface is flexible enough to support their agent frameworks. They're also the persona most likely to attempt building context infrastructure internally — understanding their build-vs-buy calculus is essential for competitive positioning.