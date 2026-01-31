export function UserGuide() {
  return (
    <div className="h-full overflow-auto bg-background p-8">
      <div className="max-w-4xl mx-auto space-y-8">
        <div>
          <h1 className="text-3xl font-bold text-foreground mb-2">DCL User Guide</h1>
          <p className="text-muted-foreground">
            Data Connectivity Layer — Part of the AutonomOS Platform
          </p>
        </div>

        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            What is DCL?
          </h2>
          <p className="text-foreground/90 leading-relaxed">
            DCL is the <strong>semantic translator</strong> for the AutonomOS (AOS) platform. 
            It answers the critical business question: <em>"What does this field mean to the business?"</em>
          </p>
          <p className="text-foreground/90 leading-relaxed">
            Enterprise systems like SAP, Salesforce, and databases use cryptic field names 
            like <code className="bg-muted px-1 rounded">KUNNR</code>, <code className="bg-muted px-1 rounded">acct_id</code>, 
            or <code className="bg-muted px-1 rounded">cust_rev_ytd</code>. DCL automatically maps these 
            to business concepts like "Account" or "Revenue" — and shows which executives care about each one.
          </p>
        </section>

        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            Where DCL Fits in AutonomOS
          </h2>
          <div className="bg-card border border-border rounded-lg p-4 font-mono text-sm">
            <div className="text-muted-foreground mb-2">AutonomOS Architecture:</div>
            <div className="space-y-1">
              <div className="text-purple-400">Applications: FinOps Agent, RevOps Agent, Domain Agents</div>
              <div className="text-blue-400">Platform: AOA (Orchestration), NLQ (Query), Control Center</div>
              <div className="text-teal-400">Infrastructure: AOD (Discover) → AAM (Connect) → <strong className="text-white">DCL (Unify)</strong></div>
            </div>
          </div>
          <p className="text-foreground/90 leading-relaxed">
            DCL sits at the infrastructure layer alongside AOD (Asset Observation & Discovery) 
            and AAM (Adaptive API Mesh). While AOD finds what exists and AAM establishes connections, 
            DCL unifies the meaning of the data for downstream consumers.
          </p>
        </section>

        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            The Sankey Visualization
          </h2>
          <p className="text-foreground/90 leading-relaxed">
            The main display is a 4-layer Sankey diagram showing data flow from left to right:
          </p>
          <div className="bg-card border border-border rounded-lg p-4 space-y-3">
            <div className="flex items-start gap-3">
              <span className="w-24 shrink-0 font-medium text-muted-foreground">L0 - Pipeline</span>
              <span className="text-foreground/80">
                Entry point showing which data mode is active (Demo or Farm)
              </span>
            </div>
            <div className="flex items-start gap-3">
              <span className="w-24 shrink-0 font-medium text-teal-400">L1 - Sources</span>
              <span className="text-foreground/80">
                Connected data systems: CRMs (Salesforce, HubSpot), ERPs (SAP, NetSuite), 
                databases (MongoDB, Supabase), warehouses, and integration platforms (MuleSoft)
              </span>
            </div>
            <div className="flex items-start gap-3">
              <span className="w-24 shrink-0 font-medium text-blue-400">L2 - Ontology</span>
              <span className="text-foreground/80">
                Business concepts that fields map to: Revenue, Cost, Account, Opportunity, 
                Health Score, Usage Metrics, Date/Timestamp, AWS Resource
              </span>
            </div>
            <div className="flex items-start gap-3">
              <span className="w-24 shrink-0 font-medium text-purple-400">L3 - Personas</span>
              <span className="text-foreground/80">
                Executive roles who consume each concept: CFO (Finance), CRO (Revenue), 
                COO (Operations), CTO (Technology)
              </span>
            </div>
          </div>
          <p className="text-foreground/90 leading-relaxed">
            Link thickness shows flow strength — more fields mapped means thicker connections.
          </p>
        </section>

        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            AI-Powered Semantic Mapping
          </h2>
          <p className="text-foreground/90 leading-relaxed">
            DCL uses AI to understand what cryptic field names actually mean:
          </p>
          <div className="bg-card border border-border rounded-lg p-4 space-y-2 font-mono text-sm">
            <div className="text-muted-foreground">Example Mapping:</div>
            <div><span className="text-teal-400">Source Field:</span> "cust_rev_ytd"</div>
            <div className="pl-4">↓ <span className="text-muted-foreground">AI Analysis</span></div>
            <div><span className="text-blue-400">Business Concept:</span> "Revenue"</div>
            <div><span className="text-purple-400">Confidence:</span> 95%</div>
          </div>
          <p className="text-foreground/90 leading-relaxed">
            The system uses OpenAI embeddings + Pinecone vector search (RAG) to find semantic matches, 
            then validates with GPT-4o-mini for confidence scoring.
          </p>
        </section>

        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            Zero-Trust Security
          </h2>
          <div className="bg-card border border-border rounded-lg p-4">
            <p className="text-foreground font-medium mb-2">DCL never stores your data. Ever.</p>
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <div className="text-green-400 font-medium mb-1">What DCL Stores:</div>
                <ul className="text-foreground/80 space-y-1 list-disc list-inside">
                  <li>Schema metadata (field names, types)</li>
                  <li>Mapping decisions (field → concept)</li>
                  <li>Pointers (offsets, cursors)</li>
                </ul>
              </div>
              <div>
                <div className="text-red-400 font-medium mb-1">What DCL Never Stores:</div>
                <ul className="text-foreground/80 space-y-1 list-disc list-inside">
                  <li>Row data</li>
                  <li>Customer records</li>
                  <li>Actual payloads</li>
                </ul>
              </div>
            </div>
          </div>
        </section>

        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            Controls
          </h2>
          <div className="space-y-4">
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Data Mode</h3>
              <ul className="text-foreground/80 space-y-1 list-disc list-inside">
                <li><strong>Demo</strong> — Pre-configured schemas from CSV files (training/demos)</li>
                <li><strong>Farm</strong> — Live schemas from AOS-Farm API (production discovery)</li>
              </ul>
            </div>
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Run Mode</h3>
              <ul className="text-foreground/80 space-y-1 list-disc list-inside">
                <li><strong>Dev</strong> — Heuristic pattern matching (~1 second, good for common patterns)</li>
                <li><strong>Prod</strong> — AI-powered RAG semantic matching (~5 seconds, higher accuracy)</li>
              </ul>
            </div>
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Persona Filters (CFO, CRO, COO, CTO)</h3>
              <p className="text-foreground/80">
                Filter the visualization to show only data flows relevant to specific executive roles. 
                Select multiple personas to see their combined view.
              </p>
            </div>
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Run Button</h3>
              <p className="text-foreground/80">
                Executes the semantic mapping pipeline. The timer shows processing duration.
              </p>
            </div>
          </div>
        </section>

        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            Right Panel
          </h2>
          <div className="space-y-3">
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Monitor Tab</h3>
              <p className="text-foreground/80">
                Shows persona-specific views and metrics. Select a persona from the top buttons 
                to see their data perspective. The RAG History sub-tab shows semantic matches from AI mapping.
              </p>
            </div>
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Narration Tab</h3>
              <p className="text-foreground/80">
                Real-time log of pipeline activity: schema loading, source normalization, 
                mapping operations, and orchestration steps.
              </p>
            </div>
          </div>
          <p className="text-foreground/90 leading-relaxed">
            Use the arrow button to collapse/expand the panel for more graph space.
          </p>
        </section>

        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            Navigation
          </h2>
          <div className="space-y-3">
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Graph</h3>
              <p className="text-foreground/80">
                The main Sankey visualization showing semantic data flow from sources → concepts → personas.
              </p>
            </div>
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Dashboard</h3>
              <p className="text-foreground/80">
                Detailed tables and statistics about sources, mappings, and semantic connections.
              </p>
            </div>
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Guide</h3>
              <p className="text-foreground/80">
                This page — explaining how to use DCL.
              </p>
            </div>
          </div>
        </section>

        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            Key Capabilities
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div className="bg-card border border-border rounded-lg p-3">
              <h3 className="font-medium text-foreground mb-1">Auto-Discovery</h3>
              <p className="text-foreground/80 text-sm">Finds schemas across source systems automatically</p>
            </div>
            <div className="bg-card border border-border rounded-lg p-3">
              <h3 className="font-medium text-foreground mb-1">AI-Powered Mapping</h3>
              <p className="text-foreground/80 text-sm">Near-perfect accuracy matching fields to concepts</p>
            </div>
            <div className="bg-card border border-border rounded-lg p-3">
              <h3 className="font-medium text-foreground mb-1">Intelligent Learning</h3>
              <p className="text-foreground/80 text-sm">Learns from every mapping decision over time</p>
            </div>
            <div className="bg-card border border-border rounded-lg p-3">
              <h3 className="font-medium text-foreground mb-1">Source Normalization</h3>
              <p className="text-foreground/80 text-sm">34 canonical sources from hundreds of raw sources</p>
            </div>
            <div className="bg-card border border-border rounded-lg p-3">
              <h3 className="font-medium text-foreground mb-1">Real-Time Visualization</h3>
              <p className="text-foreground/80 text-sm">Interactive Sankey shows data flow instantly</p>
            </div>
            <div className="bg-card border border-border rounded-lg p-3">
              <h3 className="font-medium text-foreground mb-1">Confidence Scoring</h3>
              <p className="text-foreground/80 text-sm">Low confidence triggers AI validation</p>
            </div>
          </div>
        </section>

        <div className="text-center text-muted-foreground text-sm pt-8 border-t border-border">
          DCL — Data Connectivity Layer | Part of AutonomOS
        </div>
      </div>
    </div>
  );
}
