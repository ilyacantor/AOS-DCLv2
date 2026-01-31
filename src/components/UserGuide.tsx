export function UserGuide() {
  return (
    <div className="h-full overflow-auto bg-background p-8">
      <div className="max-w-4xl mx-auto space-y-8">
        <div>
          <h1 className="text-3xl font-bold text-foreground mb-2">User Guide</h1>
          <p className="text-muted-foreground">
            A plain-English guide to understanding and using the Data Connectivity Layer
          </p>
        </div>

        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            What is DCL?
          </h2>
          <p className="text-foreground/90 leading-relaxed">
            DCL (Data Connectivity Layer) is a tool that helps you see where your company's data comes from 
            and who uses it. Think of it as a map that shows how information flows from your various 
            business systems (like Salesforce, SAP, or your databases) to the people who need it.
          </p>
          <p className="text-foreground/90 leading-relaxed">
            Instead of asking IT "where does this number come from?", you can use DCL to visually 
            trace data from its source all the way to the executive who relies on it.
          </p>
        </section>

        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            The Main Graph
          </h2>
          <p className="text-foreground/90 leading-relaxed">
            The colorful diagram you see is called a "Sankey graph." It shows data flowing from left to right:
          </p>
          <div className="bg-card border border-border rounded-lg p-4 space-y-3">
            <div className="flex items-start gap-3">
              <span className="w-20 shrink-0 text-teal-400 font-medium">Left side</span>
              <span className="text-foreground/80">
                <strong>Data Sources</strong> — Your business systems like Salesforce CRM, SAP ERP, 
                MongoDB databases, and integration platforms like MuleSoft.
              </span>
            </div>
            <div className="flex items-start gap-3">
              <span className="w-20 shrink-0 text-blue-400 font-medium">Middle</span>
              <span className="text-foreground/80">
                <strong>Business Concepts</strong> — What the data means in business terms: Revenue, 
                Cost, Account, Opportunity, Health Score, etc.
              </span>
            </div>
            <div className="flex items-start gap-3">
              <span className="w-20 shrink-0 text-purple-400 font-medium">Right side</span>
              <span className="text-foreground/80">
                <strong>Personas</strong> — Who uses this data: CFO (finance), CRO (revenue), 
                COO (operations), CTO (technology).
              </span>
            </div>
          </div>
          <p className="text-foreground/90 leading-relaxed">
            The thickness of each connecting line shows how much data flows through that path. 
            Thicker lines mean more fields or stronger connections.
          </p>
        </section>

        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            Top Controls
          </h2>
          <div className="space-y-4">
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Data Mode (Demo / Farm)</h3>
              <ul className="text-foreground/80 space-y-1 list-disc list-inside">
                <li><strong>Demo</strong> — Uses sample data for training and demonstrations</li>
                <li><strong>Farm</strong> — Connects to live production data sources</li>
              </ul>
            </div>
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Run Mode (Dev / Prod)</h3>
              <ul className="text-foreground/80 space-y-1 list-disc list-inside">
                <li><strong>Dev</strong> — Fast processing using pattern matching (about 1 second)</li>
                <li><strong>Prod</strong> — AI-powered mapping using machine learning (about 5 seconds, more accurate)</li>
              </ul>
            </div>
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Persona Buttons (CFO, CRO, COO, CTO)</h3>
              <p className="text-foreground/80">
                Click these to filter the graph to show only data relevant to that role. 
                Click multiple buttons to see data for several roles at once.
              </p>
            </div>
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Run Button</h3>
              <p className="text-foreground/80">
                Click "Run" to execute the data mapping pipeline. The timer shows how long processing takes.
              </p>
            </div>
          </div>
        </section>

        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            Right Panel
          </h2>
          <p className="text-foreground/90 leading-relaxed">
            The panel on the right side can be collapsed or expanded using the arrow button. It has two tabs:
          </p>
          <div className="space-y-3">
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Monitor Tab</h3>
              <p className="text-foreground/80">
                Shows metrics and details for each persona. Select a persona from the top buttons 
                to see their specific view and key performance indicators.
              </p>
            </div>
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Narration Tab</h3>
              <p className="text-foreground/80">
                A live log that shows what's happening during processing. You'll see messages like 
                "Loading schemas..." and "Mapping fields to concepts..." as the system works.
              </p>
            </div>
          </div>
        </section>

        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            How AI Mapping Works
          </h2>
          <p className="text-foreground/90 leading-relaxed">
            When you run in <strong>Prod mode</strong>, DCL uses artificial intelligence to understand 
            what your data fields mean:
          </p>
          <ol className="text-foreground/80 space-y-2 list-decimal list-inside">
            <li>It looks at field names like "cust_revenue_ytd" or "acct_id"</li>
            <li>It converts these into mathematical representations (embeddings)</li>
            <li>It searches a knowledge base to find the closest matching business concept</li>
            <li>An AI model validates the match and assigns a confidence score</li>
          </ol>
          <p className="text-foreground/90 leading-relaxed">
            This means even cryptic database column names can be automatically understood and 
            connected to meaningful business concepts like "Revenue" or "Account."
          </p>
        </section>

        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            Navigation Tabs
          </h2>
          <div className="space-y-3">
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Graph</h3>
              <p className="text-foreground/80">
                The main Sankey visualization showing data flow from sources to personas.
              </p>
            </div>
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Dashboard</h3>
              <p className="text-foreground/80">
                An alternative view with detailed tables and statistics about your data sources, 
                mappings, and semantic connections.
              </p>
            </div>
            <div className="bg-card border border-border rounded-lg p-4">
              <h3 className="font-medium text-foreground mb-2">Guide (this page)</h3>
              <p className="text-foreground/80">
                The user guide you're reading now, explaining how to use DCL.
              </p>
            </div>
          </div>
        </section>

        <section className="space-y-4">
          <h2 className="text-xl font-semibold text-foreground border-b border-border pb-2">
            Tips
          </h2>
          <ul className="text-foreground/80 space-y-2 list-disc list-inside">
            <li>Start with Demo mode to learn how the system works</li>
            <li>Use Dev mode for quick iterations, Prod mode for accuracy</li>
            <li>Click persona buttons to focus on data relevant to specific roles</li>
            <li>Hover over nodes in the graph to see details</li>
            <li>Watch the Narration tab to understand what's happening behind the scenes</li>
            <li>Collapse the right panel for a larger graph view</li>
          </ul>
        </section>

        <div className="text-center text-muted-foreground text-sm pt-8 border-t border-border">
          DCL — Data Connectivity Layer
        </div>
      </div>
    </div>
  );
}
