# Frontend Integration Guide (V2)

Status: ✅ Up-to-date with current backend tool outputs.

This guide shows how to map visualizations together with chatbot responses.

## 1) Current backend visualization contracts

### `run_sox_scoping_engine`
- During prompt (`success=false`, `status=awaiting_input`, `step=materiality_benchmark`):
  - `data.visualizations.benchmark_comparison`
  - `data.benchmark_options` (dynamic from engine output)
  - `data.benchmark_preview_values` (dynamic from engine output)
- During success:
  - `data.visualizations.benchmark_comparison`
  - `data.visualizations.scoping_summary`

### `run_test_of_design` (TOD via `rcm_tester.py`)
- `data.visualizations.tod_result_summary`
- `data.visualizations.tod_deficiency_distribution`
- `data.visualizations.tod_confidence_distribution`
- `data.visualizations.tod_process_hierarchy`
- `data.visualizations.tod_control_results`

### `run_test_of_effectiveness` (TOE)
- `data.visualizations.effectiveness_summary`
- `data.visualizations.control_results`
- `data.visualizations.deficiency_distribution`
- `data.visualizations.deviation_rates`

---

## 2) Important behavior for chatbot mapping

Do **not** treat all `success=false` responses as hard failures.

Some tool steps intentionally return:
- `success=false`
- `data.status="awaiting_input"`
- `data.question`
- and sometimes `data.visualizations`

These are valid assistant prompts and should render in chat + charts.

---

## 3) Minimal TypeScript mapper

```ts
export type VisualizationData = {
  type: string;
  title: string;
  data: Array<Record<string, any>>;
  [key: string]: any;
};

export type ToolResponse = {
  success: boolean;
  data: {
    status?: string;
    step?: string;
    question?: string;
    visualizations?: Record<string, VisualizationData>;
    [key: string]: any;
  };
  summary?: string;
  error?: string;
};

export type ChatItem = {
  role: "assistant" | "tool";
  text: string;
  metadata?: Record<string, any>;
};

export function mapToolResponseToChat(resp: ToolResponse): ChatItem[] {
  const isPrompt = resp.data?.status === "awaiting_input";

  if (!resp.success && !isPrompt) {
    return [{ role: "assistant", text: resp.error || "Tool failed." }];
  }

  const out: ChatItem[] = [];

  if (resp.data?.question) {
    out.push({
      role: "assistant",
      text: resp.data.question,
      metadata: { step: resp.data.step, status: resp.data.status },
    });
  } else if (resp.summary) {
    out.push({ role: "assistant", text: resp.summary });
  }

  if (resp.data?.visualizations) {
    out.push({
      role: "tool",
      text: "visualizations",
      metadata: { visualizations: resp.data.visualizations, step: resp.data.step },
    });
  }

  return out;
}
```

---

## 4) Minimal renderer dispatch

```ts
function renderVisualization(key: string, viz: VisualizationData) {
  switch (viz.type) {
    case "pie_chart":
      return renderPie(viz);
    case "stacked_bar_chart":
      return renderStackedBar(viz);
    case "horizontal_bar_chart":
      return renderHorizontalBar(viz);
    case "treemap":
      return renderTreemapOrFallback(viz);
    case "bar_chart":
    default:
      return renderBar(viz);
  }
}
```

---

## 5) Chat + chart UX sequence (recommended)

1. Render assistant text (`question` or `summary`)
2. Immediately render visualization cards (if present)
3. For prompt states (`awaiting_input`), show input controls under the charts

This is especially important for benchmark choice, where chart appears **before** user selects benchmark.

---

## 6) Installation checklist (frontend app)

If using Recharts + React + TS:
- `npm i react react-dom recharts chart.js`
- `npm i -D @types/react @types/react-dom`

---

## 7) Verification summary

- Backend tool files are healthy and aligned:
  - `agent/tools/sox_scoping_engine.py` ✅
  - `agent/tools/test_of_design.py` ✅
  - `agent/tools/test_of_effectiveness.py` ✅
- Mock backend flow sequence passes ✅
- Benchmark labels/values are sourced from engine output with compatibility fallback ✅

