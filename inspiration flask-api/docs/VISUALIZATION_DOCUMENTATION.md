# Visualization Metadata Documentation

## Overview

The agent tools now include rich visualization metadata in their responses to enable frontend rendering of charts and graphs. This document describes the visualization structures added to each tool.

---

## 1. SOX Scoping Engine (`run_sox_scoping`)

### Purpose
Visualize materiality benchmark selection and account scoping results.

### Visualizations Included

#### 1.1 Benchmark Comparison (Bar Chart)
**Purpose**: Display computed values for all materiality benchmarks to help users understand the relative magnitude of each benchmark option.

**Structure**:
```json
{
  "type": "bar_chart",
  "title": "Materiality Benchmark Values",
  "data": [
    {
      "category": "Revenue",
      "value": 50000000.0,
      "selected": true
    },
    {
      "category": "Assets", 
      "value": 125000000.0,
      "selected": false
    },
    {
      "category": "EBITDA",
      "value": 8500000.0,
      "selected": false
    }
  ],
  "selected_benchmark": "Revenue",
  "selected_value": 50000000.0,
  "materiality_percentage": 1.5,
  "threshold": 750000.0
}
```

**Fields**:
- `category`: Benchmark name (Revenue, Assets, EBITDA, PBT, Net Income, Net Interest)
- `value`: Absolute computed value from trial balance parsing
- `selected`: Boolean indicating which benchmark was selected by user
- `selected_benchmark`: Name of the selected benchmark
- `selected_value`: Value of the selected benchmark
- `materiality_percentage`: User-provided materiality percentage
- `threshold`: Calculated materiality threshold (selected_value * materiality_pct / 100)

**Frontend Rendering Recommendation**: 
- Bar chart with benchmarks on X-axis, values on Y-axis
- Highlight selected benchmark with different color
- Display threshold as horizontal line or annotation
- Format values as currency with appropriate scaling (millions, thousands)

---

#### 1.2 Scoping Summary (Pie Chart)
**Purpose**: Show proportion of accounts scoped in vs out based on materiality analysis.

**Structure**:
```json
{
  "type": "pie_chart",
  "title": "Account Scoping Results",
  "data": [
    {
      "category": "In-Scope",
      "value": 45,
      "color": "#ef4444"
    },
    {
      "category": "Out-of-Scope",
      "value": 55,
      "color": "#10b981"
    }
  ]
}
```

**Fields**:
- `category`: "In-Scope" or "Out-of-Scope"
- `value`: Count of accounts in each category
- `color`: Suggested hex color (red for in-scope indicating risk, green for out-of-scope)

**Frontend Rendering Recommendation**:
- Pie chart or donut chart
- Show percentages alongside raw counts
- Use suggested colors or map to theme colors

---

## 2. Control Assessment (`run_control_assessment`)

### Purpose
Visualize RCM control documentation status, match quality, and process distribution.

### Visualizations Included

#### 2.1 Control Status (Pie Chart)
**Purpose**: Show how many controls are documented, not documented, or partially documented in policies/SOPs.

**Structure**:
```json
{
  "type": "pie_chart",
  "title": "Control Documentation Status",
  "data": [
    {
      "category": "Documented",
      "value": 30,
      "color": "#10b981"
    },
    {
      "category": "Not Documented",
      "value": 10,
      "color": "#ef4444"
    },
    {
      "category": "Partial",
      "value": 5,
      "color": "#f59e0b"
    }
  ]
}
```

**Fields**:
- `category`: Documentation status (Documented, Not Documented, Partial)
- `value`: Count of controls in each status
- `color`: Suggested colors (green=good, red=bad, orange=warning)

**Frontend Rendering Recommendation**:
- Pie or donut chart
- Display count and percentage for each segment
- Use traffic light colors (green/amber/red)

---

#### 2.2 Match Quality (Bar Chart)
**Purpose**: Show distribution of control-to-SOP match percentages.

**Structure**:
```json
{
  "type": "bar_chart",
  "title": "Control Match Quality Distribution",
  "data": [
    {
      "category": "High Match (>80%)",
      "value": 25
    },
    {
      "category": "Medium Match (50-80%)",
      "value": 10
    },
    {
      "category": "Low Match (<50%)",
      "value": 8
    },
    {
      "category": "No Match",
      "value": 2
    }
  ]
}
```

**Fields**:
- `category`: Match quality range
- `value`: Count of controls in each quality band

**Frontend Rendering Recommendation**:
- Vertical bar chart
- Color-code bars by quality (green for high, yellow for medium, red for low)
- Add data labels on bars

---

#### 2.3 Process Hierarchy (Treemap)
**Purpose**: Show how controls are distributed across processes and subprocesses.

**Structure**:
```json
{
  "type": "treemap",
  "title": "Controls by Process/Subprocess",
  "data": [
    {
      "name": "Procure to Pay/Vendor Onboarding",
      "value": 12
    },
    {
      "name": "Record to Report/Journal Entry",
      "value": 8
    },
    {
      "name": "IT General Controls/Access Management",
      "value": 6
    }
  ]
}
```

**Fields**:
- `name`: Process/Subprocess path (format: "Process/Subprocess" or just "Process")
- `value`: Count of controls in that process/subprocess

**Frontend Rendering Recommendation**:
- Treemap or hierarchical bubble chart
- Size of each block proportional to control count
- Consider grouping by parent process if rendering as hierarchy
- Show top 15 processes to avoid clutter
- On hover/click, show control count and percentage

---

## 3. Test of Effectiveness (`run_test_of_effectiveness`)

### Purpose
Visualize TOE testing results, control effectiveness, deficiencies, and deviation rates.

### Visualizations Included

#### 3.1 Effectiveness Summary (Pie Chart)
**Purpose**: High-level summary of operating effectiveness across all tested controls.

**Structure**:
```json
{
  "type": "pie_chart",
  "title": "Operating Effectiveness Results",
  "data": [
    {
      "category": "Effective",
      "value": 35,
      "color": "#10b981"
    },
    {
      "category": "Effective with Exceptions",
      "value": 8,
      "color": "#f59e0b"
    },
    {
      "category": "Not Effective",
      "value": 2,
      "color": "#ef4444"
    }
  ]
}
```

**Fields**:
- `category`: Effectiveness classification (Effective, Effective with Exceptions, Not Effective)
- `value`: Count of controls in each classification
- `color`: Suggested colors (green/amber/red)

**Frontend Rendering Recommendation**:
- Donut chart with center summary
- Display percentage and count for each segment
- Use audit-standard traffic light colors

---

#### 3.2 Control Results (Stacked Bar Chart)
**Purpose**: Show pass/fail distribution for each control's sample testing.

**Structure**:
```json
{
  "type": "stacked_bar_chart",
  "title": "Test Results by Control",
  "data": [
    {
      "control_id": "C-P2P-001",
      "passed": 23,
      "failed": 2,
      "deviation_rate": 8.0
    },
    {
      "control_id": "C-P2P-002",
      "passed": 25,
      "failed": 0,
      "deviation_rate": 0.0
    }
  ],
  "x_axis": "control_id",
  "y_axis": "samples",
  "stacks": ["passed", "failed"]
}
```

**Fields**:
- `control_id`: Control identifier
- `passed`: Count of samples that passed
- `failed`: Count of samples that failed
- `deviation_rate`: Percentage deviation (failed / total * 100)
- `x_axis`, `y_axis`, `stacks`: Chart configuration hints

**Frontend Rendering Recommendation**:
- Horizontal or vertical stacked bar chart
- Green for passed, red for failed
- Show top 20 controls (data is pre-filtered)
- On hover, show deviation rate percentage
- Consider sorting by deviation rate descending

---

#### 3.3 Deficiency Distribution (Bar Chart)
**Purpose**: Classify controls by deficiency severity.

**Structure**:
```json
{
  "type": "bar_chart",
  "title": "Deficiency Classification",
  "data": [
    {
      "category": "No Deficiency",
      "value": 35
    },
    {
      "category": "Minor Deficiency",
      "value": 8
    },
    {
      "category": "Significant Deficiency",
      "value": 2
    },
    {
      "category": "Material Weakness",
      "value": 0
    }
  ]
}
```

**Fields**:
- `category`: Deficiency type (No Deficiency, Minor Deficiency, Significant Deficiency, Material Weakness)
- `value`: Count of controls in each classification

**Frontend Rendering Recommendation**:
- Vertical bar chart
- Color gradient from green (no deficiency) to dark red (material weakness)
- Highlight material weaknesses if present (critical audit finding)
- Show counts and percentages

---

#### 3.4 Deviation Rates (Horizontal Bar Chart)
**Purpose**: Rank controls by deviation rate to highlight controls with highest failure rates.

**Structure**:
```json
{
  "type": "horizontal_bar_chart",
  "title": "Deviation Rates by Control",
  "data": [
    {
      "control_id": "C-P2P-005",
      "deviation_rate": 12.5
    },
    {
      "control_id": "C-R2R-012",
      "deviation_rate": 8.3
    },
    {
      "control_id": "C-P2P-001",
      "deviation_rate": 4.0
    }
  ]
}
```

**Fields**:
- `control_id`: Control identifier
- `deviation_rate`: Percentage of samples that failed (0-100)

**Frontend Rendering Recommendation**:
- Horizontal bar chart sorted by deviation rate descending
- Shows top 15 controls (pre-filtered)
- Color-code bars by severity:
  - 0-5%: Green (acceptable)
  - 5-10%: Yellow (warning)
  - >10%: Red (concerning)
- Display percentage with 1 decimal place
- Consider adding threshold markers (e.g., 5%, 10%)

---

## General Implementation Guidelines

### Backend Compatibility
All visualizations are added to the `data` dict in the `ToolResult` under a `visualizations` key:

```python
return ToolResult(
    success=True,
    data={
        # ... existing data fields ...
        "visualizations": {
            "viz_name": { ... },
            "another_viz": { ... },
        },
    },
    artifacts=[...],
    summary="...",
)
```

This ensures:
- ✅ No breaking changes to existing response structure
- ✅ Frontend can check for presence of `visualizations` key
- ✅ Backward compatible with older frontend versions
- ✅ Tool wrappers (tool_adapter.py, loop.py) handle dicts correctly
- ✅ JSON serializable (all data types are primitives)

### Frontend Integration Steps

1. **Check for visualizations**:
```javascript
if (toolResult.data.visualizations) {
  renderVisualizations(toolResult.data.visualizations);
}
```

2. **Iterate over visualization types**:
```javascript
Object.entries(visualizations).forEach(([vizName, vizConfig]) => {
  switch(vizConfig.type) {
    case 'bar_chart':
      renderBarChart(vizConfig);
      break;
    case 'pie_chart':
      renderPieChart(vizConfig);
      break;
    // ... etc
  }
});
```

3. **Recommended Chart Libraries**:
- **Chart.js**: Simple, lightweight, supports all chart types
- **Recharts**: React-specific, composable components
- **D3.js**: Maximum flexibility, steeper learning curve
- **Apache ECharts**: Enterprise-grade, rich interactions

### Data Validation
All visualizations pass validation tests for:
- ✅ Required fields (type, title, data)
- ✅ Data is an array of objects
- ✅ JSON serializable
- ✅ No circular references
- ✅ Numeric values are floats/ints, not strings

### Performance Considerations
- Data is pre-aggregated in backend (no raw sample data)
- Top-N filtering applied (e.g., top 20 controls, top 15 processes)
- Visualization metadata typically adds <10KB to response
- No additional API calls needed for rendering

---

## Example: Full Tool Response with Visualizations

```json
{
  "success": true,
  "data": {
    "controls_evaluated": 45,
    "effective": 35,
    "effective_with_exceptions": 8,
    "not_effective": 2,
    "output_excel": "/path/to/output.xlsx",
    "visualizations": {
      "effectiveness_summary": {
        "type": "pie_chart",
        "title": "Operating Effectiveness Results",
        "data": [ ... ]
      },
      "control_results": {
        "type": "stacked_bar_chart",
        "title": "Test Results by Control",
        "data": [ ... ]
      }
    }
  },
  "artifacts": ["/path/to/output.xlsx"],
  "summary": "TOE: 35 Effective, 8 Exceptions, 2 Not Effective out of 45"
}
```

---

## Testing

Run the validation test:
```bash
python flask-api/test_visualization_metadata.py
```

This validates:
- Proper structure for all visualization types
- JSON serializability
- Required field presence
- Data format consistency

---

## Future Enhancements

Potential additions for future iterations:

1. **Time-series visualizations**: Track control effectiveness over multiple audit periods
2. **Heatmaps**: Show risk levels across process areas
3. **Network graphs**: Visualize control dependencies and process flows
4. **Drill-down support**: Add metadata for linking to detailed views
5. **Export metadata**: Include SVG/PNG export hints for reporting
6. **Interactive filters**: Metadata for filtering/grouping options
7. **Comparison views**: Side-by-side period comparisons

---

## Support

For questions or issues with visualizations:
1. Check `test_visualization_metadata.py` for expected structure
2. Review tool source code:
   - `agent/tools/sox_scoping_engine.py`
   - `agent/tools/control_assessment.py`
   - `agent/tools/test_of_effectiveness.py`
3. Validate JSON structure with test script
4. Ensure frontend chart library supports required chart types
