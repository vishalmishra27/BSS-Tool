# 📊 Visualization Quick Reference

## 🎯 Quick Start

All agent tools now return visualization metadata in responses. Frontend can render these as charts without additional API calls.

### Check for Visualizations
```python
# Backend
if "visualizations" in tool_result.data:
    visualizations = tool_result.data["visualizations"]
```

```javascript
// Frontend
if (response.data.visualizations) {
  renderCharts(response.data.visualizations);
}
```

---

## 📈 Chart Types at a Glance

| Chart Type | Use Case | Tools Using It |
|------------|----------|----------------|
| `bar_chart` | Compare categories | SOX Scoping, Control Assessment |
| `pie_chart` | Show proportions | All tools |
| `stacked_bar_chart` | Multi-part comparisons | TOE |
| `horizontal_bar_chart` | Rankings | TOE |
| `treemap` | Hierarchical data | Control Assessment |

---

## 🔧 Tool-Specific Visualizations

### 1️⃣ SOX Scoping Engine

**Tool:** `run_sox_scoping`

**Visualizations:**
- 📊 **benchmark_comparison** (bar_chart)
  - Shows: All benchmark values (Revenue, Assets, EBITDA, etc.)
  - Highlights: Selected benchmark
  - Includes: Materiality threshold
  
- 🥧 **scoping_summary** (pie_chart)
  - Shows: In-scope vs out-of-scope accounts
  - Colors: Red (in-scope), Green (out-of-scope)

**Sample Response:**
```json
{
  "visualizations": {
    "benchmark_comparison": {
      "type": "bar_chart",
      "data": [{"category": "Revenue", "value": 50000000, "selected": true}],
      "threshold": 750000
    },
    "scoping_summary": {
      "type": "pie_chart",
      "data": [{"category": "In-Scope", "value": 45, "color": "#ef4444"}]
    }
  }
}
```

---

### 2️⃣ Control Assessment

**Tool:** `run_control_assessment`

**Visualizations:**
- 🥧 **control_status** (pie_chart)
  - Shows: Documented, Not Documented, Partial
  - Colors: Green, Red, Amber
  
- 📊 **match_quality** (bar_chart)
  - Shows: High Match (>80%), Medium (50-80%), Low (<50%), No Match
  - Purpose: Control-to-SOP match quality
  
- 🗂️ **process_hierarchy** (treemap)
  - Shows: Controls grouped by Process/Subprocess
  - Displays: Top 15 processes

**Sample Response:**
```json
{
  "visualizations": {
    "control_status": {
      "type": "pie_chart",
      "data": [
        {"category": "Documented", "value": 30, "color": "#10b981"},
        {"category": "Not Documented", "value": 10, "color": "#ef4444"}
      ]
    },
    "match_quality": {
      "type": "bar_chart",
      "data": [{"category": "High Match (>80%)", "value": 25}]
    },
    "process_hierarchy": {
      "type": "treemap",
      "data": [{"name": "Procure to Pay/Vendor Onboarding", "value": 12}]
    }
  }
}
```

---

### 3️⃣ Test of Effectiveness (TOE)

**Tool:** `run_test_of_effectiveness`

**Visualizations:**
- 🥧 **effectiveness_summary** (pie_chart)
  - Shows: Effective, Effective with Exceptions, Not Effective
  - Colors: Green, Amber, Red
  
- 📊 **control_results** (stacked_bar_chart)
  - Shows: Passed vs Failed samples per control
  - Displays: Top 20 controls
  - Includes: Deviation rate
  
- 📊 **deficiency_distribution** (bar_chart)
  - Shows: No Deficiency, Minor, Significant, Material Weakness
  - Colors: Green → Dark Red gradient
  
- ↔️ **deviation_rates** (horizontal_bar_chart)
  - Shows: Top 15 controls by deviation rate
  - Colors: Green (<5%), Amber (5-10%), Red (>10%)

**Sample Response:**
```json
{
  "visualizations": {
    "effectiveness_summary": {
      "type": "pie_chart",
      "data": [
        {"category": "Effective", "value": 35, "color": "#10b981"},
        {"category": "Not Effective", "value": 2, "color": "#ef4444"}
      ]
    },
    "control_results": {
      "type": "stacked_bar_chart",
      "data": [{"control_id": "C-001", "passed": 23, "failed": 2}]
    },
    "deviation_rates": {
      "type": "horizontal_bar_chart",
      "data": [{"control_id": "C-005", "deviation_rate": 12.5}]
    }
  }
}
```

---

## 🎨 Standard Color Palette

### Status Colors
- ✅ **Success/Good**: `#10b981` (green)
- ⚠️ **Warning/Caution**: `#f59e0b` (amber)
- ❌ **Error/Risk**: `#ef4444` (red)
- ℹ️ **Info/Neutral**: `#3b82f6` (blue)
- 🟣 **Other**: `#8b5cf6` (purple)

### Usage
```javascript
const STATUS_COLORS = {
  success: '#10b981',
  warning: '#f59e0b',
  error: '#ef4444',
  info: '#3b82f6',
  other: '#8b5cf6',
};
```

---

## 📦 Required Frontend Packages

### Option 1: Recharts (React)
```bash
npm install recharts
npm install --save-dev @types/recharts  # TypeScript
```

### Option 2: Chart.js (Any framework)
```bash
npm install chart.js
npm install react-chartjs-2  # If using React
```

### Option 3: Apache ECharts
```bash
npm install echarts
```

---

## 💻 Minimal Implementation (React)

```tsx
import React from 'react';
import { PieChart, Pie, Cell, BarChart, Bar, XAxis, YAxis } from 'recharts';

const ChartRenderer = ({ visualizations }) => {
  return (
    <div className="space-y-6">
      {Object.entries(visualizations).map(([key, viz]) => {
        if (viz.type === 'pie_chart') {
          return (
            <div key={key}>
              <h3>{viz.title}</h3>
              <PieChart width={400} height={300}>
                <Pie
                  data={viz.data}
                  dataKey="value"
                  nameKey="category"
                  cx="50%"
                  cy="50%"
                  outerRadius={80}
                >
                  {viz.data.map((entry, i) => (
                    <Cell key={i} fill={entry.color || '#3b82f6'} />
                  ))}
                </Pie>
              </PieChart>
            </div>
          );
        }
        
        if (viz.type === 'bar_chart') {
          return (
            <div key={key}>
              <h3>{viz.title}</h3>
              <BarChart width={600} height={300} data={viz.data}>
                <XAxis dataKey="category" />
                <YAxis />
                <Bar dataKey="value" fill="#3b82f6" />
              </BarChart>
            </div>
          );
        }
        
        return null;
      })}
    </div>
  );
};
```

---

## 🧪 Testing

### Run Validation Test
```bash
cd flask-api
python test_visualization_metadata.py
```

**Expected Output:**
```
✅ All visualization tests passed!
Tests passed: 4/4
```

---

## 📚 Full Documentation

- **Complete Spec**: [VISUALIZATION_DOCUMENTATION.md](VISUALIZATION_DOCUMENTATION.md)
- **Integration Guide**: [FRONTEND_INTEGRATION_GUIDE.tsx](FRONTEND_INTEGRATION_GUIDE.tsx)
- **Summary**: [VISUALIZATION_SUMMARY.md](VISUALIZATION_SUMMARY.md)

---

## 🔍 Debugging

### Visualization Not Showing?
1. ✅ Check `success: true` in response
2. ✅ Verify `visualizations` key exists
3. ✅ Log visualization data: `console.log(response.data.visualizations)`
4. ✅ Check browser console for errors

### Data Looks Wrong?
1. Run validation test: `python test_visualization_metadata.py`
2. Check data types (numbers not strings)
3. Verify color format: `#RRGGBB`
4. Check array has items: `data.length > 0`

---

## 🚀 Performance Tips

- ✅ Charts render client-side (no backend load)
- ✅ Data pre-aggregated (top N items)
- ✅ Response size: ~5-10KB increase
- ✅ Use React.memo for chart components
- ✅ Lazy load chart library

---

## 📞 Support

**Questions?**
1. Review [VISUALIZATION_DOCUMENTATION.md](VISUALIZATION_DOCUMENTATION.md)
2. Check [FRONTEND_INTEGRATION_GUIDE.tsx](FRONTEND_INTEGRATION_GUIDE.tsx) examples
3. Run validation test: `test_visualization_metadata.py`
4. Review tool source code

---

**Last Updated:** March 6, 2026
**Version:** 1.0.0
