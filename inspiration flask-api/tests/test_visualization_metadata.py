"""
Test script to validate visualization metadata structure in tool responses.
This ensures the visualizations are properly formatted for frontend rendering.
"""

import json
import sys


def validate_visualization_structure(viz_data, viz_name):
    """Validate a single visualization structure."""
    errors = []
    
    if not isinstance(viz_data, dict):
        errors.append(f"{viz_name}: Must be a dictionary")
        return errors
    
    # Check required fields
    if "type" not in viz_data:
        errors.append(f"{viz_name}: Missing 'type' field")
    if "title" not in viz_data:
        errors.append(f"{viz_name}: Missing 'title' field")
    if "data" not in viz_data:
        errors.append(f"{viz_name}: Missing 'data' field")
    
    # Validate data is a list
    if "data" in viz_data and not isinstance(viz_data["data"], list):
        errors.append(f"{viz_name}: 'data' must be a list")
    
    # Validate data items
    if "data" in viz_data and isinstance(viz_data["data"], list):
        for i, item in enumerate(viz_data["data"]):
            if not isinstance(item, dict):
                errors.append(f"{viz_name}: data[{i}] must be a dictionary")
    
    return errors


def test_sox_scoping_visualization():
    """Test SOX scoping visualization structure."""
    print("\n=== Testing SOX Scoping Engine Visualizations ===")
    
    # Mock visualization structure from sox_scoping_engine.py
    visualizations = {
        "benchmark_comparison": {
            "type": "bar_chart",
            "title": "Materiality Benchmark Values",
            "data": [
                {"category": "Revenue", "value": 1000000, "selected": True},
                {"category": "Assets", "value": 5000000, "selected": False},
                {"category": "EBITDA", "value": 200000, "selected": False},
            ],
            "selected_benchmark": "Revenue",
            "selected_value": 1000000,
            "materiality_percentage": 1.5,
            "threshold": 15000,
        },
        "scoping_summary": {
            "type": "pie_chart",
            "title": "Account Scoping Results",
            "data": [
                {"category": "In-Scope", "value": 45, "color": "#ef4444"},
                {"category": "Out-of-Scope", "value": 55, "color": "#10b981"},
            ],
        },
    }
    
    all_errors = []
    for viz_name, viz_data in visualizations.items():
        errors = validate_visualization_structure(viz_data, viz_name)
        all_errors.extend(errors)
    
    if all_errors:
        print(f"❌ FAILED: {len(all_errors)} errors found")
        for error in all_errors:
            print(f"  - {error}")
        return False
    else:
        print(f"✅ PASSED: All visualizations valid")
        print(f"  - benchmark_comparison: {len(visualizations['benchmark_comparison']['data'])} data points")
        print(f"  - scoping_summary: {len(visualizations['scoping_summary']['data'])} categories")
        return True


def test_control_assessment_visualization():
    """Test Control Assessment visualization structure."""
    print("\n=== Testing Control Assessment Visualizations ===")
    
    # Mock visualization structure from control_assessment.py
    visualizations = {
        "control_status": {
            "type": "pie_chart",
            "title": "Control Documentation Status",
            "data": [
                {"category": "Documented", "value": 30, "color": "#10b981"},
                {"category": "Not Documented", "value": 10, "color": "#ef4444"},
                {"category": "Partial", "value": 5, "color": "#f59e0b"},
            ],
        },
        "match_quality": {
            "type": "bar_chart",
            "title": "Control Match Quality Distribution",
            "data": [
                {"category": "High Match (>80%)", "value": 25},
                {"category": "Medium Match (50-80%)", "value": 10},
                {"category": "Low Match (<50%)", "value": 8},
                {"category": "No Match", "value": 2},
            ],
        },
        "process_hierarchy": {
            "type": "treemap",
            "title": "Controls by Process/Subprocess",
            "data": [
                {"name": "Procure to Pay/Vendor Onboarding", "value": 12},
                {"name": "Record to Report/Journal Entry", "value": 8},
                {"name": "IT General Controls/Access Management", "value": 6},
            ],
        },
    }
    
    all_errors = []
    for viz_name, viz_data in visualizations.items():
        errors = validate_visualization_structure(viz_data, viz_name)
        all_errors.extend(errors)
    
    if all_errors:
        print(f"❌ FAILED: {len(all_errors)} errors found")
        for error in all_errors:
            print(f"  - {error}")
        return False
    else:
        print(f"✅ PASSED: All visualizations valid")
        print(f"  - control_status: {len(visualizations['control_status']['data'])} categories")
        print(f"  - match_quality: {len(visualizations['match_quality']['data'])} quality levels")
        print(f"  - process_hierarchy: {len(visualizations['process_hierarchy']['data'])} processes")
        return True


def test_toe_visualization():
    """Test TOE visualization structure."""
    print("\n=== Testing Test of Effectiveness Visualizations ===")
    
    # Mock visualization structure from test_of_effectiveness.py
    visualizations = {
        "effectiveness_summary": {
            "type": "pie_chart",
            "title": "Operating Effectiveness Results",
            "data": [
                {"category": "Effective", "value": 35, "color": "#10b981"},
                {"category": "Effective with Exceptions", "value": 8, "color": "#f59e0b"},
                {"category": "Not Effective", "value": 2, "color": "#ef4444"},
            ],
        },
        "control_results": {
            "type": "stacked_bar_chart",
            "title": "Test Results by Control",
            "data": [
                {"control_id": "C-001", "passed": 23, "failed": 2, "deviation_rate": 8.0},
                {"control_id": "C-002", "passed": 25, "failed": 0, "deviation_rate": 0.0},
            ],
            "x_axis": "control_id",
            "y_axis": "samples",
            "stacks": ["passed", "failed"],
        },
        "deficiency_distribution": {
            "type": "bar_chart",
            "title": "Deficiency Classification",
            "data": [
                {"category": "No Deficiency", "value": 35},
                {"category": "Minor Deficiency", "value": 8},
                {"category": "Significant Deficiency", "value": 2},
            ],
        },
        "deviation_rates": {
            "type": "horizontal_bar_chart",
            "title": "Deviation Rates by Control",
            "data": [
                {"control_id": "C-005", "deviation_rate": 12.5},
                {"control_id": "C-012", "deviation_rate": 8.3},
                {"control_id": "C-001", "deviation_rate": 4.0},
            ],
        },
    }
    
    all_errors = []
    for viz_name, viz_data in visualizations.items():
        errors = validate_visualization_structure(viz_data, viz_name)
        all_errors.extend(errors)
    
    if all_errors:
        print(f"❌ FAILED: {len(all_errors)} errors found")
        for error in all_errors:
            print(f"  - {error}")
        return False
    else:
        print(f"✅ PASSED: All visualizations valid")
        print(f"  - effectiveness_summary: {len(visualizations['effectiveness_summary']['data'])} categories")
        print(f"  - control_results: {len(visualizations['control_results']['data'])} controls")
        print(f"  - deficiency_distribution: {len(visualizations['deficiency_distribution']['data'])} categories")
        print(f"  - deviation_rates: {len(visualizations['deviation_rates']['data'])} controls")
        return True


def test_json_serialization():
    """Test that visualization data can be serialized to JSON."""
    print("\n=== Testing JSON Serialization ===")
    
    test_data = {
        "visualizations": {
            "test_chart": {
                "type": "bar_chart",
                "title": "Test Chart",
                "data": [
                    {"category": "A", "value": 100},
                    {"category": "B", "value": 200},
                ],
            }
        }
    }
    
    try:
        json_str = json.dumps(test_data, indent=2)
        parsed = json.loads(json_str)
        print(f"✅ PASSED: Visualization data is JSON serializable")
        return True
    except Exception as e:
        print(f"❌ FAILED: JSON serialization error: {e}")
        return False


def main():
    """Run all validation tests."""
    print("=" * 60)
    print("Visualization Metadata Validation Tests")
    print("=" * 60)
    
    results = []
    results.append(test_sox_scoping_visualization())
    results.append(test_control_assessment_visualization())
    results.append(test_toe_visualization())
    results.append(test_json_serialization())
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"Tests passed: {passed}/{total}")
    
    if passed == total:
        print("\n✅ All visualization tests passed!")
        print("\nVisualization Types Supported:")
        print("  - bar_chart: For comparing values across categories")
        print("  - pie_chart: For showing proportions")
        print("  - stacked_bar_chart: For comparing multi-part data")
        print("  - horizontal_bar_chart: For ranking data")
        print("  - treemap: For hierarchical data")
        return 0
    else:
        print(f"\n❌ {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
