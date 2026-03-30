import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';

const ActivityRow = ({ activity, lobData, styles, getProgressColor, getVarianceText, onHoverChange, isPopupHovered, mouseLeaveTimeout, setMouseLeaveTimeout }) => {
  // Removed unused handleMouseMove function

  return (
    <tr style={styles.tableRow}
      onMouseEnter={(e) => {
        e.currentTarget.style.background = '#f8fafc';
        e.currentTarget.style.transform = 'translateY(-1px)';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.background = '#ffffff';
        e.currentTarget.style.transform = 'translateY(0)';
      }}
    >
      <td style={{...styles.activityCell}} title={activity}>
        <span
          onClick={() => {
            onHoverChange(activity, { x: window.innerWidth / 2, y: window.innerHeight / 2 });
          }}
          style={{ cursor: 'pointer', display: 'inline-block', width: '100%' }}
        >
          {activity}
        </span>
      </td>
      {['Prepaid B2B', 'Prepaid B2C', 'Postpaid B2B', 'Postpaid B2C'].map((lob, lobIndex) => {
        const planned = lobData[lob]?.planned || 0;
        const actual = lobData[lob]?.actual || 0;
        const color = getProgressColor(planned, actual);

        return (
          <td key={`${activity}-${lob}`} style={{
            ...styles.tableCell,
            borderLeft: lobIndex > 0 ? '1px solid #f1f5f9' : 'none',
            borderRight: lobIndex === 3 ? 'none' : '1px solid #f1f5f9'
          }}>
            <div style={{
              ...styles.actualBox,
              backgroundColor: `${color}15`,
              border: `1px solid ${color}40`,
              color: color
            }}>
              {actual}%
            </div>
          </td>
        );
      })}
    </tr>
  );
};

const TransformationDashboardStatic = () => {
  const navigate = useNavigate();
  const [hoveredActivity, setHoveredActivity] = useState(null);
  const [popupPosition, setPopupPosition] = useState({ x: 0, y: 0 });
  const [workflowNodes, setWorkflowNodes] = useState([]);
  const [loading, setLoading] = useState(true);

  // Fetch workflow nodes and phases data from project milestones
  useEffect(() => {
    const fetchData = async () => {
      try {
        // Fetch workflow nodes
        const nodesResponse = await fetch('/api/workflow_nodes');
        if (!nodesResponse.ok) {
          throw new Error('Failed to fetch workflow nodes');
        }
        const nodesData = await nodesResponse.json();

        // Fetch all phases
        const phasesResponse = await fetch('/api/phases');
        let phasesData = [];
        if (phasesResponse.ok) {
          phasesData = await phasesResponse.json();
        }

        // Combine nodes with their phases
        const enrichedNodes = nodesData.map(node => {
          const nodePhases = phasesData.filter(phase =>
            phase['Phase id'] && phase['Phase id'].toString() === node.id.toString()
          );
          return {
            ...node,
            phases: nodePhases
          };
        });

        setWorkflowNodes(enrichedNodes);
        setLoading(false);
      } catch (error) {
        console.error('Error fetching data:', error);
        // Fallback to static data if API fails
        setWorkflowNodes([
          { id: 1, name: 'Initiation & Planning', status: 'completed', parameters: [], phases: [] },
          { id: 2, name: 'Requirements Analysis', status: 'current', parameters: [], phases: [] },
          { id: 3, name: 'Design & Architecture', status: 'pending', parameters: [], phases: [] },
          { id: 4, name: 'Development', status: 'pending', parameters: [], phases: [] },
          { id: 5, name: 'Testing & QA', status: 'pending', parameters: [], phases: [] }
        ]);
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  // State for checklist data
  const [checklistData, setChecklistData] = useState({});

  // Fetch checklist data for all phases
  useEffect(() => {
    const fetchChecklistData = async () => {
      if (workflowNodes.length === 0) return;

      const checklistPromises = workflowNodes.map(async (node) => {
        try {
          const response = await fetch(`/api/checklist/${node.id}`);
          if (response.ok) {
            const data = await response.json();
            return { phaseId: node.id, checklist: data };
          }
        } catch (error) {
          console.error(`Error fetching checklist for phase ${node.id}:`, error);
        }
        return { phaseId: node.id, checklist: [] };
      });

      const results = await Promise.all(checklistPromises);
      const checklistMap = {};
      results.forEach(result => {
        checklistMap[result.phaseId] = result.checklist;
      });
      setChecklistData(checklistMap);
    };

    fetchChecklistData();
  }, [workflowNodes]);

  // Calculate progress for a specific activity based on checklist completion
  const calculateActivityProgress = (activityName) => {
    const node = workflowNodes.find(n => n.name.toLowerCase() === activityName.toLowerCase());
    if (!node || !checklistData[node.id]) return 0;

    const checklist = checklistData[node.id];
    if (checklist.length === 0) return 0;

    const completedItems = checklist.filter(item => item.status === 'completed').length;
    return Math.round((completedItems / checklist.length) * 100);
  };

  // Map activities to workflow nodes and their phases
  const getActivityDetails = (activityName) => {
    // Try to find exact match first
    let node = workflowNodes.find(n => n.name.toLowerCase() === activityName.toLowerCase());

    // If no exact match, try partial matches
    if (!node) {
      const partialMatches = {
        "Process Documentation": "Requirements Analysis",
        "System Integration": "Design & Architecture",
        "User Training": "Development",
        "Testing & QA": "Testing & QA",
        "Initiation & Planning": "Initiation & Planning",
        "SRS Finalization": "Requirements Analysis",
        "Product Rationalization": "Design & Architecture",
        "Configuration Validation": "Development",
        "Data Cleanup & Preparation": "Testing & QA",
        "UAT Execution": "Testing & QA",
        "Trial / Dry Run Migrations": "Development",
        "Final Migration & Cutover": "Testing & QA"
      };

      const matchedNodeName = partialMatches[activityName];
      if (matchedNodeName) {
        node = workflowNodes.find(n => n.name.toLowerCase() === matchedNodeName.toLowerCase());
      }
    }

    if (node && node.phases && node.phases.length > 0) {
      const phase = node.phases[0]; // Use first phase for details
      const calculatedProgress = calculateActivityProgress(activityName);
      return {
        startDate: phase['Start Date'] || "2024-01-15",
        dueDate: phase['Due Date'] || "2024-12-15",
        assignee: phase['Assigned to'] || "Project Manager",
        status: phase['Status'] || node.status,
        id: node.id,
        progress: calculatedProgress,
        phaseName: phase['Phase Name'] || activityName
      };
    }

    if (node) {
      const calculatedProgress = calculateActivityProgress(activityName);
      return {
        startDate: "2024-01-15",
        dueDate: "2024-12-15",
        assignee: "Project Manager",
        status: node.status,
        id: node.id,
        progress: calculatedProgress,
        phaseName: activityName
      };
    }

    // Fallback for unmatched activities
    return {
      startDate: "2024-01-15",
      dueDate: "2024-12-15",
      assignee: "Unassigned",
      status: "pending",
      id: null,
      progress: 0,
      phaseName: activityName
    };
  };

  const getActivitySummaries = (activityName) => {
    const node = workflowNodes.find(n => n.name.toLowerCase() === activityName.toLowerCase()) ||
                 workflowNodes.find(n => activityName.toLowerCase().includes(n.name.toLowerCase()));

    if (node && checklistData[node.id] && checklistData[node.id].length > 0) {
      // Return actual checklist items from the phase with their real status
      return checklistData[node.id].map(item => ({
        text: item.item_text || item.text || 'Unnamed task',
        status: item.status || 'pending'
      }));
    }

    // If no checklist data, try to get from node parameters
    if (node && node.parameters && node.parameters.length > 0) {
      return node.parameters.map(param => ({
        text: `${param.key}: ${param.value}`,
        status: 'pending'
      }));
    }

    // Default summaries based on activity type
    const defaultSummaries = {
      "Process Documentation": [
        { text: "Document current business processes and workflows", status: "pending" },
        { text: "Create standard operating procedures (SOPs)", status: "pending" },
        { text: "Map process flows and identify improvement areas", status: "pending" },
        { text: "Establish documentation standards and templates", status: "pending" }
      ],
      "System Integration": [
        { text: "Integrate new systems with existing infrastructure", status: "pending" },
        { text: "Configure APIs and data exchange protocols", status: "pending" },
        { text: "Test system compatibility and performance", status: "pending" },
        { text: "Resolve integration issues and bugs", status: "pending" }
      ],
      "User Training": [
        { text: "Develop comprehensive training materials", status: "pending" },
        { text: "Conduct hands-on training sessions", status: "pending" },
        { text: "Create user guides and reference materials", status: "pending" },
        { text: "Evaluate training effectiveness and feedback", status: "pending" }
      ],
      "Testing & QA": [
        { text: "Execute comprehensive test plans and scenarios", status: "pending" },
        { text: "Perform functional and regression testing", status: "pending" },
        { text: "Identify and document defects and issues", status: "pending" },
        { text: "Validate system performance and reliability", status: "pending" }
      ],
      "Initiation & Planning": [
        { text: "Define migration scope (systems, modules, products)", status: "pending" },
        { text: "Establish governance structure and key stakeholders", status: "pending" },
        { text: "Set project timelines and cutover windows", status: "pending" },
        { text: "Assign roles, responsibilities, and tool access", status: "pending" },
        { text: "Prepare migration strategy and high-level plan", status: "pending" }
      ]
    };

    return defaultSummaries[activityName] || [
      { text: "Activity details not available", status: "pending" },
      { text: "Please check project milestones for more information", status: "pending" }
    ];
  };

  // Get activity details for the hovered activity
  const getHoveredActivityDetails = (activityName) => {
    if (!activityName) return null;
    return getActivityDetails(activityName);
  };

  const getHoveredActivitySummaries = (activityName) => {
    if (!activityName) return [];
    return getActivitySummaries(activityName);
  };

  const handleHoverChange = (activity, position) => {
    const trimmedActivity = activity ? activity.trim().toLowerCase() : null;
    console.log('Hovered Activity:', trimmedActivity); // Log the hovered activity
    setHoveredActivity(trimmedActivity);
    if (position) {
      setPopupPosition(position);
    }
  };

  const [isPopupHovered, setIsPopupHovered] = useState(true);
  const [mouseLeaveTimeout, setMouseLeaveTimeout] = useState(1000);

  // Static data instead of API calls
  const overview = {
    project_name: "Digital Transformation Initiative",
    start_date: "2024-01-15",
    report_date: "2024-12-15",
    planned_progress: 85,
    actual_progress: 78,
    variance: -7
  };

  // Generate activities data based on workflow nodes
  const activities = workflowNodes.map(node => ({
    activity_name: node.name,
    lob: "Prepaid B2C", // Default LOB for the data
    planned_progress: 100, // Assuming 100% planned for each activity
    actual_progress: calculateActivityProgress(node.name)
  }));

  // Calculate LOB completion data based on activities
  const lobCompletions = ['Prepaid B2B', 'Prepaid B2C', 'Postpaid B2B', 'Postpaid B2C'].map(lobName => {
    const lobActivities = activities.filter(act => act.lob === lobName);
    if (lobActivities.length === 0) {
      return { lobName, completion: 0 };
    }

    const totalProgress = lobActivities.reduce((sum, act) => sum + act.actual_progress, 0);
    const averageCompletion = Math.round(totalProgress / lobActivities.length);

    return { lobName, completion: averageCompletion };
  });

  // Generate attention areas from pending checklist items
  const attentionAreas = Object.entries(checklistData).flatMap(([phaseId, checklist]) => {
    const node = workflowNodes.find(n => n.id.toString() === phaseId.toString());
    if (!node) return [];

    const pendingItems = checklist.filter(item => item.status !== 'completed');
    return pendingItems.slice(0, 2).map(item => ({ // Limit to 2 items per phase to avoid too many
      section: node.name,
      lob: "Prepaid B2C", // Default LOB
      notes: item.item_text || item.text || 'Task details not available'
    }));
  }).slice(0, 4); // Limit to 4 total attention areas

  const getProgressColor = (planned, actual) => {
    if (!planned || !actual) return '#6b7280';
    const variance = ((actual - planned) / planned) * 100;
    if (variance >= -5) return '#16a34a';
    if (variance >= -15) return '#f59e0b';
    return '#ef4444';
  };

  const getKpiColor = (value, threshold = 80) => {
    if (value >= threshold) return '#16a34a';
    if (value >= 60) return '#f59e0b';
    return '#ef4444';
  };

  const getVarianceText = (planned, actual) => {
    if (!planned || !actual) return '';
    const variance = actual - planned;
    const prefix = variance >= 0 ? '+' : '';
    return `${prefix}${variance}%`;
  };

  const styles = {
    container: {
      height: '100vh',
      background: 'linear-gradient(135deg, #f8fafc 0%, #e2e8f0 50%, #cbd5e1 100%)',
      color: '#1e293b',
      padding: '1.5rem',
      fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
      overflowX: 'hidden',
      overflowY: 'auto',
    },
    header: {
      textAlign: 'center',
      marginBottom: '1.5rem',
    },
    title: {
      fontSize: '2.2rem',
      fontWeight: '700',
      marginBottom: '0.5rem',
      color: '#1e40af',
      textShadow: '0 2px 4px rgba(30, 64, 175, 0.1)',
    },
    subtitle: {
      fontSize: '1.1rem',
      color: '#3b82f6',
      marginBottom: '1.5rem',
      fontWeight: '500',
    },
    statsGrid: {
      display: 'grid',
      gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
      gap: '1rem',
      marginBottom: '1.5rem',
    },
    kpiCard: {
      background: 'linear-gradient(135deg, #1e40af 0%, #3b82f6 100%)',
      borderRadius: '12px',
      padding: '1.5rem',
      marginBottom: '1.5rem',
      boxShadow: '0 4px 20px rgba(30, 64, 175, 0.3)',
      border: '1px solid #1e40af',
      transition: 'transform 0.3s ease, box-shadow 0.3s ease',
      color: '#ffffff',
      position: 'relative',
      overflow: 'hidden',
      minHeight: '120px',
      display: 'flex',
      flexDirection: 'column',
      justifyContent: 'flex-start',
    },
    kpiCardHover: {
      transform: 'translateY(-3px)',
      boxShadow: '0 8px 30px rgba(30, 64, 175, 0.4)',
    },
    kpiCardTitle: {
      fontSize: '1.1rem',
      fontWeight: '600',
      marginBottom: '0.5rem',
      color: '#ffffff',
      opacity: '0.9',
    },
    kpiStatNumber: {
      fontSize: '2.2rem',
      fontWeight: '700',
      color: '#ffffff',
      marginBottom: '0.25rem',
      textShadow: '0 2px 4px rgba(0, 0, 0, 0.2)',
      wordBreak: 'break-word',
      lineHeight: '1.2',
    },
    kpiStatLabel: {
      fontSize: '0.85rem',
      color: '#e0f2fe',
      opacity: '0.9',
      wordBreak: 'break-word',
    },
    kpiMiniStats: {
      display: 'flex',
      justifyContent: 'space-between',
      marginTop: '0.5rem',
      gap: '0.5rem',
    },
    kpiMiniStat: {
      textAlign: 'center',
      flex: 1,
    },
    kpiMiniStatNumber: {
      fontSize: '1rem',
      fontWeight: '600',
      marginBottom: '0.25rem',
      color: '#ffffff',
    },
    kpiMiniStatLabel: {
      fontSize: '0.7rem',
      color: '#bfdbfe',
      opacity: '0.9',
    },
    card: {
      background: '#ffffff',
      borderRadius: '12px',
      padding: '1.5rem',
      marginBottom: '1.5rem',
      boxShadow: '0 2px 12px rgba(30, 64, 175, 0.08)',
      border: '1px solid #e2e8f0',
      transition: 'transform 0.3s ease, box-shadow 0.3s ease',
      overflow: 'hidden',
    },
    cardTitle: {
      fontSize: '1.4rem',
      fontWeight: '600',
      marginBottom: '1rem',
      color: '#1e40af',
      borderBottom: '2px solid #3b82f6',
      paddingBottom: '0.5rem',
    },
    tableContainer: {
      overflowX: 'auto',
      borderRadius: '12px',
      boxShadow: '0 2px 12px rgba(30, 64, 175, 0.08)',
      border: '1px solid #e2e8f0',
    },
    table: {
      width: '100%',
      borderCollapse: 'separate',
      borderSpacing: 0,
      background: '#ffffff',
      minWidth: '1000px',
    },
    tableHeader: {
      background: 'linear-gradient(135deg, #1e40af, #3b82f6)',
      color: '#ffffff',
    },
    tableHeaderCell: {
      padding: '1rem 0.75rem',
      textAlign: 'center',
      fontWeight: '600',
      fontSize: '0.9rem',
      color: '#ffffff',
      borderBottom: '2px solid #1e40af',
      whiteSpace: 'normal',
      wordBreak: 'break-word',
      lineHeight: '1.2',
    },
    activityHeaderCell: {
      padding: '1rem 0.75rem',
      textAlign: 'left',
      fontWeight: '600',
      fontSize: '0.9rem',
      color: '#ffffff',
      borderBottom: '2px solid #1e40af',
      whiteSpace: 'normal',
      wordBreak: 'break-word',
      lineHeight: '1.2',
      borderTopLeftRadius: '12px',
    },
    tableRow: {
      background: '#ffffff',
      borderBottom: '1px solid #e2e8f0',
      transition: 'all 0.3s ease',
      cursor: 'default',
    },
    tableCell: {
      padding: '0.75rem 0.5rem',
      fontSize: '0.85rem',
      color: '#374151',
      verticalAlign: 'middle',
      textAlign: 'center',
      whiteSpace: 'normal',
      wordBreak: 'break-word',
      lineHeight: '1.3',
      fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
    },
    activityCell: {
      textAlign: 'left',
      fontWeight: '600',
      minWidth: '150px',
      maxWidth: '180px',
      color: '#1e293b',
      paddingLeft: '0.75rem',
      whiteSpace: 'normal',
      wordBreak: 'break-word',
      lineHeight: '1.3',
      fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
    },
    lobHeaderGroup: {
      background: 'linear-gradient(135deg, #1e40af, #3b82f6)',
      color: '#ffffff',
      padding: '1rem 0.5rem',
      fontWeight: '700',
      fontSize: '0.9rem',
      textAlign: 'center',
      borderRight: '2px solid rgba(255,255,255,0.3)',
      whiteSpace: 'normal',
      wordBreak: 'break-word',
      lineHeight: '1.2',
    },
    lobHeaderGroupLast: {
      background: 'linear-gradient(135deg, #1e40af, #3b82f6)',
      color: '#ffffff',
      padding: '1rem 0.5rem',
      fontWeight: '700',
      fontSize: '0.9rem',
      textAlign: 'center',
      whiteSpace: 'normal',
      wordBreak: 'break-word',
      lineHeight: '1.2',
    },
    subHeaderRow: {
      background: '#f8fafc',
      borderBottom: '2px solid #e2e8f0',
    },
    subHeaderCell: {
      padding: '0.75rem 0.5rem',
      fontSize: '0.8rem',
      fontWeight: '600',
      color: '#1e40af',
      textAlign: 'center',
      borderRight: '1px solid #e2e8f0',
      backgroundColor: '#f8fafc',
      whiteSpace: 'normal',
      wordBreak: 'break-word',
      lineHeight: '1.2',
      fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
    },
    subHeaderCellFirst: {
      padding: '0.75rem 0.5rem',
      fontSize: '0.8rem',
      fontWeight: '600',
      color: '#1e40af',
      textAlign: 'left',
      paddingLeft: '0.75rem',
      borderRight: '1px solid #e2e8f0',
      backgroundColor: '#f8fafc',
      whiteSpace: 'normal',
      wordBreak: 'break-word',
      lineHeight: '1.2',
      fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
    },
    subHeaderCellLast: {
      padding: '0.75rem 0.5rem',
      fontSize: '0.8rem',
      fontWeight: '600',
      color: '#1e40af',
      textAlign: 'center',
      backgroundColor: '#f8fafc',
      whiteSpace: 'normal',
      wordBreak: 'break-word',
      lineHeight: '1.2',
      fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
    },
    plannedBox: {
      backgroundColor: '#dbeafe',
      border: '1px solid #3b82f6',
      borderRadius: '6px',
      padding: '0.4rem 0.5rem',
      fontSize: '0.8rem',
      fontWeight: '600',
      color: '#1e40af',
      minWidth: '40px',
      display: 'inline-block',
    },
    actualBox: {
      borderRadius: '6px',
      padding: '0.4rem 0.5rem',
      fontSize: '0.8rem',
      fontWeight: '700',
      minWidth: '40px',
      display: 'inline-block',
    },
    varianceText: {
      fontSize: '0.75rem',
      fontWeight: '700',
      lineHeight: '1.2',
    },
    notesSection: {
      display: 'grid',
      gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))',
      gap: '1rem',
    },
    noteCard: {
      background: '#ffffff',
      padding: '1rem',
      borderRadius: '10px',
      boxShadow: '0 2px 8px rgba(239, 68, 68, 0.08)',
      border: '1px solid #fecaca',
      borderTop: '3px solid #ef4444',
    },
    noteTitle: {
      fontWeight: '600',
      marginBottom: '0.5rem',
      color: '#dc2626',
      fontSize: '0.9rem',
      lineHeight: '1.3',
    },
    noteText: {
      fontSize: '0.85rem',
      color: '#7f1d1d',
      lineHeight: '1.4',
    },
    lobGrid: {
      display: 'grid',
      gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))',
      gap: '1rem',
      marginBottom: '1.5rem',
    },
    lobCard: {
      background: '#ffffff',
      borderRadius: '12px',
      padding: '1.5rem',
      boxShadow: '0 2px 12px rgba(30, 64, 175, 0.08)',
      border: '1px solid #e2e8f0',
      transition: 'transform 0.3s ease, box-shadow 0.3s ease',
      textAlign: 'center',
    },
    lobCardHover: {
      transform: 'translateY(-3px)',
      boxShadow: '0 4px 20px rgba(30, 64, 175, 0.12)',
    },
    lobTitle: {
      fontSize: '1.1rem',
      fontWeight: '600',
      marginBottom: '0.5rem',
      color: '#1e40af',
    },
    lobProgress: {
      fontSize: '1.5rem',
      fontWeight: '700',
      marginBottom: '0.5rem',
    },
    lobLabel: {
      fontSize: '0.9rem',
      color: '#6b7280',
    }
  };

  // Calculate KPI data based on workflow nodes and their completion status
  const totalActivities = workflowNodes.length;

  // Calculate overall progress as average of LOB completion percentages
  const overallProgress = lobCompletions.length > 0
    ? Math.round(lobCompletions.reduce((sum, lob) => sum + lob.completion, 0) / lobCompletions.length)
    : 0;

  // Calculate remaining tasks from all checklist items across all phases
  const remainingTasks = Object.values(checklistData).reduce((total, checklist) => {
    return total + checklist.filter(item => item.status !== 'completed').length;
  }, 0);

  // Calculate planned progress (assuming 100% for all phases)
  const totalPlanned = totalActivities * 100;
  const totalActual = overallProgress;
  const overallVariance = totalPlanned > 0 ? ((totalActual - totalPlanned) / totalPlanned * 100).toFixed(1) : 0;

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <h1 style={styles.title}>Transformation Dashboard</h1>
        <p style={styles.subtitle}>Real-time project transformation insights</p>
      </div>

      {/* KPI Cards */}
      <div style={styles.statsGrid}>
        <div
          style={styles.kpiCard}
          onMouseEnter={(e) => Object.assign(e.currentTarget.style, styles.kpiCardHover)}
          onMouseLeave={(e) => {
            e.currentTarget.style.transform = 'translateY(0)';
            e.currentTarget.style.boxShadow = '0 4px 20px rgba(30, 64, 175, 0.3)';
          }}
        >
          <h3 style={styles.kpiCardTitle}>Project</h3>
          <div style={styles.kpiStatNumber}>{overview.project_name}</div>
          <div style={styles.kpiStatLabel}>{overview.start_date} - {overview.report_date}</div>
        </div>

        <div
          style={styles.kpiCard}
          onMouseEnter={(e) => Object.assign(e.currentTarget.style, styles.kpiCardHover)}
          onMouseLeave={(e) => {
            e.currentTarget.style.transform = 'translateY(0)';
            e.currentTarget.style.boxShadow = '0 4px 20px rgba(30, 64, 175, 0.3)';
          }}
        >
          <h3 style={styles.kpiCardTitle}>Overall Progress</h3>
          <div style={{...styles.kpiStatNumber, color: '#ffffff'}}>
            {overallProgress}%
          </div>
        </div>

        <div
          style={styles.kpiCard}
          onMouseEnter={(e) => Object.assign(e.currentTarget.style, styles.kpiCardHover)}
          onMouseLeave={(e) => {
            e.currentTarget.style.transform = 'translateY(0)';
            e.currentTarget.style.boxShadow = '0 4px 20px rgba(30, 64, 175, 0.3)';
          }}
        >
          <h3 style={styles.kpiCardTitle}>Activities</h3>
          <div style={{...styles.kpiStatNumber, color: '#ffffff'}}>{totalActivities}</div>
          <div style={styles.kpiStatLabel}>Total activities</div>
        </div>

        <div
          style={styles.kpiCard}
          onMouseEnter={(e) => Object.assign(e.currentTarget.style, styles.kpiCardHover)}
          onMouseLeave={(e) => {
            e.currentTarget.style.transform = 'translateY(0)';
            e.currentTarget.style.boxShadow = '0 4px 20px rgba(30, 64, 175, 0.3)';
          }}
        >
          <h3 style={styles.kpiCardTitle}>Remaining Tasks</h3>
          <div style={{...styles.kpiStatNumber, color: '#ffffff'}}>
            {remainingTasks}
          </div>
          <div style={styles.kpiStatLabel}>tasks to complete</div>
        </div>
      </div>

      {/* LOB Completion Grid */}
      <div style={styles.card}>
        <h2 style={styles.cardTitle}>LOB Completion Status</h2>
        <div style={styles.lobGrid}>
          {lobCompletions.map((lob) => (
            <div
              key={lob.lobName}
              style={styles.lobCard}
              onMouseEnter={(e) => Object.assign(e.currentTarget.style, styles.lobCardHover)}
              onMouseLeave={(e) => {
                e.currentTarget.style.transform = 'translateY(0)';
                e.currentTarget.style.boxShadow = '0 2px 12px rgba(30, 64, 175, 0.08)';
              }}
            >
              <div style={styles.lobTitle}>{lob.lobName}</div>
              <div style={{
                ...styles.lobProgress,
                color: getKpiColor(lob.completion)
              }}>
                {lob.completion}%
              </div>
              <div style={styles.lobLabel}>completion</div>
            </div>
          ))}
        </div>
      </div>

      {/* Activities Table */}
      <div style={styles.card}>
        <h2 style={styles.cardTitle}>LoB Activities - Planned vs Actual</h2>
        <div style={styles.tableContainer}>
          <table style={styles.table}>
            <thead>
              <tr>
                <th style={styles.activityHeaderCell}>
                  Activity
                </th>
                {['Prepaid B2B', 'Prepaid B2C', 'Postpaid B2B', 'Postpaid B2C'].map((lob, index) => (
                  <th key={lob} style={{
                    ...styles.lobHeaderGroup,
                    borderTopRightRadius: index === 3 ? '12px' : '0',
                    borderLeft: index > 0 ? '2px solid rgba(255,255,255,0.3)' : 'none',
                  }}>
                    {lob}
                  </th>
                ))}
              </tr>
              <tr style={styles.subHeaderRow}>
                <th style={styles.subHeaderCellFirst}>
                  Project Activities
                </th>
                {['Prepaid B2B', 'Prepaid B2C', 'Postpaid B2B', 'Postpaid B2C'].map((lob, index) => (
                  <th key={lob} style={{
                    ...styles.subHeaderCell,
                    borderRight: index === 3 ? 'none' : '1px solid #e2e8f0'
                  }}>
                    {lob}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {Object.entries(activities.reduce((acc, act) => {
                if (!acc[act.activity_name]) acc[act.activity_name] = {};
                acc[act.activity_name][act.lob] = {
                  planned: act.planned_progress,
                  actual: act.actual_progress
                };
                return acc;
              }, {})).map(([activity, lobData]) => (
                <ActivityRow
                  key={activity}
                  activity={activity}
                  lobData={lobData}
                  styles={styles}
                  getProgressColor={getProgressColor}
                  getVarianceText={getVarianceText}
                  onHoverChange={handleHoverChange}
                  isPopupHovered={isPopupHovered}
                  mouseLeaveTimeout={mouseLeaveTimeout}
                  setMouseLeaveTimeout={setMouseLeaveTimeout}
                />
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Attention Areas */}
      <div style={styles.card}>
        <h2 style={styles.cardTitle}>⚠️ Attention Areas</h2>
        <div style={styles.notesSection}>
          {attentionAreas.map((area, index) => (
            <div
              key={index}
              style={{
                ...styles.noteCard,
                cursor: 'pointer',
                transition: 'all 0.3s ease'
              }}
              onClick={() => {
                // Find the workflow node that matches this attention area
                const matchingNode = workflowNodes.find(node =>
                  node.name.toLowerCase() === area.section.toLowerCase()
                );
                if (matchingNode) {
                  navigate(`/workflow?phase=${matchingNode.id}`);
                }
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.transform = 'translateY(-2px)';
                e.currentTarget.style.boxShadow = '0 6px 20px rgba(239, 68, 68, 0.15)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.transform = 'translateY(0)';
                e.currentTarget.style.boxShadow = '0 2px 8px rgba(239, 68, 68, 0.08)';
              }}
              title="Click to view this phase in the workflow"
            >
              <div style={styles.noteTitle}>{area.section} - {area.lob}</div>
              <div style={styles.noteText}>{area.notes}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Backdrop for blur effect */}
      {hoveredActivity && getHoveredActivityDetails(hoveredActivity) && (
        <div
          style={{
            position: 'fixed',
            top: 0,
            left: 0,
            width: '100vw',
            height: '100vh',
            background: 'rgba(0, 0, 0, 0.3)',
            backdropFilter: 'blur(4px)',
            zIndex: 999,
          }}
          onClick={() => {
            setHoveredActivity(null);
          }}
        />
      )}

      {/* Activity Details Popup */}
      {hoveredActivity && getHoveredActivityDetails(hoveredActivity) && (
        <div
          style={{
            position: 'fixed',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            width: '90vw',
            maxWidth: '600px',
            background: '#ffffff',
            border: '1px solid #e2e8f0',
            borderRadius: '12px',
            boxShadow: '0 8px 32px rgba(30, 64, 175, 0.15)',
            padding: '1.5rem',
            zIndex: 1000,
            fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
            maxHeight: '80vh',
            overflowY: 'auto',
          }}
        >
          <div style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            marginBottom: '1rem',
            borderBottom: '2px solid #3b82f6',
            paddingBottom: '0.5rem',
          }}>
            <h3 style={{
              fontSize: '1.2rem',
              fontWeight: '700',
              color: '#1e40af',
              margin: 0,
            }}>
              {hoveredActivity.split(' ').map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join(' ')}
            </h3>
            <button
              onClick={() => setHoveredActivity(null)}
              style={{
                background: 'none',
                border: 'none',
                fontSize: '1.2rem',
                cursor: 'pointer',
                color: '#6b7280',
                padding: '0.25rem',
                borderRadius: '4px',
                transition: 'background-color 0.2s ease',
              }}
              onMouseEnter={(e) => {
                e.target.style.backgroundColor = '#f3f4f6';
                e.target.style.color = '#374151';
              }}
              onMouseLeave={(e) => {
                e.target.style.backgroundColor = 'transparent';
                e.target.style.color = '#6b7280';
              }}
              title="Close"
            >
              ×
            </button>
          </div>

          <div style={{ marginBottom: '1rem' }}>
            <div style={{ fontSize: '0.9rem', fontWeight: '600', color: '#374151', marginBottom: '0.25rem' }}>
              Start Date: <span style={{ fontWeight: '400', color: '#6b7280' }}>{getHoveredActivityDetails(hoveredActivity).startDate}</span>
            </div>
            <div style={{ fontSize: '0.9rem', fontWeight: '600', color: '#374151', marginBottom: '0.25rem' }}>
              Due Date: <span style={{ fontWeight: '400', color: '#6b7280' }}>{getHoveredActivityDetails(hoveredActivity).dueDate}</span>
            </div>
            <div style={{ fontSize: '0.9rem', fontWeight: '600', color: '#374151', marginBottom: '0.25rem' }}>
              Assignee: <span style={{ fontWeight: '400', color: '#6b7280' }}>{getHoveredActivityDetails(hoveredActivity).assignee}</span>
            </div>
            <div style={{ fontSize: '0.9rem', fontWeight: '600', color: '#374151', marginBottom: '0.25rem' }}>
              Progress: <span style={{ fontWeight: '400', color: '#6b7280' }}>{getHoveredActivityDetails(hoveredActivity).progress}%</span>
            </div>
            <div style={{ fontSize: '0.9rem', fontWeight: '600', color: '#374151' }}>
              Phase: <span style={{ fontWeight: '400', color: '#6b7280' }}>{getHoveredActivityDetails(hoveredActivity).phaseName}</span>
            </div>
          </div>

          <div style={{ marginBottom: '1rem' }}>
            <div style={{ fontSize: '0.9rem', fontWeight: '600', color: '#374151', marginBottom: '0.5rem' }}>
              Key Activities Status:
            </div>
            <table style={{
              width: '100%',
              borderCollapse: 'collapse',
              fontSize: '0.85rem',
              marginBottom: '1rem'
            }}>
              <thead>
                <tr style={{ backgroundColor: '#f8fafc' }}>
                  <th style={{
                    padding: '0.5rem',
                    textAlign: 'left',
                    fontWeight: '600',
                    color: '#374151',
                    borderBottom: '1px solid #e2e8f0'
                  }}>Key Activities</th>
                  <th style={{
                    padding: '0.5rem',
                    textAlign: 'center',
                    fontWeight: '600',
                    color: '#374151',
                    borderBottom: '1px solid #e2e8f0',
                    width: '80px'
                  }}>Status</th>
                </tr>
              </thead>
              <tbody>
                {getHoveredActivitySummaries(hoveredActivity).map((item, index) => {
                  // Use the actual status from the item if available
                  let status = item.status || 'pending';
                  let statusColor = '#f59e0b'; // Default to pending color

                  // Map status to color
                  if (status === 'completed') {
                    statusColor = '#16a34a';
                  } else if (status === 'in-progress' || status === 'review') {
                    statusColor = '#3b82f6';
                  } else if (status === 'overdue') {
                    statusColor = '#ef4444';
                  }

                  // Capitalize status for display
                  const displayStatus = status.charAt(0).toUpperCase() + status.slice(1).replace('-', ' ');

      return (
        <tr
          key={index}
          style={{
            borderBottom: '1px solid #f1f5f9',
            cursor: 'pointer',
            transition: 'all 0.3s ease'
          }}
          onClick={() => {
            const activityDetails = getHoveredActivityDetails(hoveredActivity);
            if (activityDetails && activityDetails.id) {
              // Navigate with checklist item ID to focus on the specific item
              navigate(`/workflow?phase=${activityDetails.id}&checklistId=${item.ch_id || index}`);
            }
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.backgroundColor = '#f8fafc';
            e.currentTarget.style.transform = 'translateY(-1px)';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.backgroundColor = 'transparent';
            e.currentTarget.style.transform = 'translateY(0)';
          }}
          title="Click to view this phase in the workflow"
        >
          <td style={{
            padding: '0.5rem',
            color: '#6b7280',
            textAlign: 'left'
          }}>{item.text}</td>
          <td style={{
            padding: '0.5rem',
            textAlign: 'center'
          }}>
            <span style={{
              backgroundColor: statusColor + '20',
              color: statusColor,
              padding: '0.2rem 0.5rem',
              borderRadius: '4px',
              fontSize: '0.75rem',
              fontWeight: '600'
            }}>
              {displayStatus}
            </span>
          </td>
        </tr>
      );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
};

export default TransformationDashboardStatic;
