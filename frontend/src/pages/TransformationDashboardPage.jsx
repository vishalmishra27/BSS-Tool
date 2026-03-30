import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';

// Map activity names to phase IDs for navigation
const ACTIVITY_PHASE_MAP = {
  'initiation & planning': 'phase1',
  'srs finalization': 'phase2',
  'product rationalization': 'phase3',
  'configuration validation': 'phase4',
  'data cleanup & preparation': 'phase5',
  'uat execution': 'phase6',
  'trial / dry run migrations': 'phase7',
  'trial / dry run migration': 'phase7',
  'final migration & cutover': 'phase8',
  'post-migration stabilization': 'phase9',
};

const ActivityRow = ({
  activity,
  lobData,
  styles,
  getProgressColor,
  getVarianceText,
  onHoverChange,
  isPopupHovered,
  mouseLeaveTimeout,
  setMouseLeaveTimeout,
  onActivityClick,
}) => {
  return (
    <tr
      style={styles.tableRow}
      onMouseEnter={(e) => {
        e.currentTarget.style.background = '#f8fafc';
        e.currentTarget.style.transform = 'translateY(-1px)';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.background = '#ffffff';
        e.currentTarget.style.transform = 'translateY(0)';
      }}
    >
      <td style={{ ...styles.activityCell }} title={activity}>
        <span
          onClick={() => onActivityClick(activity)}
          onMouseEnter={(e) => {
            const rect = e.currentTarget.getBoundingClientRect();
            const popupWidth = 320;
            let viewportX = rect.right + 15;
            const viewportY = rect.top + rect.height / 2;
            if (viewportX + popupWidth > window.innerWidth - 20) {
              viewportX = rect.left - popupWidth - 15;
            }
            if (viewportX < 10) viewportX = 10;
            onHoverChange(activity, { x: viewportX, y: viewportY });
          }}
          onMouseLeave={() => {
            const timeout = setTimeout(() => {
              if (!isPopupHovered) onHoverChange(null, null);
            }, 300);
            if (mouseLeaveTimeout) clearTimeout(mouseLeaveTimeout);
            setMouseLeaveTimeout(timeout);
          }}
          style={{ cursor: 'pointer', display: 'inline-block', width: '100%', color: '#1e40af', textDecoration: 'underline dotted' }}
        >
          {activity}
        </span>
      </td>
      {['Prepaid B2B', 'Prepaid B2C', 'Postpaid B2B', 'Postpaid B2C'].map((lob, lobIndex) => {
        const planned = lobData[lob]?.planned || 0;
        const actual = lobData[lob]?.actual || 0;
        const color = getProgressColor(planned, actual);
        const variance = getVarianceText(planned, actual);
        return (
          <React.Fragment key={`${activity}-${lob}`}>
            <td style={{ ...styles.tableCell, borderLeft: lobIndex > 0 ? '1px solid #f1f5f9' : 'none', backgroundColor: '#fafafa' }}>
              <div style={styles.plannedBox}>{planned}%</div>
            </td>
            <td style={styles.tableCell}>
              <div style={{ ...styles.actualBox, backgroundColor: `${color}15`, border: `1px solid ${color}40`, color }}>
                {actual}%
              </div>
            </td>
            <td style={{ ...styles.tableCell, borderRight: lobIndex === 3 ? 'none' : '1px solid #f1f5f9' }}>
              <div style={{ ...styles.varianceText, color, fontWeight: '700' }}>{variance}</div>
            </td>
          </React.Fragment>
        );
      })}
    </tr>
  );
};

const TransformationDashboardPage = () => {
  const navigate = useNavigate();
  const [overview, setOverview] = useState({
    project_name: 'Digital Transformation Initiative',
    start_date: '2024-01-15',
    report_date: '2024-12-15',
    planned_progress: 85,
    actual_progress: 78,
    variance: -7,
  });

  const [activities, setActivities] = useState([]);
  const [attentionAreas, setAttentionAreas] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [hoveredActivity, setHoveredActivity] = useState(null);
  const [popupPosition, setPopupPosition] = useState({ x: 0, y: 0 });
  const [isPopupHovered, setIsPopupHovered] = useState(false);
  const [mouseLeaveTimeout, setMouseLeaveTimeout] = useState(null);

  const activityDetails = {
    'Process Documentation': { startDate: '2024-06-01', dueDate: '2024-07-15', assignee: 'Alice Johnson' },
    'System Integration': { startDate: '2024-06-15', dueDate: '2024-08-01', assignee: 'Bob Smith' },
    'User Training': { startDate: '2024-07-01', dueDate: '2024-09-10', assignee: 'Carol Lee' },
    'Testing & QA': { startDate: '2024-08-01', dueDate: '2024-10-05', assignee: 'David Kim' },
    'Initiation & Planning': { startDate: '2024-05-01', dueDate: '2024-06-20', assignee: 'Eve Martinez' },
    'SRS Finalization': { startDate: '2024-06-10', dueDate: '2024-07-30', assignee: 'Frank Wilson' },
    'Product Rationalization': { startDate: '2024-07-15', dueDate: '2024-08-15', assignee: 'Grace Chen' },
    'Configuration Validation': { startDate: '2024-08-01', dueDate: '2024-09-01', assignee: 'Hank Green' },
    'Data Cleanup & Preparation': { startDate: '2024-08-15', dueDate: '2024-09-20', assignee: 'Ivy Brown' },
    'UAT Execution': { startDate: '2024-09-15', dueDate: '2024-10-15', assignee: 'Jack White' },
    'Trial / Dry Run Migrations': { startDate: '2024-10-01', dueDate: '2024-11-01', assignee: 'Karen Black' },
    'Final Migration & Cutover': { startDate: '2024-11-01', dueDate: '2024-12-01', assignee: 'Leo Davis' },
  };

  const activitySummaries = {
    'Initiation & Planning': [
      'Define migration scope (systems, modules, products)',
      'Establish governance structure and key stakeholders',
      'Set project timelines and cutover windows',
      'Assign roles, responsibilities, and tool access',
      'Prepare migration strategy and high-level plan',
    ],
    'SRS Finalization': [
      'Align SRS with business and functional objectives',
      'Map traceability matrix for all requirements',
      'Conduct stakeholder walkthroughs and sign-offs',
      'Document system integration points and exception flows',
      'Approve readiness checklist for build/UAT',
    ],
    'Product Rationalization': [
      'Analyze current product portfolio (B2B/B2C)',
      'Compare parameters across legacy and target systems',
      'Reconcile product setup changes (LoB, bundles)',
      'Define customer transition rules',
      'Approve final rationalized catalog',
    ],
    'Configuration Validation': [
      'Validate product configurations (plans, benefits, charges)',
      'Compare parameters across legacy and target systems',
      'Reconcile product setup changes (LoB, bundles)',
      'Test lifecycle scenarios (activation to termination)',
      'Approve configuration sign-off',
    ],
    'Data Cleanup & Preparation': [
      'Identify and purge inactive or duplicate accounts',
      'Align CRM, billing, and order management data',
      'Validate reference data and lookup tables',
      'Perform pre-migration reconciliation (CBS vs CRM, Inventory)',
      'Sign-off on cleaned and approved dataset',
    ],
    'UAT Execution': [
      'Execute end-to-end test cases across business journeys',
      'Validate bill formats, tax, usage, and notifications',
      'Track and close UAT defects',
      'Reconcile outputs against expected values',
      'Obtain formal UAT sign-off',
    ],
    'Trial / Dry Run Migrations': [
      'Load sample or full-size data into target environment',
      'Perform batch runs and monitor processing issues',
      'Validate post-load data accuracy',
      'Reconcile key KPIs (customer count, balance, status)',
      'Review trial run report with business',
    ],
    'Final Migration & Cutover': [
      'Freeze final cutover plan and timings',
      'Execute data extraction, transformation, and loading',
      'Perform reconciliation: legacy vs target',
      'Run critical workflows and validate outputs',
      'Approve go-live readiness report',
    ],
  };

  const activityDetailsLower = {};
  Object.keys(activityDetails).forEach((key) => {
    activityDetailsLower[key.toLowerCase()] = activityDetails[key];
  });

  const activitySummariesLower = {};
  Object.keys(activitySummaries).forEach((key) => {
    activitySummariesLower[key.toLowerCase()] = activitySummaries[key];
  });

  const handleHoverChange = (activity, position) => {
    const trimmedActivity = activity ? activity.trim().toLowerCase() : null;
    setHoveredActivity(trimmedActivity);
    if (position) setPopupPosition(position);
  };

  const handleActivityClick = (activityName) => {
    const phaseId = ACTIVITY_PHASE_MAP[activityName.trim().toLowerCase()];
    if (phaseId) {
      navigate(`/workflow?phase=${phaseId}`);
    }
  };

  useEffect(() => {
    const fetchTransformationData = async () => {
      try {
        setLoading(true);

        const overviewResponse = await fetch('/api/project_overview');
        if (!overviewResponse.ok) throw new Error(`Failed to fetch project overview: ${overviewResponse.status}`);
        const overviewData = await overviewResponse.json();
        setOverview(overviewData);

        const activitiesResponse = await fetch('/api/project_activities');
        if (!activitiesResponse.ok) throw new Error(`Failed to fetch project activities: ${activitiesResponse.status}`);
        const activitiesData = await activitiesResponse.json();
        setActivities(activitiesData);

        const attentionResponse = await fetch('/api/attention_areas');
        if (!attentionResponse.ok) throw new Error(`Failed to fetch attention areas: ${attentionResponse.status}`);
        const attentionData = await attentionResponse.json();
        setAttentionAreas(attentionData);

        setLoading(false);
      } catch (err) {
        setError(err.message);
        setLoading(false);
      }
    };

    fetchTransformationData();
  }, []);

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
    header: { textAlign: 'center', marginBottom: '1.5rem' },
    title: {
      fontSize: '2.2rem', fontWeight: '700', marginBottom: '0.5rem',
      color: '#1e40af', textShadow: '0 2px 4px rgba(30, 64, 175, 0.1)',
    },
    subtitle: { fontSize: '1.1rem', color: '#3b82f6', marginBottom: '1.5rem', fontWeight: '500' },
    statsGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))', gap: '1rem', marginBottom: '1.5rem' },
    kpiCard: {
      background: 'linear-gradient(135deg, #1e40af 0%, #3b82f6 100%)',
      borderRadius: '12px', padding: '1.5rem', marginBottom: '1.5rem',
      boxShadow: '0 4px 20px rgba(30, 64, 175, 0.3)', border: '1px solid #1e40af',
      transition: 'transform 0.3s ease, box-shadow 0.3s ease',
      color: '#ffffff', position: 'relative', overflow: 'hidden', minHeight: '120px',
      display: 'flex', flexDirection: 'column', justifyContent: 'space-between',
    },
    kpiCardHover: { transform: 'translateY(-3px)', boxShadow: '0 8px 30px rgba(30, 64, 175, 0.4)' },
    kpiCardTitle: { fontSize: '1.1rem', fontWeight: '600', marginBottom: '0.5rem', color: '#ffffff', opacity: '0.9' },
    kpiStatNumber: {
      fontSize: '1.8rem', fontWeight: '700', color: '#ffffff', marginBottom: '0.25rem',
      textShadow: '0 2px 4px rgba(0, 0, 0, 0.2)', wordBreak: 'break-word', lineHeight: '1.2',
    },
    kpiStatLabel: { fontSize: '0.85rem', color: '#e0f2fe', opacity: '0.9', wordBreak: 'break-word' },
    kpiMiniStats: { display: 'flex', justifyContent: 'space-between', marginTop: '0.5rem', gap: '0.5rem' },
    kpiMiniStat: { textAlign: 'center', flex: 1 },
    kpiMiniStatNumber: { fontSize: '1rem', fontWeight: '600', marginBottom: '0.25rem', color: '#ffffff' },
    kpiMiniStatLabel: { fontSize: '0.7rem', color: '#bfdbfe', opacity: '0.9' },
    card: {
      background: '#ffffff', borderRadius: '12px', padding: '1.5rem', marginBottom: '1.5rem',
      boxShadow: '0 2px 12px rgba(30, 64, 175, 0.08)', border: '1px solid #e2e8f0',
      transition: 'transform 0.3s ease, box-shadow 0.3s ease', overflow: 'hidden',
    },
    cardTitle: {
      fontSize: '1.4rem', fontWeight: '600', marginBottom: '1rem', color: '#1e40af',
      borderBottom: '2px solid #3b82f6', paddingBottom: '0.5rem',
    },
    tableContainer: {
      overflowX: 'auto', borderRadius: '12px',
      boxShadow: '0 2px 12px rgba(30, 64, 175, 0.08)', border: '1px solid #e2e8f0',
    },
    table: { width: '100%', borderCollapse: 'separate', borderSpacing: 0, background: '#ffffff', minWidth: '1000px' },
    tableHeader: { background: 'linear-gradient(135deg, #1e40af, #3b82f6)', color: '#ffffff' },
    activityHeaderCell: {
      padding: '1rem 0.75rem', textAlign: 'left', fontWeight: '600', fontSize: '0.9rem', color: '#ffffff',
      borderBottom: '2px solid #1e40af', borderTopLeftRadius: '12px',
    },
    lobHeaderGroup: {
      background: 'linear-gradient(135deg, #1e40af, #3b82f6)', color: '#ffffff',
      padding: '1rem 0.5rem', fontWeight: '700', fontSize: '0.9rem', textAlign: 'center',
      borderRight: '2px solid rgba(255,255,255,0.3)',
    },
    subHeaderRow: { background: '#f8fafc', borderBottom: '2px solid #e2e8f0' },
    subHeaderCell: {
      padding: '0.75rem 0.5rem', fontSize: '0.8rem', fontWeight: '600', color: '#1e40af',
      textAlign: 'center', borderRight: '1px solid #e2e8f0', backgroundColor: '#f8fafc',
    },
    subHeaderCellFirst: {
      padding: '0.75rem 0.5rem', fontSize: '0.8rem', fontWeight: '600', color: '#1e40af',
      textAlign: 'left', paddingLeft: '0.75rem', borderRight: '1px solid #e2e8f0', backgroundColor: '#f8fafc',
    },
    tableRow: { background: '#ffffff', borderBottom: '1px solid #e2e8f0', transition: 'all 0.3s ease', cursor: 'default' },
    tableCell: {
      padding: '0.75rem 0.5rem', fontSize: '0.85rem', color: '#374151', verticalAlign: 'middle', textAlign: 'center',
    },
    activityCell: {
      textAlign: 'left', fontWeight: '600', minWidth: '200px', maxWidth: '250px',
      color: '#1e293b', paddingLeft: '0.75rem',
    },
    plannedBox: {
      backgroundColor: '#dbeafe', border: '1px solid #3b82f6', borderRadius: '6px',
      padding: '0.4rem 0.5rem', fontSize: '0.8rem', fontWeight: '600', color: '#1e40af',
      minWidth: '40px', display: 'inline-block',
    },
    actualBox: { borderRadius: '6px', padding: '0.4rem 0.5rem', fontSize: '0.8rem', fontWeight: '700', minWidth: '40px', display: 'inline-block' },
    varianceText: { fontSize: '0.75rem', fontWeight: '700', lineHeight: '1.2' },
    notesSection: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '1rem' },
    noteCard: {
      background: '#ffffff', padding: '1rem', borderRadius: '10px',
      boxShadow: '0 2px 8px rgba(239, 68, 68, 0.08)', border: '1px solid #fecaca', borderTop: '3px solid #ef4444',
    },
    noteTitle: { fontWeight: '600', marginBottom: '0.5rem', color: '#dc2626', fontSize: '0.9rem', lineHeight: '1.3' },
    noteText: { fontSize: '0.85rem', color: '#7f1d1d', lineHeight: '1.4' },
  };

  // Group activities by name
  const groupedActivities = activities.reduce((acc, act) => {
    if (!acc[act.activity_name]) acc[act.activity_name] = {};
    acc[act.activity_name][act.lob] = { planned: act.planned_progress, actual: act.actual_progress };
    return acc;
  }, {});

  const totalActivities = Object.keys(groupedActivities).length;
  const totalPlanned = activities.reduce((sum, act) => sum + (act.planned_progress || 0), 0);
  const totalActual = activities.reduce((sum, act) => sum + (act.actual_progress || 0), 0);
  const overallVariance = totalPlanned > 0 ? ((totalActual - totalPlanned) / totalPlanned * 100).toFixed(1) : 0;

  if (loading) {
    return (
      <div style={styles.container}>
        <div style={{ textAlign: 'center', padding: '4rem' }}>
          <div style={{ fontSize: '1.5rem', color: '#1e40af' }}>Loading transformation dashboard...</div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div style={styles.container}>
        <div style={{ textAlign: 'center', padding: '4rem' }}>
          <div style={{ fontSize: '1.5rem', color: '#ef4444' }}>Error: {error}</div>
        </div>
      </div>
    );
  }

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
          onMouseLeave={(e) => { e.currentTarget.style.transform = 'translateY(0)'; e.currentTarget.style.boxShadow = '0 4px 20px rgba(30, 64, 175, 0.3)'; }}
        >
          <h3 style={styles.kpiCardTitle}>Project</h3>
          <div style={styles.kpiStatNumber}>{overview.project_name}</div>
          <div style={styles.kpiStatLabel}>{overview.start_date} - {overview.report_date}</div>
        </div>

        <div
          style={styles.kpiCard}
          onMouseEnter={(e) => Object.assign(e.currentTarget.style, styles.kpiCardHover)}
          onMouseLeave={(e) => { e.currentTarget.style.transform = 'translateY(0)'; e.currentTarget.style.boxShadow = '0 4px 20px rgba(30, 64, 175, 0.3)'; }}
        >
          <h3 style={styles.kpiCardTitle}>Overall Progress</h3>
          <div style={{ ...styles.kpiStatNumber, color: getKpiColor(overview.actual_progress) }}>{overview.actual_progress}%</div>
          <div style={styles.kpiMiniStats}>
            <div style={styles.kpiMiniStat}>
              <div style={{ ...styles.kpiMiniStatNumber, color: getKpiColor(overview.planned_progress) }}>{overview.planned_progress}%</div>
              <div style={styles.kpiMiniStatLabel}>Planned</div>
            </div>
            <div style={styles.kpiMiniStat}>
              <div style={{ ...styles.kpiMiniStatNumber, color: overview.variance >= 0 ? '#86efac' : '#fca5a5' }}>{overview.variance}%</div>
              <div style={styles.kpiMiniStatLabel}>Variance</div>
            </div>
          </div>
        </div>

        <div
          style={styles.kpiCard}
          onMouseEnter={(e) => Object.assign(e.currentTarget.style, styles.kpiCardHover)}
          onMouseLeave={(e) => { e.currentTarget.style.transform = 'translateY(0)'; e.currentTarget.style.boxShadow = '0 4px 20px rgba(30, 64, 175, 0.3)'; }}
        >
          <h3 style={styles.kpiCardTitle}>Activities</h3>
          <div style={{ ...styles.kpiStatNumber, color: '#ffffff' }}>{totalActivities}</div>
          <div style={styles.kpiStatLabel}>Total activities</div>
        </div>

        <div
          style={styles.kpiCard}
          onMouseEnter={(e) => Object.assign(e.currentTarget.style, styles.kpiCardHover)}
          onMouseLeave={(e) => { e.currentTarget.style.transform = 'translateY(0)'; e.currentTarget.style.boxShadow = '0 4px 20px rgba(30, 64, 175, 0.3)'; }}
        >
          <h3 style={styles.kpiCardTitle}>Remaining Tasks</h3>
          <div style={{ ...styles.kpiStatNumber, color: getKpiColor(100 + parseFloat(overallVariance)) }}>
            {Math.max(0, totalActivities * 4 - Math.floor(totalActual / 25))}
          </div>
          <div style={styles.kpiStatLabel}>tasks to complete</div>
        </div>
      </div>

      {/* LOB Completion Status */}
      <div style={styles.card}>
        <h2 style={styles.cardTitle}>LOB Completion Status</h2>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '1.5rem', textAlign: 'center' }}>
          {['Prepaid B2B', 'Prepaid B2C', 'Postpaid B2B', 'Postpaid B2C'].map((lob) => {
            const lobActs = activities.filter((a) => a.lob === lob);
            const totalP = lobActs.reduce((s, a) => s + (a.actual_progress || 0), 0);
            const avg = lobActs.length > 0 ? Math.round(totalP / lobActs.length) : 0;
            const color = getKpiColor(avg);
            return (
              <div key={lob}>
                <div style={{ fontSize: '1rem', fontWeight: '600', color: '#374151', marginBottom: '0.5rem' }}>{lob}</div>
                <div style={{ fontSize: '1.5rem', fontWeight: '700', color }}>{avg}%</div>
                <div style={{ fontSize: '0.8rem', color: '#6b7280' }}>completion</div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Activities Table */}
      <div style={styles.card}>
        <h2 style={styles.cardTitle}>LoB Activities - Planned vs Actual</h2>
        <div style={styles.tableContainer}>
          <table style={styles.table}>
            <thead>
              <tr>
                <th style={styles.activityHeaderCell}>Activity</th>
                {['Prepaid B2B', 'Prepaid B2C', 'Postpaid B2B', 'Postpaid B2C'].map((lob, index) => (
                  <th key={lob} colSpan="3" style={{
                    ...styles.lobHeaderGroup,
                    borderTopRightRadius: index === 3 ? '12px' : '0',
                    borderLeft: index > 0 ? '2px solid rgba(255,255,255,0.3)' : 'none',
                  }}>
                    {lob}
                  </th>
                ))}
              </tr>
              <tr style={styles.subHeaderRow}>
                <th style={styles.subHeaderCellFirst}>Project Activities</th>
                {['Prepaid B2B', 'Prepaid B2C', 'Postpaid B2B', 'Postpaid B2C'].map((lob) => (
                  <React.Fragment key={`sub-${lob}`}>
                    <th style={styles.subHeaderCell}>Planned</th>
                    <th style={styles.subHeaderCell}>Actual</th>
                    <th style={styles.subHeaderCell}>Variance</th>
                  </React.Fragment>
                ))}
              </tr>
            </thead>
            <tbody>
              {Object.entries(groupedActivities).map(([activity, lobData]) => (
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
                  onActivityClick={handleActivityClick}
                />
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Attention Areas */}
      <div style={styles.card}>
        <h2 style={styles.cardTitle}>⚠ Attention Areas</h2>
        <div style={styles.notesSection}>
          {attentionAreas.map((note, idx) => (
            <div key={idx} style={styles.noteCard}
              onMouseEnter={(e) => { e.currentTarget.style.transform = 'translateY(-2px)'; e.currentTarget.style.boxShadow = '0 4px 20px rgba(239, 68, 68, 0.12)'; }}
              onMouseLeave={(e) => { e.currentTarget.style.transform = 'translateY(0)'; e.currentTarget.style.boxShadow = '0 2px 10px rgba(239, 68, 68, 0.08)'; }}
            >
              <div style={styles.noteTitle}>{note.section} - {note.lob}</div>
              <div style={styles.noteText}>{note.notes}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Activity Details Popup */}
      {hoveredActivity && activityDetailsLower[hoveredActivity] && (
        <div
          style={{
            position: 'fixed', top: popupPosition.y, left: popupPosition.x, width: '380px',
            background: '#ffffff', border: '1px solid #e2e8f0', borderRadius: '12px',
            boxShadow: '0 8px 32px rgba(30, 64, 175, 0.15)', padding: '1.5rem', zIndex: 1000,
            maxHeight: '450px', overflowY: 'auto',
          }}
          onMouseEnter={() => setIsPopupHovered(true)}
          onMouseLeave={() => { setIsPopupHovered(false); setHoveredActivity(null); }}
        >
          <h3 style={{ fontSize: '1.2rem', fontWeight: '700', marginBottom: '1rem', color: '#1e40af', borderBottom: '2px solid #3b82f6', paddingBottom: '0.5rem' }}>
            {Object.keys(activityDetails).find((key) => key.toLowerCase() === hoveredActivity)}
          </h3>

          <div style={{ marginBottom: '1rem' }}>
            <div style={{ fontSize: '0.9rem', fontWeight: '600', color: '#374151', marginBottom: '0.25rem' }}>
              Start Date: <span style={{ fontWeight: '400', color: '#6b7280' }}>{activityDetailsLower[hoveredActivity].startDate}</span>
            </div>
            <div style={{ fontSize: '0.9rem', fontWeight: '600', color: '#374151', marginBottom: '0.25rem' }}>
              Due Date: <span style={{ fontWeight: '400', color: '#6b7280' }}>{activityDetailsLower[hoveredActivity].dueDate}</span>
            </div>
            <div style={{ fontSize: '0.9rem', fontWeight: '600', color: '#374151' }}>
              Assignee: <span style={{ fontWeight: '400', color: '#6b7280' }}>{activityDetailsLower[hoveredActivity].assignee}</span>
            </div>
          </div>

          <div style={{ marginBottom: '1rem' }}>
            <div style={{ fontSize: '0.9rem', fontWeight: '600', color: '#374151', marginBottom: '0.5rem' }}>Key Activities Status:</div>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem', marginBottom: '1rem' }}>
              <thead>
                <tr style={{ backgroundColor: '#f8fafc' }}>
                  <th style={{ padding: '0.5rem', textAlign: 'left', fontWeight: '600', color: '#374151', borderBottom: '1px solid #e2e8f0' }}>Key Activities</th>
                  <th style={{ padding: '0.5rem', textAlign: 'center', fontWeight: '600', color: '#374151', borderBottom: '1px solid #e2e8f0', width: '80px' }}>Status</th>
                </tr>
              </thead>
              <tbody>
                {(activitySummariesLower[hoveredActivity] || []).map((item, index) => {
                  let status = 'Pending';
                  let statusColor = '#f59e0b';
                  if (index % 3 === 0) { status = 'Completed'; statusColor = '#16a34a'; }
                  else if (index % 3 === 1) { status = 'Review'; statusColor = '#3b82f6'; }
                  return (
                    <tr key={index} style={{ borderBottom: '1px solid #f1f5f9' }}>
                      <td style={{ padding: '0.5rem', color: '#6b7280', textAlign: 'left' }}>{item}</td>
                      <td style={{ padding: '0.5rem', textAlign: 'center' }}>
                        <span style={{ backgroundColor: statusColor + '20', color: statusColor, padding: '0.2rem 0.5rem', borderRadius: '4px', fontSize: '0.75rem', fontWeight: '600' }}>
                          {status}
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

export default TransformationDashboardPage;
