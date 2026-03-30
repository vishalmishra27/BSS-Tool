import { useEffect, useState } from 'react';
import LOBCompletionColumn from './LOBCompletionColumn';

const TransformationDashboardFixed = () => {
  const [overview, setOverview] = useState({
    project_name: "Digital Transformation Initiative",
    start_date: "2024-01-15",
    report_date: "2024-12-15",
    planned_progress: 85,
    actual_progress: 78,
    variance: -7
  });
  const [lobCompletions, setLobCompletions] = useState([]);
  const [activities, setActivities] = useState([]);
  const [attentionAreas, setAttentionAreas] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const LOB_NAMES = ['Prepaid B2B', 'Prepaid B2C', 'Postpaid B2B', 'Postpaid B2C'];

  useEffect(() => {
    const fetchTransformationData = async () => {
      try {
        setLoading(true);
        const overviewResponse = await fetch('/api/project_overview');
        if (!overviewResponse.ok) {
          throw new Error(`Failed to fetch project overview: ${overviewResponse.status}`);
        }
        const overviewData = await overviewResponse.json();
        setOverview(overviewData);

        const lobPromises = LOB_NAMES.map(async (lob) => {
          const response = await fetch(`/api/lob/checklist/completion-percentage/${encodeURIComponent(lob)}`);
          const data = await response.json();
          return { lobName: lob, completion: data.completion_percentage || 0 };
        });
        const lobData = await Promise.all(lobPromises);
        setLobCompletions(lobData);

        const activitiesResponse = await fetch('/api/project_activities');
        if (!activitiesResponse.ok) {
          throw new Error(`Failed to fetch project activities: ${activitiesResponse.status}`);
        }
        const activitiesData = await activitiesResponse.json();
        setActivities(activitiesData);

        const attentionResponse = await fetch('/api/attention_areas');
        if (!attentionResponse.ok) {
          throw new Error(`Failed to fetch attention areas: ${attentionResponse.status}`);
        }
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

  const getKpiColor = (value, threshold = 80) => {
    if (value >= threshold) return '#16a34a';
    if (value >= 60) return '#f59e0b';
    return '#ef4444';
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
      justifyContent: 'space-between',
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
      fontSize: '1.8rem',
      fontWeight: '700',
      color: '#ffffff',
      marginBottom: '0.25rem',
      textShadow: '0 2px 4px rgba(0, 0, 0, 0.2)',
      wordBreak: 'break-word',
      lineHeight: '1.2',
    },
    kpiStatLabel: {
      fontSize: '0.9rem',
      color: '#bfdbfe',
      opacity: '0.9',
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
  };

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

      <div style={styles.statsGrid}>
        <div
          style={styles.kpiCard}
          onMouseEnter={(e) => Object.assign(e.currentTarget.style, styles.kpiCardHover)}
          onMouseLeave={(e) => {
            e.currentTarget.style.transform = 'translateY(0)';
            e.currentTarget.style.boxShadow = '0 4px 20px rgba(30, 64, 175, 0.3)';
          }}>
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
          }}>
          <h3 style={styles.kpiCardTitle}>Overall Progress</h3>
          <div style={{...styles.kpiStatNumber, color: getKpiColor(overview.actual_progress)}}>
            {overview.actual_progress}%
          </div>
          <div style={styles.kpiStatLabel}>Actual Progress</div>
        </div>
        <div
          style={styles.kpiCard}
          onMouseEnter={(e) => Object.assign(e.currentTarget.style, styles.kpiCardHover)}
          onMouseLeave={(e) => {
            e.currentTarget.style.transform = 'translateY(0)';
            e.currentTarget.style.boxShadow = '0 4px 20px rgba(30, 64, 175, 0.3)';
          }}>
          <h3 style={styles.kpiCardTitle}>LOBs</h3>
          <div style={styles.kpiStatNumber}>{LOB_NAMES.length}</div>
          <div style={styles.kpiStatLabel}>Active Lines of Business</div>
        </div>
      </div>

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
              }}>
              <LOBCompletionColumn
                lobName={lob.lobName}
                completion={lob.completion}
              />
            </div>
          ))}
        </div>
      </div>

      <div style={styles.card}>
        <h2 style={styles.cardTitle}>Attention Areas</h2>
        <div style={styles.notesSection}>
          {attentionAreas.map((note, idx) => (
            <div
              key={idx}
              style={styles.noteCard}
              onMouseEnter={(e) => {
                e.currentTarget.style.transform = 'translateY(-2px)';
                e.currentTarget.style.boxShadow = '0 4px 20px rgba(239, 68, 68, 0.12)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.transform = 'translateY(0)';
                e.currentTarget.style.boxShadow = '0 2px 10px rgba(239, 68, 68, 0.08)';
              }}>
              <div style={styles.noteTitle} title={`${note.section} - ${note.lob}`}>
                {note.section} - {note.lob}
              </div>
              <div style={styles.noteText} title={note.notes}>
                {note.notes}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default TransformationDashboardFixed;
