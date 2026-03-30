import React, { useEffect, useState } from 'react';

const TransformationLOBPage = () => {
  const [overview, setOverview] = useState(null);
  const [lobData, setLobData] = useState([]);
  const [milestones, setMilestones] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [overviewRes, lobRes, milestonesRes] = await Promise.all([
          fetch('/api/lob_overview'),
          fetch('/api/lob_data'),
          fetch('/api/lob_milestones')
        ]);

        if (!overviewRes.ok) {
          throw new Error(`LOB overview API error: ${overviewRes.status} ${overviewRes.statusText}`);
        }
        if (!lobRes.ok) {
          throw new Error(`LOB data API error: ${lobRes.status} ${lobRes.statusText}`);
        }
        if (!milestonesRes.ok) {
          throw new Error(`LOB milestones API error: ${milestonesRes.status} ${milestonesRes.statusText}`);
        }

        const overviewData = await overviewRes.json();
        const lobData = await lobRes.json();
        const milestonesData = await milestonesRes.json();

        console.log('LOB overview:', overviewData);
        console.log('LOB data:', lobData);
        console.log('Milestones:', milestonesData);

        setOverview(overviewData);
        setLobData(lobData);
        setMilestones(milestonesData);
      } catch (err) {
        console.error('Error fetching LOB data:', err);
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  const formatDate = (dateString) => {
    if (!dateString) return '';
    const date = new Date(dateString);
    return date.toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric'
    });
  };

  const getStatusColor = (status) => {
    if (status === 'COMPLETED') return '#16a34a';
    if (status === 'IN_PROGRESS') return '#f59e0b';
    if (status === 'PENDING') return '#6b7280';
    return '#ef4444';
  };

  const getProgressColor = (planned, actual) => {
    if (!planned || !actual) return '#6b7280';
    const variance = ((actual - planned) / planned) * 100;
    if (variance >= -5) return '#16a34a';
    if (variance >= -15) return '#f59e0b';
    return '#ef4444';
  };

  const getVarianceText = (planned, actual) => {
    if (!planned || !actual) return '';
    const variance = actual - planned;
    const prefix = variance >= 0 ? '+' : '';
    return `${prefix}${variance}%`;
  };

  const getKpiColor = (value, threshold = 80) => {
    if (value >= threshold) return '#16a34a';
    if (value >= 60) return '#f59e0b';
    return '#ef4444';
  };

  const styles = {
    container: {
      minHeight: '100vh',
      background: 'linear-gradient(135deg, #f8fafc 0%, #e2e8f0 50%, #cbd5e1 100%)',
      color: '#1e293b',
      padding: '1.5rem',
      fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
      overflowX: 'hidden',
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
    cardHover: {
      transform: 'translateY(-3px)',
      boxShadow: '0 4px 20px rgba(30, 64, 175, 0.12)',
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
      minWidth: '200px',
      maxWidth: '250px',
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
  };

  if (loading) {
    return <div style={styles.container}><div style={styles.header}><h1 style={styles.title}>Loading...</h1></div></div>;
  }

  if (error) {
    return <div style={styles.container}><div style={styles.header}><h1 style={styles.title}>Error: {error}</h1></div></div>;
  }

  const lobList = ['Residential', 'Prepaid', 'Employee', 'Postpaid'];
  const lobDataMap = {};

  reconciliationData.forEach(item => {
    if (!lobDataMap[item.category]) {
      lobDataMap[item.category] = {};
    }
    lobDataMap[item.category][item.lob] = {
      planned: item.planned_value,
      actual: item.actual_value,
      status: item.status
    };
  });

  // Calculate KPI data
  const totalCategories = Object.keys(lobDataMap).length;
  const totalPlanned = reconciliationData.reduce((sum, item) => sum + (item.planned_value || 0), 0);
  const totalActual = reconciliationData.reduce((sum, item) => sum + (item.actual_value || 0), 0);
  const overallVariance = totalPlanned > 0 ? ((totalActual - totalPlanned) / totalPlanned * 100).toFixed(1) : 0;

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <h1 style={styles.title}>Transformation LOB</h1>
        <p style={styles.subtitle}>Line of Business transformation tracking</p>
      </div>

      {/* KPI Cards - Blue Background with White Text */}
      <div style={styles.statsGrid}>
        <div
          style={styles.kpiCard}
          onMouseEnter={(e) => Object.assign(e.currentTarget.style, styles.kpiCardHover)}
          onMouseLeave={(e) => {
            e.currentTarget.style.transform = 'translateY(0)';
            e.currentTarget.style.boxShadow = '0 4px 20px rgba(30, 64, 175, 0.3)';
          }}
        >
          <h3 style={styles.kpiCardTitle}>LOB Overview</h3>
          <div style={styles.kpiStatNumber}>{overview?.project_name || 'Transformation'}</div>
          <div style={styles.kpiStatLabel}>{formatDate(overview?.start_date)} - {formatDate(overview?.end_date)}</div>
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
          <div style={{...styles.kpiStatNumber, color: getKpiColor(overview?.completion_rate || 0)}}>{overview?.completion_rate || 0}%</div>
          <div style={styles.kpiMiniStats}>
            <div style={styles.kpiMiniStat}>
              <div style={{...styles.kpiMiniStatNumber, color: getKpiColor(overview?.target_rate || 0)}}>{overview?.target_rate || 0}%</div>
              <div style={styles.kpiMiniStatLabel}>Target</div>
            </div>
            <div style={styles.kpiMiniStat}>
              <div style={{...styles.kpiMiniStatNumber, color: getProgressColor(overview?.target_rate || 0, overview?.completion_rate || 0) === '#16a34a' ? '#86efac' : getProgressColor(overview?.target_rate || 0, overview?.completion_rate || 0) === '#f59e0b' ? '#fde047' : '#fca5a5'}}>
                {((overview?.completion_rate || 0) - (overview?.target_rate || 0)).toFixed(1)}%
              </div>
              <div style={styles.kpiMiniStatLabel}>Variance</div>
            </div>
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
          <h3 style={styles.kpiCardTitle}>Categories</h3>
          <div style={{...styles.kpiStatNumber, color: '#ffffff'}}>{totalCategories}</div>
          <div style={styles.kpiStatLabel}>Total categories</div>
        </div>

        <div
          style={styles.kpiCard}
          onMouseEnter={(e) => Object.assign(e.currentTarget.style, styles.kpiCardHover)}
          onMouseLeave={(e) => {
            e.currentTarget.style.transform = 'translateY(0)';
            e.currentTarget.style.boxShadow = '0 4px 20px rgba(30, 64, 175, 0.3)';
          }}
        >
          <h3 style={styles.kpiCardTitle}>Performance</h3>
          <div style={{...styles.kpiStatNumber, color: getKpiColor(100 + parseFloat(overallVariance))}}>
            {overallVariance > 0 ? '+' : ''}{overallVariance}%
          </div>
          <div style={styles.kpiStatLabel}>Overall variance</div>
        </div>
      </div>

      <div style={styles.card}>
        <h2 style={styles.cardTitle}>LOB Transformation Categories</h2>
        <div style={styles.tableContainer}>
          <table style={styles.table}>
            <thead>
              <tr>
                <th style={styles.activityHeaderCell}>
                  Category
                </th>
                {lobList.map((lob, index) => (
                  <th key={lob} colSpan="3" style={{
                    ...styles.lobHeaderGroup,
                    borderTopRightRadius: index === lobList.length - 1 ? '12px' : '0',
                    borderLeft: index > 0 ? '2px solid rgba(255,255,255,0.3)' : 'none',
                  }}>
                    {lob}
                  </th>
                ))}
              </tr>
              <tr style={styles.subHeaderRow}>
                <th style={styles.subHeaderCellFirst}>
                  Transformation Areas
                </th>
                {lobList.map(() => (
                  <React.Fragment key={`sub-${lobList.join('-')}`}>
                    <th style={styles.subHeaderCell}>Planned</th>
                    <th style={styles.subHeaderCell}>Actual</th>
                    <th style={styles.subHeaderCellLast}>Status</th>
                  </React.Fragment>
                ))}
              </tr>
            </thead>
            <tbody>
              {Object.entries(lobDataMap).map(([category, lobData]) => (
                <tr key={category} style={styles.tableRow}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.background = '#f8fafc';
                    e.currentTarget.style.transform = 'translateY(-1px)';
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.background = '#ffffff';
                    e.currentTarget.style.transform = 'translateY(0)';
                  }}
                >
                  <td style={styles.activityCell} title={category}>
                    {category}
                  </td>
                  {lobList.map((lob, lobIndex) => {
                    const planned = lobData[lob]?.planned || 0;
                    const actual = lobData[lob]?.actual || 0;
                    const status = lobData[lob]?.status || 'PENDING';
                    const color = getStatusColor(status);
                   
                    return (
                      <React.Fragment key={`${category}-${lob}`}>
                        <td style={{
                          ...styles.tableCell,
                          borderLeft: lobIndex > 0 ? '1px solid #f1f5f9' : 'none',
                          backgroundColor: '#fafafa'
                        }}>
                          <div style={styles.plannedBox}>{planned}%</div>
                        </td>
                        <td style={styles.tableCell}>
                          <div style={{
                            ...styles.actualBox,
                            backgroundColor: `${color}15`,
                            border: `1px solid ${color}40`,
                            color: color
                          }}>
                            {actual}%
                          </div>
                        </td>
                        <td style={{
                          ...styles.tableCell,
                          borderRight: lobIndex === lobList.length - 1 ? 'none' : '1px solid #f1f5f9'
                        }}>
                          <div style={{
                            ...styles.varianceText,
                            color: color,
                            fontWeight: '700'
                          }}>
                            {status}
                          </div>
                        </td>
                      </React.Fragment>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div style={styles.card}>
        <h2 style={styles.cardTitle}>LOB Milestones & Issues</h2>
        <div style={styles.notesSection}>
          {milestones.map((milestone, idx) => (
            <div key={idx} style={styles.noteCard}
              onMouseEnter={(e) => {
                e.currentTarget.style.transform = 'translateY(-2px)';
                e.currentTarget.style.boxShadow = '0 4px 20px rgba(239, 68, 68, 0.12)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.transform = 'translateY(0)';
                e.currentTarget.style.boxShadow = '0 2px 10px rgba(239, 68, 68, 0.08)';
              }}
            >
              <div style={styles.noteTitle} title={`${milestone.lob} - ${milestone.milestone}`}>
                {milestone.lob} - {milestone.milestone}
              </div>
              <div style={styles.noteText} title={milestone.description}>
                {milestone.description}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default TransformationLOBPage;