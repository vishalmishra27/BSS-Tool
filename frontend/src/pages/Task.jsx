import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';

const tasksData = [
  {
    testCase: 'TTCRM-2503',
    lob: 'Postpaid MBB',
    priority: 'Critical',
    title: 'POMBB_348 Add Booster to MBB Postpaid subscriber and check Boosters free airtime can be used as long as the main plan is active',
    description: 'ADD Internet Booster 3KD(5G) request submitted successfully -Boosters free airtime can be used as long as the main plan is active',
    validationStatus: 'OK',
    jiraStatus: 'Closed',
    kpmgStatus: 'Closed',
  },
  {
    testCase: 'TTCRM-2502',
    lob: 'Postpaid MBB',
    priority: 'Low',
    title: 'POMBB_347 Add Two Month rental discount to MBB Postpaid Customer with main plan Monthly 10KD , Monthly 18KD',
    description: 'System apply two month rental discount to MBB Postpaid Customer with main plan Monthly 10KD , Monthly 18KD',
    validationStatus: 'OK',
    jiraStatus: 'Closed',
    kpmgStatus: 'Closed',
  },
  {
    testCase: 'TTCRM-2500',
    lob: 'Postpaid MBB',
    priority: 'Major',
    title: 'POMBB_345 Create MBB Postpaid Customer with Rental Discount',
    description: 'ok',
    validationStatus: 'OK',
    jiraStatus: 'Closed',
    kpmgStatus: 'Closed',
  },
  {
    testCase: 'TTCRM-2494',
    lob: 'Postpaid MBB',
    priority: 'Major',
    title: 'POMBB_339 Test DCLM allows DSP user/customer to capture Attributes related to Product as described',
    description: 'POMBB_339 Test DCLM allows DSP user/customer to capture Attributes related to Product as described',
    validationStatus: 'OK',
    jiraStatus: 'Closed',
    kpmgStatus: 'Closed',
  },
  {
    testCase: 'TTCRM-2493',
    lob: 'Postpaid MBB',
    priority: 'Major',
    title: 'POMBB_338 successful search of a customer',
    description: 'Customer Id,Customer Name,Identification Number,Billing Account Id',
    validationStatus: 'OK',
    jiraStatus: 'Closed',
    kpmgStatus: 'Closed',
  },
  {
    testCase: 'TTCRM-2492',
    lob: 'Postpaid MBB',
    priority: 'Major',
    title: 'POMBB_337 Test Customer Search feature to check authorised user/customer search for customer profile',
    description: 'Only authorised user based on permission can search of customer profile using one from below search parameters Customer Id Customer Name Identification Number Billing Account Id',
    validationStatus: 'OK',
    jiraStatus: 'Closed',
    kpmgStatus: 'Closed',
  },
  {
    testCase: 'TTCRM-2491',
    lob: 'Postpaid MBB',
    priority: 'Major',
    title: 'POMBB_336 Update customer contact information',
    description: 'DCLM allow to update/ edit Customer Informations Any modification in billing details will be reflected in Huawei CBS',
    validationStatus: 'ok',
    jiraStatus: 'Closed',
    kpmgStatus: 'Closed',
  },
  {
    testCase: 'TTCRM-2490',
    lob: 'Postpaid MBB',
    priority: 'Major',
    title: 'POMBB_335 Update customer contact information',
    description: 'DCLM allow to update/ edit Customer Informations Any modification in billing details will be reflected in Huawei CBS.',
    validationStatus: 'ok',
    jiraStatus: 'Closed',
    kpmgStatus: 'Closed',
  },
  {
    testCase: 'TTCRM-2488',
    lob: 'Postpaid MBB',
    priority: 'Major',
    title: 'POMBB_333 Test order form should have the key information which will be available on the order',
    description: 'Customer Created successfully and can view all information on 360 View Account Created on CBS and can find all details on CBS',
    validationStatus: 'OK',
    jiraStatus: 'Closed',
    kpmgStatus: 'Closed',
  },
  {
    testCase: 'TTCRM-2487',
    lob: 'Postpaid MBB',
    priority: 'Major',
    title: 'POMBB_332 Test DCLM should provide order form (Pre-defined template for Post-paid subscription) based on the information captured during the onboarding process',
    description: 'Customer Created successfully and can view all information on 360 View Account Created on CBS and can find all details on CBS The subscription form should be available and viewable for printing/download/sharing online, under Requests (related request) and under Documents',
    validationStatus: 'ok',
    jiraStatus: 'Closed',
    kpmgStatus: 'Closed',
  },
  {
    testCase: 'TTCRM-2482',
    lob: 'Postpaid MBB',
    priority: 'Major',
    title: 'POMBB_327 If the DSP selects the Manual option, then the DCLM triggers the DRM interface to get the free service IDs list',
    description: 'System get the free service IDs list User Selects one of the service ID from the list User select SIM Card Rental fees and one time fees will have captured for this On-Boarding',
    validationStatus: 'OK',
    jiraStatus: 'Closed',
    kpmgStatus: 'Closed',
  },
  {
    testCase: 'TTCRM-2481',
    lob: 'Postpaid MBB',
    priority: 'Major',
    title: 'POMBB_326 If the DSP selects the Manual option, then the DCLM allows entering the desired service ID, Then triggers the DRM',
    description: 'Customer Created successfully and can view all information on 360 View Account Created on CBS and can find all details on CBS',
    validationStatus: 'OK',
    jiraStatus: 'Closed',
    kpmgStatus: 'Closed',
  },
  {
    testCase: 'TTCRM-2480',
    lob: 'Postpaid MBB',
    priority: 'Major',
    title: 'POMBB_325 If the DSP selects the Automatic option, then the DCLM triggers the DRM interface to select & allocate the free service ID',
    description: 'If the DSP selects the Automatic option, then the DCLM triggers the DRM interface to select & allocate the free service ID. Once after getting the request, DRM reserves a random service ID from the pool of free service IDs.',
    validationStatus: 'OK',
    jiraStatus: 'Closed',
    kpmgStatus: 'Closed',
  },
];

const statusHeaders = [
  { label: 'OPEN', className: 'status-open' },
  { label: 'REOPENED', className: 'status-reopened' },
  { label: 'CLOSED', className: 'status-closed' },
  { label: 'CANCELLED', className: 'status-cancelled' },
  { label: 'READY FOR TESTING', className: 'status-ready' },
  { label: 'NEEDS_FIX', className: 'status-needsfix' },
  { label: 'DEFECT', className: 'status-defect' },
];

const lobData = [
  { name: 'Channels', OPEN: 49, REOPENED: 0, CLOSED: 114, CANCELLED: 2, READY_FOR_TESTING: 70, NEEDS_FIX: 17, DEFECT: 10 },
  { name: 'DPOS', OPEN: 0, REOPENED: 8, CLOSED: 16, CANCELLED: 3, READY_FOR_TESTING: 6, NEEDS_FIX: 14, DEFECT: 0 },
  { name: 'DRM-Reports', OPEN: 0, REOPENED: 0, CLOSED: 6, CANCELLED: 6, READY_FOR_TESTING: 17, NEEDS_FIX: 3, DEFECT: 0 },
  { name: 'Fiber', OPEN: 2, REOPENED: 0, CLOSED: 0, CANCELLED: 0, READY_FOR_TESTING: 0, NEEDS_FIX: 0, DEFECT: 0 },
  { name: 'Finance_AR', OPEN: 0, REOPENED: 0, CLOSED: 0, CANCELLED: 3, READY_FOR_TESTING: 0, NEEDS_FIX: 0, DEFECT: 15 },
  { name: 'Finance_Business', OPEN: 0, REOPENED: 0, CLOSED: 0, CANCELLED: 0, READY_FOR_TESTING: 0, NEEDS_FIX: 0, DEFECT: 7 },
  { name: 'Finance_CACO', OPEN: 0, REOPENED: 0, CLOSED: 0, CANCELLED: 0, READY_FOR_TESTING: 1, NEEDS_FIX: 0, DEFECT: 3 },
  { name: 'Finance_CC', OPEN: 0, REOPENED: 0, CLOSED: 53, CANCELLED: 22, READY_FOR_TESTING: 9, NEEDS_FIX: 1, DEFECT: 2 },
  { name: 'Finance_RAFM', OPEN: 1, REOPENED: 6, CLOSED: 13, CANCELLED: 8, READY_FOR_TESTING: 1, NEEDS_FIX: 2, DEFECT: 39 },
];

const Task = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const currentView = searchParams.get('view') || 'all';
  const [uploadedFiles, setUploadedFiles] = useState([]);
  const [selectedStatus, setSelectedStatus] = useState('CLOSED');
  const [selectedSummaryData, setSelectedSummaryData] = useState(null);
  const [commentInput, setCommentInput] = useState('');
  const [comments, setComments] = useState([
    "Tested Successfully!",
    "Already tested by Business and working as per requirements"
  ]);

  useEffect(() => {
    const styleElement = document.createElement('style');
    styleElement.innerHTML = `
      .task-scrollbar::-webkit-scrollbar {
        width: 8px;
      }

      .task-scrollbar::-webkit-scrollbar-track {
        background: rgba(255, 255, 255, 0.1);
        border-radius: 10px;
      }

      .task-scrollbar::-webkit-scrollbar-thumb {
        background: linear-gradient(45deg, #4178d9, #3059a0);
        border-radius: 10px;
        border: 1px solid rgba(255, 255, 255, 0.2);
      }

      .task-scrollbar::-webkit-scrollbar-thumb:hover {
        background: linear-gradient(45deg, #3059a0, #00338d);
      }

      .task-scrollbar {
        scrollbar-width: thin;
        scrollbar-color: #4178d9 rgba(255, 255, 255, 0.1);
      }

      .status-select option[value="OPEN"] {
        background-color: #3b82f6 !important;
        color: #ffffff !important;
      }

      .status-select option[value="REOPENED"] {
        background-color: #f59e0b !important;
        color: #ffffff !important;
      }

      .status-select option[value="CLOSED"] {
        background-color: #22c55e !important;
        color: #ffffff !important;
      }

      .status-select option[value="CANCELED"] {
        background-color: #ef4444 !important;
        color: #ffffff !important;
      }

      .status-select option[value="READY FOR TESTING"] {
        background-color: #8b5cf6 !important;
        color: #ffffff !important;
      }

      .status-select option[value="NEED_FIX"] {
        background-color: #f97316 !important;
        color: #ffffff !important;
      }

      .status-select option[value="DEFECT"] {
        background-color: #ec4899 !important;
        color: #ffffff !important;
      }

      .status-select option {
        padding: 10px !important;
        font-weight: 600 !important;
        border: none !important;
      }
    `;

    document.head.appendChild(styleElement);
    return () => {
      if (document.head.contains(styleElement)) {
        document.head.removeChild(styleElement);
      }
    };
  }, []);

  const handleFileUpload = (event) => {
    const files = Array.from(event.target.files);
    const newFiles = files.map(file => ({
      name: file.name,
      url: URL.createObjectURL(file),
      fileObject: file,
    }));
    setUploadedFiles(prevFiles => [...prevFiles, ...newFiles]);
  };

  const handleNumberClick = async (lobName, statusLabel, count) => {
    try {
      const response = await fetch('https://jsonplaceholder.typicode.com/posts/1');
      if (!response.ok) {
        throw new Error('Network response was not ok');
      }
      const data = await response.json();
      alert(`API call successful for ${lobName} - ${statusLabel} with count ${count}`);
    } catch (error) {
      alert(`API call failed: ${error.message}`);
    }
    setSelectedSummaryData(null);
    setSearchParams({ view: 'lob' });
  };

  const handleTestCaseClick = async (task) => {
    try {
      const response = await fetch('https://jsonplaceholder.typicode.com/posts/1');
      if (!response.ok) {
        throw new Error('Network response was not ok');
      }
      const data = await response.json();
      alert(`API call successful for TestCase: ${task.testCase}`);

      setSelectedSummaryData(task);
      setSearchParams({ view: 'summary', id: task.testCase });
    } catch (error) {
      alert(`API call failed: ${error.message}`);
    }
  };

  const handleCommentChange = (e) => {
    setCommentInput(e.target.value);
  };

  const handleSendClick = () => {
    if (commentInput.trim() !== '') {
      setComments(prevComments => [...prevComments, commentInput.trim()]);
      setCommentInput('');
    }
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'OPEN':
        return { bg: '#3b82f6', border: '#2563eb' };
      case 'REOPENED':
        return { bg: '#f59e0b', border: '#d97706' };
      case 'CLOSED':
        return { bg: '#22c55e', border: '#16a34a' };
      case 'CANCELED':
        return { bg: '#ef4444', border: '#dc2626' };
      case 'READY FOR TESTING':
        return { bg: '#8b5cf6', border: '#7c3aed' };
      case 'NEED_FIX':
        return { bg: '#f97316', border: '#ea580c' };
      case 'DEFECT':
        return { bg: '#ec4899', border: '#db2777' };
      default:
        return { bg: '#6b7280', border: '#4b5563' };
    }
  };

  const styles = {
    container: {
      height: '100vh',
      background: 'linear-gradient(135deg, #00215a 0%, #1e3a8a 50%, #3059a0 100%)',
      color: '#ffffff',
      padding: '2rem',
      fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
      overflowY: 'auto',
      overflowX: 'hidden',
    },
    header: {
      marginBottom: '2rem',
      textAlign: 'center',
    },
    title: {
      fontSize: '2.5rem',
      fontWeight: '700',
      marginBottom: '0.5rem',
      background: 'linear-gradient(45deg, #ffffff, #a5b4fc)',
      WebkitBackgroundClip: 'text',
      WebkitTextFillColor: 'transparent',
      backgroundClip: 'text',
    },
    subtitle: {
      fontSize: '1.1rem',
      color: 'rgba(255, 255, 255, 0.8)',
    },
    card: {
      background: 'rgba(255, 255, 255, 0.1)',
      backdropFilter: 'blur(20px)',
      borderRadius: '20px',
      padding: '2rem',
      border: '1px solid rgba(255, 255, 255, 0.2)',
      boxShadow: '0 8px 32px rgba(0, 0, 0, 0.3)',
      marginBottom: '2rem',
    },
    cardTitle: {
      fontSize: '1.8rem',
      fontWeight: '600',
      marginBottom: '1.5rem',
      color: '#ffffff',
    },
    table: {
      width: '100%',
      borderCollapse: 'collapse',
      borderRadius: '12px',
      overflow: 'hidden',
      boxShadow: '0 4px 20px rgba(0, 0, 0, 0.3)',
    },
    tableHeader: {
      background: 'linear-gradient(135deg, #1e40af, #3730a3)',
    },
    tableHeaderCell: {
      padding: '1rem 1.5rem',
      textAlign: 'left',
      fontWeight: '600',
      fontSize: '0.9rem',
      textTransform: 'uppercase',
      letterSpacing: '0.5px',
      color: '#ffffff',
      borderBottom: '2px solid rgba(255, 255, 255, 0.2)',
    },
    tableRow: {
      background: 'rgba(255, 255, 255, 0.05)',
      borderBottom: '1px solid rgba(255, 255, 255, 0.1)',
      transition: 'all 0.3s ease',
    },
    tableRowHover: {
      background: 'rgba(255, 255, 255, 0.15)',
      transform: 'translateY(-1px)',
    },
    tableCell: {
      padding: '1rem 1.5rem',
      fontSize: '0.9rem',
      color: '#ffffff',
      verticalAlign: 'middle',
    },
    clickableNumber: {
      cursor: 'pointer',
      color: '#60a5fa',
      fontWeight: '600',
      textDecoration: 'underline',
      transition: 'all 0.3s ease',
      display: 'inline-block',
    },
    clickableTestCase: {
      cursor: 'pointer',
      color: '#a5b4fc',
      fontWeight: '600',
      textDecoration: 'underline',
      transition: 'all 0.3s ease',
    },
    priorityBadge: {
      padding: '0.3rem 0.8rem',
      borderRadius: '20px',
      fontSize: '0.75rem',
      fontWeight: '600',
      textTransform: 'uppercase',
      letterSpacing: '0.5px',
    },
    priorityCritical: {
      background: 'linear-gradient(135deg, #ef4444, #dc2626)',
      color: '#ffffff',
    },
    priorityMajor: {
      background: 'linear-gradient(135deg, #f97316, #ea580c)',
      color: '#ffffff',
    },
    priorityLow: {
      background: 'linear-gradient(135deg, #3b82f6, #2563eb)',
      color: '#ffffff',
    },
    summaryContainer: {
      display: 'grid',
      gridTemplateColumns: '2fr 1fr',
      gap: '2rem',
      alignItems: 'start',
      marginBottom: '2rem',
    },
    summaryLeft: {
      display: 'flex',
      flexDirection: 'column',
      gap: '1.5rem',
    },
    summaryRight: {
      display: 'flex',
      flexDirection: 'column',
      gap: '1.5rem',
      position: 'relative',
    },
    issueKey: {
      fontSize: '1.1rem',
      fontWeight: '600',
      color: '#a5b4fc',
      marginBottom: '0.5rem',
    },
    issueTitle: {
      fontSize: '1.8rem',
      fontWeight: '700',
      color: '#ffffff',
      lineHeight: '1.3',
      marginBottom: '1rem',
    },
    descriptionCard: {
      background: 'rgba(255, 255, 255, 0.08)',
      borderRadius: '12px',
      padding: '1.5rem',
      border: '1px solid rgba(255, 255, 255, 0.1)',
    },
    descriptionTitle: {
      fontSize: '1.1rem',
      fontWeight: '600',
      marginBottom: '1rem',
      color: '#ffffff',
    },
    descriptionList: {
      listStyle: 'none',
      padding: '0',
      margin: '0',
    },
    descriptionItem: {
      padding: '0.5rem 0',
      borderBottom: '1px solid rgba(255, 255, 255, 0.1)',
      color: 'rgba(255, 255, 255, 0.9)',
    },
    activitySection: {
      background: 'rgba(255, 255, 255, 0.08)',
      borderRadius: '12px',
      padding: '1.5rem',
      border: '1px solid rgba(255, 255, 255, 0.1)',
    },
    activityTitle: {
      fontSize: '1.1rem',
      fontWeight: '600',
      marginBottom: '1rem',
      color: '#ffffff',
    },
    commentBox: {
      marginBottom: '1.5rem',
    },
    textarea: {
      width: '100%',
      minHeight: '80px',
      background: 'rgba(0, 0, 0, 0.3)',
      border: '1px solid rgba(255, 255, 255, 0.2)',
      borderRadius: '8px',
      padding: '0.75rem',
      color: '#ffffff',
      fontSize: '0.9rem',
      resize: 'vertical',
      outline: 'none',
      transition: 'border-color 0.3s ease',
    },
    sendButton: {
      background: 'linear-gradient(135deg, #3b82f6, #2563eb)',
      color: '#ffffff',
      border: 'none',
      borderRadius: '8px',
      padding: '0.6rem 1.2rem',
      fontSize: '0.9rem',
      fontWeight: '600',
      cursor: 'pointer',
      transition: 'all 0.3s ease',
      marginTop: '0.5rem',
      float: 'right',
    },
    comment: {
      background: 'rgba(255, 255, 255, 0.05)',
      borderRadius: '8px',
      padding: '1rem',
      marginBottom: '1rem',
      border: '1px solid rgba(255, 255, 255, 0.1)',
    },
    commentAuthor: {
      fontWeight: '600',
      color: '#a5b4fc',
      marginBottom: '0.25rem',
    },
    commentDate: {
      fontSize: '0.75rem',
      color: 'rgba(255, 255, 255, 0.6)',
      marginBottom: '0.5rem',
    },
    commentText: {
      color: 'rgba(255, 255, 255, 0.9)',
      lineHeight: '1.5',
    },
    statusDropdown: {
      width: '100%',
      background: getStatusColor(selectedStatus).bg,
      border: `2px solid ${getStatusColor(selectedStatus).border}`,
      borderRadius: '8px',
      padding: '0.75rem',
      color: '#ffffff',
      fontSize: '0.9rem',
      fontWeight: '600',
      outline: 'none',
      position: 'relative',
      zIndex: 1000,
      appearance: 'auto',
    },
    detailsCard: {
      background: 'rgba(255, 255, 255, 0.08)',
      borderRadius: '12px',
      padding: '1.5rem',
      border: '1px solid rgba(255, 255, 255, 0.1)',
    },
    detailsTitle: {
      fontSize: '1.1rem',
      fontWeight: '600',
      marginBottom: '1rem',
      color: '#ffffff',
    },
    detailRow: {
      display: 'flex',
      justifyContent: 'space-between',
      padding: '0.5rem 0',
      borderBottom: '1px solid rgba(255, 255, 255, 0.1)',
    },
    detailLabel: {
      color: 'rgba(255, 255, 255, 0.7)',
      fontSize: '0.85rem',
    },
    detailValue: {
      color: '#ffffff',
      fontSize: '0.85rem',
      fontWeight: '500',
    },
    attachmentsCard: {
      background: 'rgba(255, 255, 255, 0.08)',
      borderRadius: '12px',
      padding: '1.5rem',
      border: '1px solid rgba(255, 255, 255, 0.1)',
    },
    fileInput: {
      width: '100%',
      padding: '0.75rem',
      background: 'rgba(255, 255, 255, 0.1)',
      border: '2px dashed rgba(255, 255, 255, 0.3)',
      borderRadius: '8px',
      color: '#ffffff',
      cursor: 'pointer',
      transition: 'all 0.3s ease',
    },
    attachmentItem: {
      padding: '0.5rem 0',
      color: '#a5b4fc',
      fontSize: '0.85rem',
    },
  };

  const renderAllTestCases = () => (
    <div style={styles.card}>
      <h2 style={styles.cardTitle}>Two Dimensional Filter Statistics: ALL TestCases</h2>
      <div style={{ overflowX: 'auto' }}>
        <table style={styles.table}>
          <thead style={styles.tableHeader}>
            <tr>
              <th style={{ ...styles.tableHeaderCell, borderTopLeftRadius: '12px' }}>LoB</th>
              {statusHeaders.map((status, index) => (
                <th
                  key={status.label}
                  style={{
                    ...styles.tableHeaderCell,
                    textAlign: 'center',
                    borderTopRightRadius: index === statusHeaders.length - 1 ? '12px' : '0',
                  }}
                >
                  {status.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {lobData.map((lob, idx) => (
              <tr
                key={idx}
                style={styles.tableRow}
                onMouseEnter={(e) => {
                  e.currentTarget.style.background = 'rgba(255, 255, 255, 0.15)';
                  e.currentTarget.style.transform = 'translateY(-1px)';
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.background = 'rgba(255, 255, 255, 0.05)';
                  e.currentTarget.style.transform = 'translateY(0)';
                }}
              >
                <td style={styles.tableCell}>{lob.name}</td>
                {statusHeaders.map((status) => {
                  const key = status.label.replace(/ /g, '_');
                  const count = lob[key] || 0;
                  return (
                    <td key={status.label} style={{ ...styles.tableCell, textAlign: 'center' }}>
                      <span
                        style={styles.clickableNumber}
                        onClick={() => handleNumberClick(lob.name, status.label, count)}
                        onMouseEnter={(e) => {
                          e.target.style.color = '#3b82f6';
                          e.target.style.transform = 'scale(1.1)';
                        }}
                        onMouseLeave={(e) => {
                          e.target.style.color = '#60a5fa';
                          e.target.style.transform = 'scale(1)';
                        }}
                      >
                        {count}
                      </span>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );

  const renderLobView = () => (
    <div style={styles.card}>
      <h2 style={styles.cardTitle}>All Lobs - List View</h2>
      <div style={{ overflowX: 'auto' }}>
        <table style={styles.table}>
          <thead style={styles.tableHeader}>
            <tr>
              <th style={{ ...styles.tableHeaderCell, borderTopLeftRadius: '12px' }}>Test Case</th>
              <th style={styles.tableHeaderCell}>LoB</th>
              <th style={styles.tableHeaderCell}>Priority</th>
              <th style={styles.tableHeaderCell}>Title</th>
              <th style={styles.tableHeaderCell}>Validation Status</th>
              <th style={styles.tableHeaderCell}>Client Status</th>
              <th style={{ ...styles.tableHeaderCell, borderTopRightRadius: '12px' }}>KPMG Status</th>
            </tr>
          </thead>
          <tbody>
            {tasksData.map((task, idx) => (
              <tr
                key={idx}
                style={styles.tableRow}
                onMouseEnter={(e) => {
                  e.currentTarget.style.background = 'rgba(255, 255, 255, 0.15)';
                  e.currentTarget.style.transform = 'translateY(-1px)';
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.background = 'rgba(255, 255, 255, 0.05)';
                  e.currentTarget.style.transform = 'translateY(0)';
                }}
              >
                <td style={styles.tableCell}>
                  <span
                    style={styles.clickableTestCase}
                    onClick={() => handleTestCaseClick(task)}
                    onMouseEnter={(e) => {
                      e.target.style.color = '#8b5cf6';
                      e.target.style.textDecoration = 'none';
                    }}
                    onMouseLeave={(e) => {
                      e.target.style.color = '#a5b4fc';
                      e.target.style.textDecoration = 'underline';
                    }}
                  >
                    {task.testCase}
                  </span>
                </td>
                <td style={styles.tableCell}>{task.lob}</td>
                <td style={styles.tableCell}>
                  <span
                    style={{
                      ...styles.priorityBadge,
                      ...(task.priority === 'Critical'
                        ? styles.priorityCritical
                        : task.priority === 'Major'
                        ? styles.priorityMajor
                        : styles.priorityLow),
                    }}
                  >
                    {task.priority}
                  </span>
                </td>
                <td style={styles.tableCell}>{task.title}</td>
                <td style={styles.tableCell}>
                  <span style={{ color: '#22c55e', fontWeight: '600' }}>✓ {task.validationStatus}</span>
                </td>
                <td style={styles.tableCell}>{task.jiraStatus}</td>
                <td style={styles.tableCell}>{task.kpmgStatus}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );

  const renderSummaryView = () => {
    if (!selectedSummaryData) {
      return (
        <div style={styles.card}>
          <p style={{ textAlign: 'center', fontSize: '1.1rem' }}>
            No Test Case selected. Please click on a Test Case from the LoB view.
          </p>
        </div>
      );
    }
    return (
      <div style={styles.summaryContainer}>
        <div style={styles.summaryLeft}>
          <div>
            <div style={styles.issueKey}>{selectedSummaryData.testCase}</div>
            <h2 style={styles.issueTitle}>{selectedSummaryData.title}</h2>
          </div>

          <div style={styles.descriptionCard}>
            <h3 style={styles.descriptionTitle}>Description</h3>
            <ul style={styles.descriptionList}>
              <li style={styles.descriptionItem}>LoB: {selectedSummaryData.lob}</li>
              <li style={styles.descriptionItem}>Validation Status: {selectedSummaryData.validationStatus}</li>
              <li style={styles.descriptionItem}>Client Status: {selectedSummaryData.jiraStatus}</li>
              <li style={styles.descriptionItem}>KPMG Status: {selectedSummaryData.kpmgStatus}</li>
            </ul>
          </div>

          <div style={styles.activitySection}>
            <h3 style={styles.activityTitle}>Activity</h3>
            <div style={styles.commentBox}>
              <textarea
                style={styles.textarea}
                placeholder="Add a comment..."
                value={commentInput}
                onChange={handleCommentChange}
                onFocus={(e) => (e.target.style.borderColor = 'rgba(59, 130, 246, 0.5)')}
                onBlur={(e) => (e.target.style.borderColor = 'rgba(255, 255, 255, 0.2)')}
              />
              <button
                style={styles.sendButton}
                onClick={handleSendClick}
                onMouseEnter={(e) => {
                  e.target.style.background = 'linear-gradient(135deg, #2563eb, #1d4ed8)';
                  e.target.style.transform = 'translateY(-1px)';
                }}
                onMouseLeave={(e) => {
                  e.target.style.background = 'linear-gradient(135deg, #3b82f6, #2563eb)';
                  e.target.style.transform = 'translateY(0)';
                }}
              >
                Send
              </button>
              <div style={{ clear: 'both' }}></div>
            </div>

            <div style={styles.comment}>
              <div style={styles.commentAuthor}>parag muley</div>
              <div style={styles.commentDate}>April 15, 2025 at 8:03 AM</div>
              <div style={styles.commentText}>
                <ul style={{ margin: 0, paddingLeft: '1.5rem' }}>
                  {comments.map((comment, index) => (
                    <li key={index} style={{ marginBottom: '0.5rem' }}>
                      {comment}
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          </div>
        </div>

        <div style={styles.summaryRight}>
          <select
            style={styles.statusDropdown}
            className="status-select"
            value={selectedStatus}
            onChange={(e) => setSelectedStatus(e.target.value)}
          >
            <option value="OPEN">OPEN</option>
            <option value="REOPENED">REOPENED</option>
            <option value="CLOSED">CLOSED</option>
            <option value="CANCELED">CANCELED</option>
            <option value="READY FOR TESTING">READY FOR TESTING</option>
            <option value="NEED_FIX">NEED_FIX</option>
            <option value="DEFECT">DEFECT</option>
          </select>

          <div style={styles.detailsCard}>
            <h4 style={styles.detailsTitle}>Details</h4>
            <div style={styles.detailRow}>
              <span style={styles.detailLabel}>Assignee</span>
              <span style={styles.detailValue}>parag muley</span>
            </div>
            <div style={styles.detailRow}>
              <span style={styles.detailLabel}>Reporter</span>
              <span style={styles.detailValue}>ahmed ragab</span>
            </div>
            <div style={styles.detailRow}>
              <span style={styles.detailLabel}>Priority</span>
              <span
                style={{
                  ...styles.detailValue,
                  ...(selectedSummaryData.priority === 'Critical'
                    ? { color: '#ef4444' }
                    : selectedSummaryData.priority === 'Major'
                    ? { color: '#f97316' }
                    : { color: '#3b82f6' }),
                }}
              >
                {selectedSummaryData.priority}
              </span>
            </div>
            <div style={styles.detailRow}>
              <span style={styles.detailLabel}>Requirement Type</span>
              <span style={styles.detailValue}>Integration</span>
            </div>
            <div style={styles.detailRow}>
              <span style={styles.detailLabel}>Reference SRS</span>
              <span style={styles.detailValue}>Postpaid Voice SRS</span>
            </div>
            <div style={{ ...styles.detailRow, borderBottom: 'none' }}>
              <span style={styles.detailLabel}>Phase Detected</span>
              <span style={styles.detailValue}>User Acceptance Testing (UAT)</span>
            </div>
          </div>

          <div style={styles.attachmentsCard}>
            <h4 style={styles.detailsTitle}>Attachments</h4>
            <input
              type="file"
              multiple
              onChange={handleFileUpload}
              style={styles.fileInput}
              onMouseEnter={(e) => {
                e.target.style.background = 'rgba(255, 255, 255, 0.15)';
                e.target.style.borderColor = 'rgba(59, 130, 246, 0.5)';
              }}
              onMouseLeave={(e) => {
                e.target.style.background = 'rgba(255, 255, 255, 0.1)';
                e.target.style.borderColor = 'rgba(255, 255, 255, 0.3)';
              }}
            />
            <div style={{ marginTop: '1rem' }}>
              {uploadedFiles.map((attachment, index) => (
                <div key={index} style={styles.attachmentItem}>
                  📎 {attachment.name}
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    );
  };

  return (
    <div style={styles.container} className="task-scrollbar">
      <div style={styles.header}>
        <h1 style={styles.title}>Test Case Management</h1>
        <p style={styles.subtitle}>Comprehensive test case tracking and management system</p>
      </div>

      {currentView === 'summary' && renderSummaryView()}
      {currentView === 'lob' && renderLobView()}
      {(currentView === 'all' || !currentView) && renderAllTestCases()}
    </div>
  );
};

export default Task;
