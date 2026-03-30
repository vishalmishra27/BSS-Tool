import { useState } from 'react';

const AllProjects = () => {
  const [projectsData, setProjectsData] = useState([
    {
      projectCode: 'PRJ-001',
      projectTitle: 'Project Alpha',
      description: 'Develop a new mobile application for iOS and Android platforms.',
      assignToName: 'John Doe',
      startDate: '2023-01-01',
      endDate: '2023-06-30',
      attachmentName: 'alpha-brief.pdf'
    },
    {
      projectCode: 'PRJ-002',
      projectTitle: 'Project Beta',
      description: 'Migrate the legacy database system to a cloud-based infrastructure.',
      assignToName: 'Jane Smith',
      startDate: '2023-02-15',
      endDate: '2023-08-15',
      attachmentName: 'beta-plan.docx'
    },
    {
      projectCode: 'PRJ-003',
      projectTitle: 'Project Gamma',
      description: 'Complete redesign of the corporate website with a new CMS.',
      assignToName: 'Alana Song',
      startDate: '2023-03-10',
      endDate: '2023-09-30',
      attachmentName: ''
    },
    {
      projectCode: 'PRJ-004',
      projectTitle: 'Project Delta',
      description: 'Launch a new marketing campaign for the Q4 product release.',
      assignToName: 'Amar Sundaram',
      startDate: '2023-04-01',
      endDate: '2023-10-31',
      attachmentName: 'delta-campaign.zip'
    }
  ]);

  const [selectedAssignToName, setSelectedAssignToName] = useState('');
  const [selectedDueDateFilter, setSelectedDueDateFilter] = useState('');
  const [showCreateProjectPopup, setShowCreateProjectPopup] = useState(false);
  const [newProject, setNewProject] = useState({
    projectCode: '',
    projectTitle: '',
    description: '',
    assignToName: '',
    startDate: '',
    endDate: '',
    attachmentFile: null,
    attachmentName: ''
  });
  const [selectedProjectForEdit, setSelectedProjectForEdit] = useState(null);

  const filteredProjects = projectsData.filter((project) => {
    let assignToNameMatch = true;
    let dueDateMatch = true;

    if (selectedAssignToName) {
      assignToNameMatch = project.assignToName === selectedAssignToName;
    }

    if (selectedDueDateFilter) {
      const today = new Date();
      const projectEndDate = new Date(project.endDate);
      if (selectedDueDateFilter === '7days') {
        const sevenDaysLater = new Date();
        sevenDaysLater.setDate(today.getDate() + 7);
        dueDateMatch = projectEndDate >= today && projectEndDate <= sevenDaysLater;
      } else if (selectedDueDateFilter === 'monthly') {
        dueDateMatch = projectEndDate.getFullYear() === today.getFullYear() && projectEndDate.getMonth() === today.getMonth();
      } else if (selectedDueDateFilter === 'yearly') {
        dueDateMatch = projectEndDate.getFullYear() === today.getFullYear();
      }
    }

    return assignToNameMatch && dueDateMatch;
  });

  const handleNewProjectInputChange = (e) => {
    const { name, value } = e.target;
    setNewProject((prev) => ({ ...prev, [name]: value }));
  };

  const handleNewProjectFileChange = (e) => {
    const file = e.target.files[0];
    if (file) {
      setNewProject((prev) => ({ ...prev, attachmentFile: file, attachmentName: file.name }));
    }
  };

  const handleAddProject = () => {
    if (!newProject.projectTitle || !newProject.assignToName || !newProject.startDate || !newProject.endDate) {
      alert('Please fill in all required fields.');
      return;
    }
    setProjectsData((prev) => [...prev, { ...newProject, projectCode: generateNextProjectCode() }]);
    setShowCreateProjectPopup(false);
    setNewProject({
      projectCode: '',
      projectTitle: '',
      description: '',
      assignToName: '',
      startDate: '',
      endDate: '',
      attachmentFile: null,
      attachmentName: ''
    });
  };

  const generateNextProjectCode = () => `PRJ-${String(projectsData.length + 1).padStart(3, '0')}`;

  const openCreateProjectPopup = () => {
    setNewProject((prev) => ({ ...prev, projectCode: generateNextProjectCode() }));
    setShowCreateProjectPopup(true);
  };

  const handleCancelProject = () => setShowCreateProjectPopup(false);

  const openEditProjectPopup = (project) => setSelectedProjectForEdit({ ...project });

  const closeEditProjectPopup = () => setSelectedProjectForEdit(null);

  const handleEditProjectInputChange = (e) => {
    const { name, value } = e.target;
    setSelectedProjectForEdit((prev) => ({ ...prev, [name]: value }));
  };

  const handleEditProjectFileChange = (e) => {
    const file = e.target.files[0];
    if (file) {
      setSelectedProjectForEdit((prev) => ({ ...prev, attachmentName: file.name }));
    }
  };

  const handleSaveEditedProject = () => {
    setProjectsData((prev) =>
      prev.map((p) => (p.projectCode === selectedProjectForEdit.projectCode ? selectedProjectForEdit : p))
    );
    closeEditProjectPopup();
  };

  const handleDeleteProject = () => {
    if (window.confirm('Are you sure you want to delete this project?')) {
      setProjectsData((prev) => prev.filter((p) => p.projectCode !== selectedProjectForEdit.projectCode));
      closeEditProjectPopup();
    }
  };

  const ScrollbarStyles = () => (
    <style>{`
      ::-webkit-scrollbar { width: 10px; }
      ::-webkit-scrollbar-track { background: rgba(29, 42, 71, 0.5); border-radius: 10px; }
      ::-webkit-scrollbar-thumb { background: #4a3c85; border-radius: 10px; }
      ::-webkit-scrollbar-thumb:hover { background: #6a5acd; }
    `}</style>
  );

  const styles = {
    pageContainer: {
      display: 'flex',
      flexDirection: 'column',
      fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
      height: '100vh',
      background: 'linear-gradient(135deg, #1e3c72, #2a5298)',
      color: '#e0e0e0'
    },
    mainContent: {
      flexGrow: 1,
      padding: '30px 40px',
      overflowY: 'auto'
    },
    contentContainer: {
      backgroundColor: 'rgba(29, 42, 71, 0.7)',
      borderRadius: '20px',
      padding: '25px 35px',
      backdropFilter: 'blur(10px)',
      border: '1px solid rgba(255, 255, 255, 0.1)'
    },
    header: {
      display: 'flex',
      justifyContent: 'space-between',
      alignItems: 'center',
      marginBottom: '25px'
    },
    headerH1: {
      fontWeight: '700',
      fontSize: '1.8rem',
      color: '#ffffff'
    },
    filterContainer: {
      display: 'flex',
      gap: '15px'
    },
    filterSelect: {
      backgroundColor: 'rgba(255, 255, 255, 0.1)',
      border: '1px solid rgba(255, 255, 255, 0.2)',
      borderRadius: '20px',
      padding: '8px 35px 8px 20px',
      fontWeight: '600',
      fontSize: '0.9rem',
      color: '#e0e0e0',
      cursor: 'pointer',
      appearance: 'none',
      backgroundImage: `url("data:image/svg+xml;charset=US-ASCII,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' fill='none' stroke='%23e0e0e0' stroke-linecap='round' stroke-linejoin='round' stroke-width='2'%3E%3Cpath d='m6 9 6-6H0z'/%3E%3C/svg%3E")`,
      backgroundRepeat: 'no-repeat',
      backgroundPosition: 'right 15px center',
      backgroundSize: '10px'
    },
    option: {
      backgroundColor: '#1e3c72',
      color: '#e0e0e0'
    },
    createBtn: {
      backgroundColor: '#4a3c85',
      border: 'none',
      borderRadius: '20px',
      padding: '8px 25px',
      fontWeight: 'bold',
      fontSize: '0.9rem',
      color: '#ffffff',
      cursor: 'pointer',
      transition: 'background-color 0.3s ease'
    },
    table: {
      width: '100%',
      borderCollapse: 'collapse'
    },
    th: {
      padding: '15px',
      textAlign: 'left',
      fontSize: '0.8rem',
      color: '#c0c0c0',
      textTransform: 'uppercase',
      letterSpacing: '1px',
      borderBottom: '2px solid #4a3c85'
    },
    td: {
      padding: '15px',
      verticalAlign: 'middle',
      borderBottom: '1px solid rgba(255, 255, 255, 0.1)',
      fontSize: '0.9rem'
    },
    projectCodeCell: {
      color: '#82aaff',
      fontWeight: 'bold',
      cursor: 'pointer',
      textDecoration: 'none'
    },
    attachmentCell: {
      display: 'flex',
      alignItems: 'center',
      gap: '8px',
      color: '#a0a0a0',
      fontStyle: 'italic'
    },
    popupOverlay: {
      position: 'fixed',
      top: 0,
      left: 0,
      width: '100%',
      height: '100%',
      backgroundColor: 'rgba(0, 0, 0, 0.7)',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
      zIndex: 1000,
      backdropFilter: 'blur(5px)'
    },
    popupContent: {
      backgroundColor: 'rgba(29, 42, 71, 0.9)',
      padding: '30px',
      borderRadius: '12px',
      width: '450px',
      maxWidth: '90%',
      border: '1px solid rgba(255, 255, 255, 0.2)',
      boxShadow: '0 8px 32px 0 rgba(0, 0, 0, 0.37)'
    },
    popupTitle: {
      marginTop: 0,
      marginBottom: '25px',
      color: '#ffffff'
    },
    popupForm: {
      display: 'grid',
      gridTemplateColumns: '1fr 1fr',
      gap: '15px'
    },
    popupLabel: {
      display: 'flex',
      flexDirection: 'column',
      gap: '5px',
      fontSize: '0.9rem',
      color: '#c0c0c0'
    },
    popupInput: {
      padding: '10px',
      borderRadius: '5px',
      border: '1px solid #4a3c85',
      background: 'rgba(0,0,0,0.2)',
      color: '#e0e0e0',
      fontSize: '0.9rem'
    },
    popupDescription: {
      gridColumn: '1 / -1'
    },
    popupButtons: {
      display: 'flex',
      justifyContent: 'flex-end',
      gap: '10px',
      marginTop: '25px'
    },
    buttonPrimary: {
      padding: '10px 20px',
      borderRadius: '5px',
      border: 'none',
      background: '#4a3c85',
      color: '#fff',
      cursor: 'pointer'
    },
    buttonSecondary: {
      padding: '10px 20px',
      borderRadius: '5px',
      border: 'none',
      background: '#555',
      color: '#fff',
      cursor: 'pointer'
    },
    buttonDanger: {
      padding: '10px 20px',
      borderRadius: '5px',
      border: 'none',
      background: '#b91c1c',
      color: '#fff',
      cursor: 'pointer',
      marginRight: 'auto'
    }
  };

  const renderPopup = (project, isEditMode) => (
    <div style={styles.popupOverlay}>
      <div style={styles.popupContent}>
        <h2 style={styles.popupTitle}>{isEditMode ? 'Edit Project' : 'Create New Project'}</h2>
        <form style={styles.popupForm}>
          <label style={styles.popupLabel}>
            Project Code
            <input style={styles.popupInput} type="text" value={project.projectCode} disabled />
          </label>
          <label style={styles.popupLabel}>
            Project Title
            <input
              style={styles.popupInput}
              type="text"
              name="projectTitle"
              value={project.projectTitle}
              onChange={isEditMode ? handleEditProjectInputChange : handleNewProjectInputChange}
              required
            />
          </label>
          <label style={{ ...styles.popupLabel, ...styles.popupDescription }}>
            Description
            <textarea
              style={{ ...styles.popupInput, minHeight: '80px', resize: 'vertical' }}
              name="description"
              value={project.description}
              onChange={isEditMode ? handleEditProjectInputChange : handleNewProjectInputChange}
              required
            />
          </label>
          <label style={styles.popupLabel}>
            Assign To
            <input
              style={styles.popupInput}
              type="text"
              name="assignToName"
              value={project.assignToName}
              onChange={isEditMode ? handleEditProjectInputChange : handleNewProjectInputChange}
              required
            />
          </label>
          <label style={styles.popupLabel}>
            Attachment
            <input
              style={{ ...styles.popupInput, padding: '8px' }}
              type="file"
              onChange={isEditMode ? handleEditProjectFileChange : handleNewProjectFileChange}
            />
          </label>
          <label style={styles.popupLabel}>
            Start Date
            <input
              style={styles.popupInput}
              type="date"
              name="startDate"
              value={project.startDate}
              onChange={isEditMode ? handleEditProjectInputChange : handleNewProjectInputChange}
              required
            />
          </label>
          <label style={styles.popupLabel}>
            End Date
            <input
              style={styles.popupInput}
              type="date"
              name="endDate"
              value={project.endDate}
              onChange={isEditMode ? handleEditProjectInputChange : handleNewProjectInputChange}
              required
            />
          </label>
        </form>
        <div style={styles.popupButtons}>
          {isEditMode && (
            <button style={styles.buttonDanger} onClick={handleDeleteProject}>
              Delete
            </button>
          )}
          <button
            style={styles.buttonSecondary}
            onClick={isEditMode ? closeEditProjectPopup : handleCancelProject}
          >
            Cancel
          </button>
          <button
            style={styles.buttonPrimary}
            onClick={isEditMode ? handleSaveEditedProject : handleAddProject}
          >
            {isEditMode ? 'Save Changes' : 'Add Project'}
          </button>
        </div>
      </div>
    </div>
  );

  return (
    <div style={styles.pageContainer}>
      <ScrollbarStyles />
      <main style={styles.mainContent}>
        <div style={styles.contentContainer}>
          <header style={styles.header}>
            <h1 style={styles.headerH1}>All Projects</h1>
            <div style={styles.filterContainer}>
              <select
                style={styles.filterSelect}
                value={selectedAssignToName}
                onChange={(e) => setSelectedAssignToName(e.target.value)}
              >
                <option style={styles.option} value="">
                  All Assignees
                </option>
                {Array.from(new Set(projectsData.map((p) => p.assignToName))).map((name) => (
                  <option key={name} value={name} style={styles.option}>
                    {name}
                  </option>
                ))}
              </select>
              <select
                style={styles.filterSelect}
                value={selectedDueDateFilter}
                onChange={(e) => setSelectedDueDateFilter(e.target.value)}
              >
                <option style={styles.option} value="">
                  Filter by Due Date
                </option>
                <option style={styles.option} value="7days">
                  Next 7 Days
                </option>
                <option style={styles.option} value="monthly">
                  This Month
                </option>
                <option style={styles.option} value="yearly">
                  This Year
                </option>
              </select>
              <button style={styles.createBtn} onClick={openCreateProjectPopup}>
                + Create Project
              </button>
            </div>
          </header>
          <table style={styles.table}>
            <thead>
              <tr>
                <th style={styles.th}>Code</th>
                <th style={styles.th}>Project Title</th>
                <th style={styles.th}>Assignee</th>
                <th style={styles.th}>End Date</th>
                <th style={styles.th}>Attachment</th>
              </tr>
            </thead>
            <tbody>
              {filteredProjects.map((project) => (
                <tr key={project.projectCode}>
                  <td style={styles.td}>
                    <a
                      href="#"
                      style={styles.projectCodeCell}
                      onClick={(e) => {
                        e.preventDefault();
                        openEditProjectPopup(project);
                      }}
                    >
                      {project.projectCode}
                    </a>
                  </td>
                  <td style={styles.td}>{project.projectTitle}</td>
                  <td style={styles.td}>{project.assignToName}</td>
                  <td style={styles.td}>{project.endDate}</td>
                  <td style={styles.td}>
                    {project.attachmentName && (
                      <div style={styles.attachmentCell}>
                        <svg
                          xmlns="http://www.w3.org/2000/svg"
                          width="16"
                          height="16"
                          fill="currentColor"
                          viewBox="0 0 16 16"
                        >
                          <path d="M4.5 3a2.5 2.5 0 0 1 5 0v9a1.5 1.5 0 0 1-3 0V5a.5.5 0 0 1 1 0v7a.5.5 0 0 0 1 0V3a1.5 1.5 0 1 0-3 0v9a2.5 2.5 0 0 0 5 0V5A.5.5 0 0 1 11 5v7a3.5 3.5 0 1 1-7 0z" />
                        </svg>
                        <span>{project.attachmentName}</span>
                      </div>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </main>
      {showCreateProjectPopup && renderPopup(newProject, false)}
      {selectedProjectForEdit && renderPopup(selectedProjectForEdit, true)}
    </div>
  );
};

export default AllProjects;
