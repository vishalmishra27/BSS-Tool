import { useState } from 'react';

const tasksData = [
  {
    key: 'NBT-12',
    summary: 'Collaborate with Finance to approve budget',
    status: 'TO DO',
    assignee: { name: 'Alana Song', avatar: 'https://randomuser.me/api/portraits/women/65.jpg' },
    dueDate: '17 Feb, 2021',
    priority: 'low'
  },
  {
    key: 'NBT-14',
    summary: 'Request Marketing assets for brand refresh',
    status: 'TO DO',
    assignee: { name: 'Jie Yan', avatar: 'https://randomuser.me/api/portraits/children/2.jpg' },
    dueDate: '18 Mar, 2021',
    priority: 'low'
  },
  {
    key: 'NBT-18',
    summary: 'Set up a full Design team review',
    status: 'TO DO',
    assignee: { name: 'Fran Perez', avatar: 'https://randomuser.me/api/portraits/men/45.jpg' },
    dueDate: '29 Mar, 2021',
    priority: 'low'
  },
  {
    key: 'NBT-22',
    summary: 'Write content for blog posts and',
    status: 'TO DO',
    assignee: { name: 'Amar Sundaram', avatar: 'https://randomuser.me/api/portraits/men/32.jpg' },
    dueDate: '01 Apr, 2021',
    priority: 'medium'
  },
  {
    key: 'NBT-27',
    summary: 'Sales enablement materials and pitch',
    status: 'IN PROGRESS',
    assignee: { name: 'Alana Song', avatar: 'https://randomuser.me/api/portraits/women/65.jpg' },
    dueDate: '12 Apr, 2021',
    priority: 'medium'
  },
  {
    key: 'NBT-33',
    summary: 'Establish post-launch success',
    status: 'IN PROGRESS',
    assignee: { name: 'Jie Yan', avatar: 'https://randomuser.me/api/portraits/children/2.jpg' },
    dueDate: '12 Apr, 2021',
    priority: 'medium'
  },
  {
    key: 'NBT-35',
    summary: 'Legal contract approval for vendors',
    status: 'IN REVIEW',
    assignee: { name: 'Alana Song', avatar: 'https://randomuser.me/api/portraits/women/65.jpg' },
    dueDate: '27 Apr, 2021',
    priority: 'high'
  },
  {
    key: 'NBT-40',
    summary: 'Hire contractors for April and May',
    status: 'DONE',
    assignee: { name: 'Amar Sundaram', avatar: 'https://randomuser.me/api/portraits/men/32.jpg' },
    dueDate: '28 Apr, 2021',
    priority: 'high'
  },
  {
    key: 'NBT-43',
    summary: 'Pitch marketing campaign options',
    status: 'DONE',
    assignee: { name: 'Jie Yan', avatar: 'https://randomuser.me/api/portraits/children/2.jpg' },
    dueDate: '07 May, 2021',
    priority: 'high'
  },
  {
    key: 'NBT-44',
    summary: 'Finalize budget of The Next Big Thing',
    status: 'IN REVIEW',
    assignee: { name: 'Fran Perez', avatar: 'https://randomuser.me/api/portraits/men/45.jpg' },
    dueDate: '16 May, 2021',
    priority: 'medium'
  }
];

const AllTasks = () => {
  const [tasks, setTasks] = useState(tasksData);
  const [selectedAssignee, setSelectedAssignee] = useState('');
  const [dueFilter, setDueFilter] = useState('');
  const [showCreatePopup, setShowCreatePopup] = useState(false);
  const [newTask, setNewTask] = useState({
    key: '',
    summary: '',
    status: 'TO DO',
    assigneeName: '',
    assigneeAvatar: '',
    dueDate: '',
    priority: 'medium'
  });
  const [selectedTaskForEdit, setSelectedTaskForEdit] = useState(null);
  const statusOptions = ['TO DO', 'IN PROGRESS', 'IN REVIEW', 'DONE'];

  const handleAssigneeChange = (e) => setSelectedAssignee(e.target.value);
  const handleDueFilterChange = (e) => setDueFilter(e.target.value);

  const parseDueDate = (dueDateStr) => {
    const parts = dueDateStr.replace(/,/, '').split(' ');
    const day = parseInt(parts[0], 10);
    const month = parts[1];
    const year = parseInt(parts[2], 10);
    const monthIndex = new Date(Date.parse(month + ' 1, 2012')).getMonth();
    return new Date(year, monthIndex, day);
  };

  const filteredTasks = tasks.filter((task) => {
    const assigneeMatch = selectedAssignee ? task.assignee.name === selectedAssignee : true;
    if (!dueFilter) return assigneeMatch;
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const dueDate = parseDueDate(task.dueDate);
    if (dueFilter === '7days') {
      const sevenDaysLater = new Date();
      sevenDaysLater.setDate(today.getDate() + 7);
      return assigneeMatch && dueDate >= today && dueDate <= sevenDaysLater;
    } else if (dueFilter === 'monthly') {
      return (
        assigneeMatch &&
        dueDate.getFullYear() === today.getFullYear() &&
        dueDate.getMonth() === today.getMonth()
      );
    } else if (dueFilter === 'yearly') {
      return assigneeMatch && dueDate.getFullYear() === today.getFullYear();
    }
    return assigneeMatch;
  });

  const generateNextTaskKey = () =>
    `NBT-${Math.max(...tasks.map((t) => parseInt(t.key.split('-')[1]))) + 1}`;

  const handleCreateButtonClick = () => {
    setNewTask((prev) => ({ ...prev, key: generateNextTaskKey() }));
    setShowCreatePopup(true);
  };

  const handlePopupInputChange = (e) => {
    const { name, value } = e.target;
    setNewTask((prev) => ({ ...prev, [name]: value }));
  };

  const handleAddTask = () => {
    if (!newTask.summary || !newTask.assigneeName || !newTask.dueDate) {
      return alert('Please fill in required fields.');
    }
    const taskToAdd = {
      ...newTask,
      assignee: { name: newTask.assigneeName, avatar: newTask.assigneeAvatar || 'https://i.pravatar.cc/150' }
    };
    setTasks((prev) => [...prev, taskToAdd]);
    setShowCreatePopup(false);
  };

  const openEditTaskPopup = (task) => setSelectedTaskForEdit({ ...task });
  const closeEditTaskPopup = () => setSelectedTaskForEdit(null);

  const handleEditTaskInputChange = (e) => {
    const { name, value } = e.target;
    if (name === 'assigneeName') {
      setSelectedTaskForEdit((prev) => ({ ...prev, assignee: { ...prev.assignee, name: value } }));
    } else {
      setSelectedTaskForEdit((prev) => ({ ...prev, [name]: value }));
    }
  };

  const handleSaveEditedTask = () => {
    setTasks((prev) =>
      prev.map((t) => (t.key === selectedTaskForEdit.key ? selectedTaskForEdit : t))
    );
    closeEditTaskPopup();
  };

  const handleDeleteTask = () => {
    if (window.confirm('Are you sure?')) {
      setTasks((prev) => prev.filter((t) => t.key !== selectedTaskForEdit.key));
      closeEditTaskPopup();
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
      fontFamily: "'Segoe UI', Tahoma, Verdana, sans-serif",
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
    tr: {
      borderBottom: '1px solid rgba(255, 255, 255, 0.1)'
    },
    td: {
      padding: '15px',
      verticalAlign: 'middle',
      fontSize: '0.9rem'
    },
    keyCell: {
      backgroundColor: '#4a4e69',
      borderRadius: '8px',
      padding: '8px 12px',
      textAlign: 'center',
      fontWeight: 'bold',
      display: 'inline-block',
      cursor: 'pointer'
    },
    priorityCell: {
      borderRadius: '12px',
      padding: '5px 15px',
      textAlign: 'center',
      fontWeight: 'bold',
      color: '#fff',
      textTransform: 'uppercase',
      fontSize: '0.8rem',
      display: 'inline-block'
    },
    priorityLow: {
      backgroundColor: '#3b82f6'
    },
    priorityMedium: {
      backgroundColor: '#f97316'
    },
    priorityHigh: {
      backgroundColor: '#ef4444'
    },
    statusDone: {
      color: '#4ade80',
      display: 'flex',
      alignItems: 'center',
      gap: '5px'
    },
    assigneeCell: {
      display: 'flex',
      alignItems: 'center',
      gap: '10px'
    },
    avatar: {
      width: '32px',
      height: '32px',
      borderRadius: '50%',
      objectFit: 'cover'
    },
    popupOverlay: {
      position: 'fixed',
      top: 0,
      left: 0,
      width: '100%',
      height: '100%',
      backgroundColor: 'rgba(0,0,0,0.7)',
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
      display: 'flex',
      flexDirection: 'column',
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

  const renderPopup = (task, isEditMode) => {
    const handler = isEditMode ? handleEditTaskInputChange : handlePopupInputChange;
    return (
      <div style={styles.popupOverlay}>
        <div style={styles.popupContent}>
          <h2 style={styles.popupTitle}>{isEditMode ? 'Edit Task' : 'Create New Task'}</h2>
          <form style={styles.popupForm} onSubmit={(e) => e.preventDefault()}>
            <label style={styles.popupLabel}>
              Summary
              <input
                style={styles.popupInput}
                type="text"
                name="summary"
                value={task.summary}
                onChange={handler}
                required
              />
            </label>
            <label style={styles.popupLabel}>
              Assignee
              <input
                style={styles.popupInput}
                type="text"
                name="assigneeName"
                value={isEditMode ? task.assignee.name : task.assigneeName}
                onChange={handler}
                required
              />
            </label>
            <label style={styles.popupLabel}>
              Due Date
              <input
                style={styles.popupInput}
                type="date"
                name="dueDate"
                value={task.dueDate}
                onChange={handler}
                required
              />
            </label>
            <label style={styles.popupLabel}>
              Status
              <select style={styles.popupInput} name="status" value={task.status} onChange={handler}>
                {statusOptions.map((s) => (
                  <option key={s} value={s} style={styles.option}>
                    {s}
                  </option>
                ))}
              </select>
            </label>
          </form>
          <div style={styles.popupButtons}>
            {isEditMode && (
              <button style={styles.buttonDanger} onClick={handleDeleteTask}>
                Delete
              </button>
            )}
            <button
              style={styles.buttonSecondary}
              onClick={isEditMode ? closeEditTaskPopup : () => setShowCreatePopup(false)}
            >
              Cancel
            </button>
            <button
              style={styles.buttonPrimary}
              onClick={isEditMode ? handleSaveEditedTask : handleAddTask}
            >
              {isEditMode ? 'Save Changes' : 'Add Task'}
            </button>
          </div>
        </div>
      </div>
    );
  };

  return (
    <div style={styles.pageContainer}>
      <ScrollbarStyles />
      <main style={styles.mainContent}>
        <div style={styles.contentContainer}>
          <header style={styles.header}>
            <h1 style={styles.headerH1}>All Tasks</h1>
            <div style={styles.filterContainer}>
              <select
                style={styles.filterSelect}
                onChange={handleAssigneeChange}
                value={selectedAssignee}
              >
                <option style={styles.option} value="">
                  All Assignees
                </option>
                {[...new Set(tasksData.map((t) => t.assignee.name))].map((name) => (
                  <option key={name} value={name} style={styles.option}>
                    {name}
                  </option>
                ))}
              </select>
              <select
                style={styles.filterSelect}
                onChange={handleDueFilterChange}
                value={dueFilter}
              >
                <option style={styles.option} value="">
                  Filter by Due Date
                </option>
                <option style={styles.option} value="7days">
                  Next 7 days
                </option>
                <option style={styles.option} value="monthly">
                  This Month
                </option>
                <option style={styles.option} value="yearly">
                  This Year
                </option>
              </select>
              <button style={styles.createBtn} onClick={handleCreateButtonClick}>
                + Create
              </button>
            </div>
          </header>
          <table style={styles.table}>
            <thead>
              <tr>
                <th style={styles.th}>Key</th>
                <th style={styles.th}>Summary</th>
                <th style={styles.th}>Priority</th>
                <th style={styles.th}>Status</th>
                <th style={styles.th}>Assignee</th>
                <th style={styles.th}>Due date</th>
              </tr>
            </thead>
            <tbody>
              {filteredTasks.map((task) => (
                <tr key={task.key} style={styles.tr}>
                  <td style={styles.td}>
                    <span style={styles.keyCell} onClick={() => openEditTaskPopup(task)}>
                      {task.key}
                    </span>
                  </td>
                  <td style={styles.td}>{task.summary}</td>
                  <td style={styles.td}>
                    <span
                      style={{
                        ...styles.priorityCell,
                        ...styles[
                          `priority${task.priority.charAt(0).toUpperCase() + task.priority.slice(1)}`
                        ]
                      }}
                    >
                      {task.priority}
                    </span>
                  </td>
                  <td style={styles.td}>
                    {task.status === 'DONE' ? (
                      <div style={styles.statusDone}>
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                          <path d="M9 16.17L4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41L9 16.17z" />
                        </svg>
                        <span>Completed</span>
                      </div>
                    ) : (
                      <span>{task.status.replace('_', ' ')}</span>
                    )}
                  </td>
                  <td style={styles.td}>
                    <div style={styles.assigneeCell}>
                      <img src={task.assignee.avatar} alt={task.assignee.name} style={styles.avatar} />
                      <span>{task.assignee.name}</span>
                    </div>
                  </td>
                  <td style={styles.td}>{task.dueDate}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {showCreatePopup && renderPopup(newTask, false)}
        {selectedTaskForEdit && renderPopup(selectedTaskForEdit, true)}
      </main>
    </div>
  );
};

export default AllTasks;
