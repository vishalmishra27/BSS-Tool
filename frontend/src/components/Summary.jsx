import { useState } from 'react';

const Summary = () => {
  const [selectedUser, setSelectedUser] = useState(null);

  const userData = [
    {
      id: 1,
      name: 'Alana Song',
      avatar: 'https://randomuser.me/api/portraits/women/65.jpg',
      role: 'Project Manager',
      tasks: [
        { id: 'T-001', title: 'Review Q4 budget proposal', status: 'In Progress', priority: 'High', dueDate: '2024-01-15' },
        { id: 'T-002', title: 'Finalize marketing campaign', status: 'To Do', priority: 'Medium', dueDate: '2024-01-20' },
        { id: 'T-003', title: 'Team performance review', status: 'Completed', priority: 'Low', dueDate: '2024-01-10' }
      ]
    },
    {
      id: 2,
      name: 'Jie Yan',
      avatar: 'https://randomuser.me/api/portraits/children/2.jpg',
      role: 'Developer',
      tasks: [
        { id: 'T-004', title: 'Implement new API endpoints', status: 'In Progress', priority: 'High', dueDate: '2024-01-18' },
        { id: 'T-005', title: 'Fix login bug', status: 'Completed', priority: 'Medium', dueDate: '2024-01-12' },
        { id: 'T-006', title: 'Code review for PR #123', status: 'To Do', priority: 'Medium', dueDate: '2024-01-22' }
      ]
    },
    {
      id: 3,
      name: 'Fran Perez',
      avatar: 'https://randomuser.me/api/portraits/men/45.jpg',
      role: 'Designer',
      tasks: [
        { id: 'T-007', title: 'Create new dashboard mockup', status: 'In Progress', priority: 'High', dueDate: '2024-01-16' },
        { id: 'T-008', title: 'Update brand guidelines', status: 'To Do', priority: 'Low', dueDate: '2024-01-25' }
      ]
    },
    {
      id: 4,
      name: 'Amar Sundaram',
      avatar: 'https://randomuser.me/api/portraits/men/32.jpg',
      role: 'QA Engineer',
      tasks: [
        { id: 'T-009', title: 'Test new feature release', status: 'In Progress', priority: 'High', dueDate: '2024-01-17' },
        { id: 'T-010', title: 'Regression testing', status: 'Completed', priority: 'Medium', dueDate: '2024-01-14' }
      ]
    }
  ];

  const getStatusColor = (status) => {
    switch(status) {
      case 'Completed': return '#4ade80';
      case 'In Progress': return '#f97316';
      case 'To Do': return '#3b82f6';
      default: return '#6b7280';
    }
  };

  const getPriorityColor = (priority) => {
    switch(priority) {
      case 'High': return '#ef4444';
      case 'Medium': return '#f97316';
      case 'Low': return '#3b82f6';
      default: return '#6b7280';
    }
  };

  return (
    <div style={{ padding: '2rem', fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif", height: '100vh', overflowY: 'auto' }}>
      <h1 style={{ color: '#1e3c72', marginBottom: '1.5rem' }}>BPM Summary</h1>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '1.5rem', marginBottom: '2rem' }}>
        <div style={{ background: '#fff', padding: '1.5rem', borderRadius: '12px', boxShadow: '0 4px 6px rgba(0,0,0,0.1)', border: '1px solid #e1e4eb' }}>
          <h3 style={{ color: '#1e3c72', marginBottom: '1rem' }}>Process Summary</h3>
          <div style={{ display: 'grid', gap: '0.5rem' }}>
            <div><strong>Total Processes:</strong> 24</div>
            <div><strong>Active Processes:</strong> 12</div>
            <div><strong>Completed Processes:</strong> 8</div>
            <div><strong>Pending Review:</strong> 4</div>
          </div>
        </div>

        <div style={{ background: '#fff', padding: '1.5rem', borderRadius: '12px', boxShadow: '0 4px 6px rgba(0,0,0,0.1)', border: '1px solid #e1e4eb' }}>
          <h3 style={{ color: '#1e3c72', marginBottom: '1rem' }}>Project Summary</h3>
          <div style={{ display: 'grid', gap: '0.5rem' }}>
            <div><strong>Total Projects:</strong> 8</div>
            <div><strong>In Progress:</strong> 5</div>
            <div><strong>Completed:</strong> 2</div>
            <div><strong>On Hold:</strong> 1</div>
          </div>
        </div>

        <div style={{ background: '#fff', padding: '1.5rem', borderRadius: '12px', boxShadow: '0 4px 6px rgba(0,0,0,0.1)', border: '1px solid #e1e4eb' }}>
          <h3 style={{ color: '#1e3c72', marginBottom: '1rem' }}>Task Summary</h3>
          <div style={{ display: 'grid', gap: '0.5rem' }}>
            <div><strong>Total Tasks:</strong> 15</div>
            <div><strong>To Do:</strong> 5</div>
            <div><strong>In Progress:</strong> 7</div>
            <div><strong>Completed:</strong> 3</div>
          </div>
        </div>
      </div>

      <div style={{ background: '#fff', padding: '2rem', borderRadius: '12px', boxShadow: '0 4px 6px rgba(0,0,0,0.1)', border: '1px solid #e1e4eb', marginBottom: '2rem' }}>
        <h3 style={{ color: '#1e3c72', marginBottom: '1rem' }}>Performance Overview</h3>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '1rem' }}>
          <div style={{ textAlign: 'center', padding: '1rem', background: '#f8f9fa', borderRadius: '8px' }}>
            <div style={{ fontSize: '2rem', fontWeight: 'bold', color: '#1e3c72' }}>92%</div>
            <div style={{ color: '#666' }}>Completion Rate</div>
          </div>
          <div style={{ textAlign: 'center', padding: '1rem', background: '#f8f9fa', borderRadius: '8px' }}>
            <div style={{ fontSize: '2rem', fontWeight: 'bold', color: '#1e3c72' }}>87%</div>
            <div style={{ color: '#666' }}>Efficiency Rate</div>
          </div>
          <div style={{ textAlign: 'center', padding: '1rem', background: '#f8f9fa', borderRadius: '8px' }}>
            <div style={{ fontSize: '2rem', fontWeight: 'bold', color: '#1e3c72' }}>5</div>
            <div style={{ color: '#666' }}>Pending Improvements</div>
          </div>
        </div>
      </div>

      <div style={{ background: '#fff', padding: '2rem', borderRadius: '12px', boxShadow: '0 4px 6px rgba(0,0,0,0.1)', border: '1px solid #e1e4eb' }}>
        <h3 style={{ color: '#1e3c72', marginBottom: '1.5rem' }}>User Task Summary</h3>

        {selectedUser ? (
          <div>
            <button
              onClick={() => setSelectedUser(null)}
              style={{ background: '#1e3c72', color: 'white', border: 'none', padding: '0.5rem 1rem', borderRadius: '6px', cursor: 'pointer', marginBottom: '1rem' }}>
              ← Back to Users
            </button>

            <div style={{ display: 'flex', alignItems: 'center', marginBottom: '1rem' }}>
              <img
                src={selectedUser.avatar}
                alt={selectedUser.name}
                style={{ width: '50px', height: '50px', borderRadius: '50%', marginRight: '1rem' }}
              />
              <div>
                <h4 style={{ margin: 0, color: '#1e3c72' }}>{selectedUser.name}</h4>
                <p style={{ margin: 0, color: '#666' }}>{selectedUser.role}</p>
              </div>
            </div>

            <div style={{ display: 'grid', gap: '1rem' }}>
              {selectedUser.tasks.map(task => (
                <div key={task.id} style={{ padding: '1rem', background: '#f8f9fa', borderRadius: '8px', borderLeft: `4px solid ${getStatusColor(task.status)}` }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                    <strong style={{ color: '#1e3c72' }}>{task.title}</strong>
                    <span style={{ padding: '0.25rem 0.5rem', borderRadius: '12px', backgroundColor: getPriorityColor(task.priority), color: 'white', fontSize: '0.8rem' }}>
                      {task.priority}
                    </span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', color: '#666', fontSize: '0.9rem' }}>
                    <span>Status: {task.status}</span>
                    <span>Due: {task.dueDate}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        ) : (
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))', gap: '1rem' }}>
            {userData.map(user => (
              <div
                key={user.id}
                style={{ padding: '1.5rem', background: '#f8f9fa', borderRadius: '12px', cursor: 'pointer', transition: 'transform 0.2s, box-shadow 0.2s', border: '1px solid #e1e4eb' }}
                onClick={() => setSelectedUser(user)}
                onMouseEnter={(e) => {
                  e.currentTarget.style.transform = 'translateY(-2px)';
                  e.currentTarget.style.boxShadow = '0 4px 12px rgba(0,0,0,0.1)';
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.transform = 'translateY(0)';
                  e.currentTarget.style.boxShadow = 'none';
                }}>
                <div style={{ display: 'flex', alignItems: 'center', marginBottom: '1rem' }}>
                  <img
                    src={user.avatar}
                    alt={user.name}
                    style={{ width: '40px', height: '40px', borderRadius: '50%', marginRight: '1rem' }}
                  />
                  <div>
                    <h4 style={{ margin: 0, color: '#1e3c72' }}>{user.name}</h4>
                    <p style={{ margin: 0, color: '#666', fontSize: '0.9rem' }}>{user.role}</p>
                  </div>
                </div>
                <div style={{ display: 'grid', gap: '0.5rem', fontSize: '0.9rem' }}>
                  <div><strong>Tasks:</strong> {user.tasks.length}</div>
                  <div><strong>In Progress:</strong> {user.tasks.filter(t => t.status === 'In Progress').length}</div>
                  <div><strong>Completed:</strong> {user.tasks.filter(t => t.status === 'Completed').length}</div>
                  <div><strong>Due Soon:</strong> {user.tasks.filter(t => t.status !== 'Completed').length}</div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export default Summary;
