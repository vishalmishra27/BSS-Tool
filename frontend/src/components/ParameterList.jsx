import { useState, useMemo } from 'react';
import PropTypes from 'prop-types';
import styles from './ProductDashboardPage.module.css';

const ParameterList = ({ parameters }) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [statusFilter, setStatusFilter] = useState('all');
  const [sortBy, setSortBy] = useState('name');

  const filteredAndSortedParameters = useMemo(() => {
    let filtered = parameters;

    // Filter by search term
    if (searchTerm) {
      filtered = filtered.filter(param =>
        param.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
        param.description.toLowerCase().includes(searchTerm.toLowerCase())
      );
    }

    // Filter by status
    if (statusFilter !== 'all') {
      filtered = filtered.filter(param => param.status === statusFilter);
    }

    // Sort
    filtered.sort((a, b) => {
      switch (sortBy) {
        case 'name':
          return a.name.localeCompare(b.name);
        case 'status':
          return a.status.localeCompare(b.status);
        case 'progress':
          return b.progress - a.progress;
        default:
          return 0;
      }
    });

    return filtered;
  }, [parameters, searchTerm, statusFilter, sortBy]);

  const getStatusColor = (status) => {
    const colors = {
      completed: '#10b981',
      inProgress: '#f59e0b',
      notStarted: '#ef4444'
    };
    return colors[status] || '#6b7280';
  };

  const getStatusLabel = (status) => {
    const labels = {
      completed: 'Completed',
      inProgress: 'In Progress',
      notStarted: 'Not Started'
    };
    return labels[status] || status;
  };

  return (
    <div className={styles.parameterListContainer}>
      <div className={styles.parameterListHeader}>
        <h3>Product Parameters</h3>
        <div className={styles.parameterControls}>
          <input
            type="text"
            placeholder="Search parameters..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className={styles.searchInput}
          />
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value)}
            className={styles.filterSelect}>
            <option value="all">All Status</option>
            <option value="completed">Completed</option>
            <option value="inProgress">In Progress</option>
            <option value="notStarted">Not Started</option>
          </select>
          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            className={styles.sortSelect}>
            <option value="name">Sort by Name</option>
            <option value="status">Sort by Status</option>
            <option value="progress">Sort by Progress</option>
          </select>
        </div>
      </div>

      <div className={styles.parameterList}>
        {filteredAndSortedParameters.map((parameter) => (
          <div key={parameter.id} className={styles.parameterCard}>
            <div className={styles.parameterInfo}>
              <h4 className={styles.parameterName}>{parameter.name}</h4>
              <p className={styles.parameterDescription}>{parameter.description}</p>
            </div>
            <div className={styles.parameterStatus}>
              <span
                className={styles.statusBadge}
                style={{ backgroundColor: getStatusColor(parameter.status) }}>
                {getStatusLabel(parameter.status)}
              </span>
              <div className={styles.progressBar}>
                <div
                  className={styles.progressFill}
                  style={{ width: `${parameter.progress}%` }}
                />
              </div>
              <span className={styles.progressText}>{parameter.progress}%</span>
            </div>
          </div>
        ))}
      </div>

      {filteredAndSortedParameters.length === 0 && (
        <div className={styles.emptyState}>
          <p>No parameters found matching your criteria.</p>
        </div>
      )}
    </div>
  );
};

ParameterList.propTypes = {
  parameters: PropTypes.arrayOf(
    PropTypes.shape({
      id: PropTypes.string.isRequired,
      name: PropTypes.string.isRequired,
      description: PropTypes.string.isRequired,
      status: PropTypes.oneOf(['completed', 'inProgress', 'notStarted']).isRequired,
      progress: PropTypes.number.isRequired
    })
  ).isRequired
};

export default ParameterList;
