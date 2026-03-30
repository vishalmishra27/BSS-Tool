import styles from './ProductDashboardPage.module.css';

const ParameterItem = ({ parameter }) => {
  const getStatusBadgeClass = (status) => {
    switch (status) {
      case 'completed': return styles.completed;
      case 'in_progress': return styles.inProgress;
      case 'pending': return styles.pending;
      case 'failed': return styles.failed;
      default: return styles.pending;
    }
  };

  const getStatusText = (status) => {
    switch (status) {
      case 'completed': return 'Completed';
      case 'in_progress': return 'In Progress';
      case 'pending': return 'Pending';
      case 'failed': return 'Failed';
      default: return 'Pending';
    }
  };

  const getIcon = (type) => {
    switch (type) {
      case 'temperature': return '🌡️';
      case 'pressure': return '⚡';
      case 'speed': return '🚀';
      case 'quality': return '✅';
      default: return '📊';
    }
  };

  return (
    <div className={styles.parameterItem}>
      <div className={styles.parameterInfo}>
        <div className={styles.parameterIcon}>
          {getIcon(parameter.type)}
        </div>
        <div className={styles.parameterDetails}>
          <h4>{parameter.name}</h4>
          <p>{parameter.description}</p>
        </div>
      </div>
      <div className={styles.parameterStatus}>
        <span className={`${styles.statusBadge} ${getStatusBadgeClass(parameter.status)}`}>
          {getStatusText(parameter.status)}
        </span>
        {parameter.status === 'in_progress' && (
          <div className={styles.progressBar}>
            <div
              className={styles.progressFill}
              style={{ width: `${parameter.progress || 0}%` }}
            />
          </div>
        )}
      </div>
    </div>
  );
};

export default ParameterItem;
