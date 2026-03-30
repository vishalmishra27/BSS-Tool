import PropTypes from 'prop-types';
import styles from './ProductDashboardPage.module.css';

const ProductStats = ({ stats }) => {
  const { totalProducts, rationalized, pending, notStarted } = stats;

  const calculatePercentage = (value, total) => {
    return total > 0 ? Math.round((value / total) * 100) : 0;
  };

  const statCards = [
    {
      title: 'Total Products',
      value: totalProducts,
      color: '#3b82f6',
      icon: '📦'
    },
    {
      title: 'Rationalized',
      value: rationalized,
      percentage: calculatePercentage(rationalized, totalProducts),
      color: '#10b981',
      icon: '✅'
    },
    {
      title: 'Pending',
      value: pending,
      percentage: calculatePercentage(pending, totalProducts),
      color: '#f59e0b',
      icon: '⏳'
    },
    {
      title: 'Not Started',
      value: notStarted,
      percentage: calculatePercentage(notStarted, totalProducts),
      color: '#ef4444',
      icon: '📋'
    }
  ];

  return (
    <div className={styles.statsContainer}>
      {statCards.map((card, index) => (
        <div key={index} className={styles.statCard}>
          <div className={styles.statIcon}>{card.icon}</div>
          <div className={styles.statContent}>
            <h3 className={styles.statValue}>{card.value.toLocaleString()}</h3>
            <p className={styles.statTitle}>{card.title}</p>
            {card.percentage !== undefined && (
              <p className={styles.statPercentage}>{card.percentage}%</p>
            )}
          </div>
          <div
            className={styles.statIndicator}
            style={{ backgroundColor: card.color }}
          />
        </div>
      ))}
    </div>
  );
};

ProductStats.propTypes = {
  stats: PropTypes.shape({
    totalProducts: PropTypes.number.isRequired,
    rationalized: PropTypes.number.isRequired,
    pending: PropTypes.number.isRequired,
    notStarted: PropTypes.number.isRequired
  }).isRequired
};

export default ProductStats;
