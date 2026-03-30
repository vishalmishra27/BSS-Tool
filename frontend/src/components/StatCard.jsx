import styles from './ProductDashboardPage.module.css';

const StatCard = ({ title, value, icon, iconClass }) => {
  return (
    <div className={styles.statCard}>
      <div className={styles.statCardHeader}>
        <h3 className={styles.statCardTitle}>{title}</h3>
        <div className={`${styles.statCardIcon} ${styles[iconClass]}`}>
          {icon}
        </div>
      </div>
      <p className={styles.statCardValue}>{value}</p>
    </div>
  );
};

export default StatCard;
