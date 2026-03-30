import PropTypes from 'prop-types';
import styles from './ProductDashboardPage.module.css';

const RationalizationChart = ({ data }) => {
  const { rationalized, pending, notStarted } = data;

  const total = rationalized + pending + notStarted;
  const rationalizedAngle = (rationalized / total) * 360;
  const pendingAngle = (pending / total) * 360;
  const notStartedAngle = (notStarted / total) * 360;

  const chartData = [
    { label: 'Rationalized', value: rationalized, color: '#10b981', angle: rationalizedAngle },
    { label: 'Pending', value: pending, color: '#f59e0b', angle: pendingAngle },
    { label: 'Not Started', value: notStarted, color: '#ef4444', angle: notStartedAngle }
  ];

  const radius = 80;
  const strokeWidth = 12;
  const size = 200;
  const center = size / 2;

  const polarToCartesian = (centerX, centerY, radius, angleInDegrees) => {
    const angleInRadians = (angleInDegrees - 90) * Math.PI / 180.0;
    return {
      x: centerX + (radius * Math.cos(angleInRadians)),
      y: centerY + (radius * Math.sin(angleInRadians))
    };
  };

  const createArcPath = (startAngle, endAngle) => {
    const start = polarToCartesian(center, center, radius, endAngle);
    const end = polarToCartesian(center, center, radius, startAngle);
    const largeArcFlag = endAngle - startAngle <= 180 ? "0" : "1";

    return [
      "M", start.x, start.y,
      "A", radius, radius, 0, largeArcFlag, 0, end.x, end.y
    ].join(" ");
  };

  let currentAngle = 0;
  const arcs = chartData.map((item) => {
    const startAngle = currentAngle;
    const endAngle = currentAngle + item.angle;
    currentAngle = endAngle;

    return {
      ...item,
      path: createArcPath(startAngle, endAngle)
    };
  });

  return (
    <div className={styles.chartContainer}>
      <h3 className={styles.chartTitle}>Rationalization Status</h3>
      <div className={styles.chartWrapper}>
        <svg width={size} height={size} className={styles.donutChart}>
          <circle
            cx={center}
            cy={center}
            r={radius}
            fill="none"
            stroke="#e5e7eb"
            strokeWidth={strokeWidth}
          />
          {arcs.map((arc, index) => (
            <path
              key={index}
              d={arc.path}
              fill="none"
              stroke={arc.color}
              strokeWidth={strokeWidth}
              strokeLinecap="round"
            />
          ))}
          <text
            x={center}
            y={center}
            textAnchor="middle"
            dominantBaseline="middle"
            className={styles.chartCenterText}>
            {total}
          </text>
          <text
            x={center}
            y={center + 20}
            textAnchor="middle"
            dominantBaseline="middle"
            className={styles.chartCenterLabel}>
            Total
          </text>
        </svg>

        <div className={styles.chartLegend}>
          {chartData.map((item, index) => (
            <div key={index} className={styles.legendItem}>
              <div
                className={styles.legendColor}
                style={{ backgroundColor: item.color }}
              />
              <span className={styles.legendLabel}>{item.label}</span>
              <span className={styles.legendValue}>{item.value}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

RationalizationChart.propTypes = {
  data: PropTypes.shape({
    rationalized: PropTypes.number.isRequired,
    pending: PropTypes.number.isRequired,
    notStarted: PropTypes.number.isRequired
  }).isRequired
};

export default RationalizationChart;
