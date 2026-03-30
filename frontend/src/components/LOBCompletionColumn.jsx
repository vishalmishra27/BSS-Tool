import './LOBCompletionColumn.css';

const LOBCompletionColumn = ({ lobName, completion }) => {
  const getProgressColor = (percentage) => {
    if (percentage >= 80) return '#16a34a';
    if (percentage >= 60) return '#f59e0b';
    return '#ef4444';
  };

  const getStatusText = (percentage) => {
    if (percentage >= 80) return 'On Track';
    if (percentage >= 60) return 'At Risk';
    return 'Needs Attention';
  };

  return (
    <div className="lob-completion-card">
      <div className="lob-header">
        <h3>{lobName}</h3>
        <span className="completion-percentage">{completion}%</span>
      </div>
      <div className="progress-container">
        <div
          className="progress-bar"
          style={{
            width: `${completion}%`,
            backgroundColor: getProgressColor(completion)
          }}
        />
      </div>
      <div className="completion-status">
        {getStatusText(completion)}
      </div>
    </div>
  );
};

export default LOBCompletionColumn;
