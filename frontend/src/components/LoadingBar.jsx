import { useLoading } from '../context/LoadingContext';

const LoadingBar = () => {
  const { isLoading, progress } = useLoading();

  return (
    <div style={{
      position: 'fixed', top: 0, left: 0, width: '100%', height: '4px',
      backgroundColor: '#f0f0f0', zIndex: 9999,
      opacity: isLoading || progress > 0 ? 1 : 0,
      transition: 'opacity 0.3s ease-out',
      visibility: isLoading || progress > 0 ? 'visible' : 'hidden',
    }}>
      <div style={{
        width: `${progress}%`, height: '100%',
        backgroundColor: '#0748fc',
        transition: 'width 0.3s ease-out',
        boxShadow: '0 0 4px rgba(6, 7, 8, 0.5)',
      }} />
    </div>
  );
};

export default LoadingBar;
