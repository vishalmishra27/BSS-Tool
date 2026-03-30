import { createContext, useContext, useState, useEffect, useRef } from 'react';
import { patchFetchWithLoading, restoreFetch } from './useFetchWithLoading';

const LoadingContext = createContext();

export const useLoading = () => {
  const context = useContext(LoadingContext);
  if (!context) {
    throw new Error('useLoading must be used within a LoadingProvider');
  }
  return context;
};

export const LoadingProvider = ({ children }) => {
  const [loadingCount, setLoadingCount] = useState(0);
  const [progress, setProgress] = useState(0);
  const isPatched = useRef(false);

  const isLoading = loadingCount > 0;

  const startLoading = () => {
    setLoadingCount(prev => prev + 1);
  };

  const stopLoading = () => {
    setLoadingCount(prev => Math.max(0, prev - 1));
  };

  useEffect(() => {
    let interval;
    if (isLoading) {
      setProgress(30);
      interval = setInterval(() => {
        setProgress(prev => {
          const increment = Math.random() * 20;
          return Math.min(90, prev + increment);
        });
      }, 300);
    } else {
      setProgress(100);
      const timer = setTimeout(() => {
        setProgress(0);
      }, 300);
      return () => clearTimeout(timer);
    }
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [isLoading]);

  useEffect(() => {
    if (!isPatched.current) {
      setTimeout(() => {
        patchFetchWithLoading(startLoading, stopLoading);
        isPatched.current = true;
      }, 0);
    }
    return () => {
      if (isPatched.current) {
        restoreFetch();
        isPatched.current = false;
      }
    };
  }, []);

  const contextValue = { isLoading, progress, startLoading, stopLoading };
  if (typeof window !== 'undefined') {
    window.loadingContext = contextValue;
  }

  return (
    <LoadingContext.Provider value={contextValue}>
      {children}
    </LoadingContext.Provider>
  );
};
