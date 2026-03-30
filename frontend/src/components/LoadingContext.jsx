import React, { createContext, useContext, useState, useEffect, useRef } from 'react';
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

  // Increment loading count (start tracking a request)
  const startLoading = () => {
    setLoadingCount(prev => prev + 1);
  };

  // Decrement loading count (finish tracking a request)
  const stopLoading = () => {
    setLoadingCount(prev => Math.max(0, prev - 1));
  };

  // Effect to handle progress bar animation
  useEffect(() => {
    let interval;
   
    if (isLoading) {
      // Start progress bar animation
      setProgress(30); // Start at 30%
     
      // Simulate progress
      interval = setInterval(() => {
        setProgress(prev => {
          // Increase progress but cap at 90% while loading
          const increment = Math.random() * 20;
          return Math.min(90, prev + increment);
        });
      }, 300);
    } else {
      // Complete progress bar when loading finishes
      setProgress(100);
     
      // Reset progress after a short delay
      const timer = setTimeout(() => {
        setProgress(0);
      }, 300);
     
      return () => clearTimeout(timer);
    }
   
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [isLoading]);

  // Effect to patch fetch when component mounts
  useEffect(() => {
    // Patch fetch to automatically track loading
    if (!isPatched.current) {
      // We need to pass the functions after they're defined
      setTimeout(() => {
        patchFetchWithLoading(startLoading, stopLoading);
        isPatched.current = true;
      }, 0);
    }
   
    // Cleanup function to restore original fetch
    return () => {
      if (isPatched.current) {
        restoreFetch();
        isPatched.current = false;
      }
    };
  }, []);

  // Store context in global variable for access outside React
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
