// Utility to monkey-patch fetch API for automatic loading state tracking
// Patches the global fetch to call provided callbacks for loading states

let originalFetch = null;

export const patchFetchWithLoading = (startLoading, stopLoading) => {
  if (typeof window === 'undefined' || originalFetch) return; // Already patched or server-side

  originalFetch = window.fetch;

  window.fetch = async (...args) => {
    startLoading();
    try {
      const response = await originalFetch(...args);
      return response;
    } finally {
      stopLoading();
    }
  };

  console.log('Fetch patched with loading tracking');
};

export const restoreFetch = () => {
  if (originalFetch && typeof window !== 'undefined') {
    window.fetch = originalFetch;
    originalFetch = null;
    console.log('Fetch restored to original');
  }
};

