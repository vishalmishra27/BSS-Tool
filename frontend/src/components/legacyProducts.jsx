import React, { useEffect, useState } from 'react';
import { useLocation } from 'react-router-dom';

const exportLegacyProducts = async () => {
  const response = await fetch('/api/legacy-products/export');
  const blob = await response.blob();
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'legacy_products.csv';
  document.body.appendChild(a);
  a.click();
  a.remove();
};


const LegacyP = () => {
  const location = useLocation();
  const queryParams = new URLSearchParams(location.search);
  const initialStatus = queryParams.get('status') || '';
  const initialLob = queryParams.get('lob') || '';

  const [legacyProducts, setLegacyProducts] = useState([]);
  const [lobFilter, setLobFilter] = useState(initialLob);
  const [statusFilter, setStatusFilter] = useState(initialStatus);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchLegacyProducts = async () => {
      try {
        const response = await fetch('/api/legacy-products/raw');
        if (!response.ok) {
          throw new Error(`Error: ${response.status}`);
        }
        const result = await response.json();
        setLegacyProducts(result.data);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchLegacyProducts();
  }, []);


  const uniqueLobs = Array.from(new Set(legacyProducts.map(p => p.lob).filter(Boolean))).sort();
  const uniqueStatuses = Array.from(new Set(legacyProducts.map(p => p.rationalization_status).filter(Boolean))).sort();

  const filteredProducts = legacyProducts.filter(p => {
    const lobMatch = lobFilter ? (p.lob && p.lob.toLowerCase() === lobFilter.toLowerCase()) : true;
    const statusMatch = statusFilter ? (p.rationalization_status && p.rationalization_status.toLowerCase() === statusFilter.toLowerCase()) : true;
    return lobMatch && statusMatch;
  });

  const styles = {
    container: {
      height: '100vh',
      background: 'linear-gradient(135deg, #f8fafc 0%, #e2e8f0 50%, #cbd5e1 100%)',
      color: '#1e293b',
      padding: '1.5rem',
      fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
      overflowX: 'hidden',
      overflowY: 'auto',
    },
    card: {
      backgroundColor: '#ffffff',
      borderRadius: '12px',
      padding: '1.5rem',
      boxShadow: '0 2px 12px rgba(30, 64, 175, 0.08)',
      border: '1px solid #e2e8f0',
      marginBottom: '1.5rem',
      position: 'relative',
    },
    cardTitle: {
      fontSize: '2.2rem',
      fontWeight: '700',
      marginBottom: '1.5rem',
      color: '#1e40af',
      textAlign: 'center',
    },
    filtersContainer: {
      marginBottom: '1rem',
      display: 'flex',
      gap: '0.5rem',
      justifyContent: 'flex-end',
      flexWrap: 'wrap',
    },
    select: {
      padding: '0.5rem 1rem',
      borderRadius: '6px',
      border: '1px solid #d1d5db',
      fontSize: '0.875rem',
      backgroundColor: '#ffffff',
      color: '#374151',
      minWidth: '120px',
    },
    table: {
      width: '100%',
      borderCollapse: 'collapse',
      borderRadius: '8px',
      overflow: 'hidden',
      boxShadow: '0 1px 3px rgba(0, 0, 0, 0.1)',
    },
    tableHeader: {
      background: 'linear-gradient(135deg, #1e40af 0%, #3b82f6 100%)',
      color: '#ffffff',
    },
    tableHeaderCell: {
      padding: '1rem 1.5rem',
      textAlign: 'left',
      fontWeight: '600',
      fontSize: '0.875rem',
      borderBottom: '2px solid #1e40af',
    },
    tableRow: {
      backgroundColor: '#ffffff',
      borderBottom: '1px solid #e5e7eb',
    },
    tableCell: {
      padding: '1rem 1.5rem',
      fontSize: '0.875rem',
      color: '#374151',
      verticalAlign: 'middle',
    },
    backButton: {
      position: 'absolute',
      top: '2.5rem',
      left: '3rem',
      backgroundColor: '#1e40af',
      color: '#ffffff',
      border: 'none',
      borderRadius: '6px',
      padding: '0.5rem 1rem',
      fontWeight: '600',
      cursor: 'pointer',
      zIndex: 1000,
    },
  };

  return (

    <div style={styles.container}>
      <div style={styles.card}>
        <h2 style={styles.cardTitle}>Legacy Products Inventory</h2>
        <button
  onClick={exportLegacyProducts}
  style={{
    position: 'absolute',
    top: '2.5rem',
    right: '3rem',
    backgroundColor: '#1e40af',
    color: '#ffffff',
    border: 'none',
    borderRadius: '6px',
    padding: '0.5rem 1rem',
    fontWeight: '600',
    cursor: 'pointer',
    zIndex: 1000,
  }}
>
  Export
</button>
        <button onClick={() => window.history.back()} style={styles.backButton}>Back</button>
       
        <div style={styles.filtersContainer}>
          <select style={styles.select} value={lobFilter} onChange={(e) => setLobFilter(e.target.value)}>
            <option value="">All LoBs</option>
            {uniqueLobs.map((lob) => (
              <option key={lob} value={lob}>{lob}</option>
            ))}
          </select>
          <select style={styles.select} value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
            <option value="">All Statuses</option>
            {uniqueStatuses.map((status) => (
              <option key={status} value={status}>{status}</option>
            ))}
          </select>
        </div>
        {loading ? (
          <div style={{ color: 'gray', padding: '2rem' }}>Loading legacy products...</div>
        ) : error ? (
          <div style={{ color: 'red', padding: '2rem' }}>Error: {error}</div>
        ) : (
          <div style={{ overflowX: 'auto' }}>
            <table style={styles.table}>
              <thead style={styles.tableHeader}>
                <tr>
                  <th style={styles.tableHeaderCell}>Product ID</th>
                  <th style={styles.tableHeaderCell}>Product Name</th>
                  <th style={styles.tableHeaderCell}>LOB</th>
                  <th style={styles.tableHeaderCell}>Rationalization Status</th>
                  <th style={styles.tableHeaderCell}>Pending On</th>
                </tr>
              </thead>
              <tbody>
                {filteredProducts.map((product, idx) => (
                  <tr key={idx} style={styles.tableRow}>
                    <td style={styles.tableCell}>{product.product_id}</td>
                    <td style={styles.tableCell}>{product.product_name}</td>
                    <td style={styles.tableCell}>{product.lob}</td>
                    <td style={styles.tableCell}>{product.rationalization_status}</td>
                    <td style={styles.tableCell}>{product.pending_on}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
};

export default LegacyP;
