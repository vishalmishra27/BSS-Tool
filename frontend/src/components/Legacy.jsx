import React, { useState } from 'react';

// Static legacy customer data
const data = [
  {
    ACCOUNT_ID: '1000001261',
    CUSTOMER_ID: '1388',
    ID_TYPE: 'C',
    ID_NO: '27505300',
    expiry_date: '12/17/2029',
    issue_date: '',
    CATEGORY: 'HVU',
    NATIONALITY: 'KWT',
    DATE_OF_BIRTH: '1975053000000',
    created_date: '11/4/2008',
    EN_FULL_NAME: 'MUBARAK REJA SUL',
    BLKLST_FLAG: 'N',
    BLKLST_REASON: '',
    ARTICLE_NO: '',
    bss_article_no: '0',
  },
  {
    ACCOUNT_ID: '1000001421',
    CUSTOMER_ID: '1547',
    ID_TYPE: 'C',
    ID_NO: '27908160',
    expiry_date: '5/18/2030',
    issue_date: '',
    CATEGORY: 'HVU',
    NATIONALITY: 'KWT',
    DATE_OF_BIRTH: '1979081600000',
    created_date: '11/7/2008',
    EN_FULL_NAME: 'SAFAA SHAABAN AT',
    BLKLST_FLAG: 'N',
    BLKLST_REASON: '',
    ARTICLE_NO: '',
    bss_article_no: '0',
  },
  {
    ACCOUNT_ID: '1000001261',
    CUSTOMER_ID: '1388',
    ID_TYPE: 'C',
    ID_NO: '27401012',
    expiry_date: '12/5/2025',
    issue_date: '12/10/2023',
    CATEGORY: 'ING',
    NATIONALITY: 'LBN',
    DATE_OF_BIRTH: '1974051100000',
    created_date: '11/9/2008',
    EN_FULL_NAME: 'NIDAL FAOUZAT NO',
    BLKLST_FLAG: 'N',
    BLKLST_REASON: '',
    ARTICLE_NO: '18',
    bss_article_no: '18',
  },
  {
    ACCOUNT_ID: '1000001261',
    CUSTOMER_ID: '1388',
    ID_TYPE: 'C',
    ID_NO: '28202040',
    expiry_date: '11/16/2026',
    issue_date: '11/16/2021',
    CATEGORY: 'INP',
    NATIONALITY: 'KWT',
    DATE_OF_BIRTH: '1982020400000',
    created_date: '11/9/2008',
    EN_FULL_NAME: 'ALI MOSTAFA ALI M',
    BLKLST_FLAG: 'N',
    BLKLST_REASON: '',
    ARTICLE_NO: '',
    bss_article_no: '0',
  },
  {
    ACCOUNT_ID: '1000001261',
    CUSTOMER_ID: '1388',
    ID_TYPE: 'C',
    ID_NO: '27401012',
    expiry_date: '12/5/2025',
    issue_date: '12/10/2023',
    CATEGORY: 'ING',
    NATIONALITY: 'LBN',
    DATE_OF_BIRTH: '1974051100000',
    created_date: '11/9/2008',
    EN_FULL_NAME: 'NIDAL FAOUZAT NO',
    BLKLST_FLAG: 'N',
    BLKLST_REASON: '',
    ARTICLE_NO: '18',
    bss_article_no: '18',
  },
  {
    ACCOUNT_ID: '1000001261',
    CUSTOMER_ID: '1388',
    ID_TYPE: 'C',
    ID_NO: '28202040',
    expiry_date: '11/16/2026',
    issue_date: '11/16/2021',
    CATEGORY: 'INP',
    NATIONALITY: 'KWT',
    DATE_OF_BIRTH: '1982020400000',
    created_date: '11/9/2008',
    EN_FULL_NAME: 'ALI MOSTAFA ALI M',
    BLKLST_FLAG: 'N',
    BLKLST_REASON: '',
    ARTICLE_NO: '',
    bss_article_no: '0',
  },
  {
    ACCOUNT_ID: '1000001261',
    CUSTOMER_ID: '1388',
    ID_TYPE: 'C',
    ID_NO: '27401012',
    expiry_date: '12/5/2025',
    issue_date: '12/10/2023',
    CATEGORY: 'ING',
    NATIONALITY: 'LBN',
    DATE_OF_BIRTH: '1974051100000',
    created_date: '11/9/2008',
    EN_FULL_NAME: 'NIDAL FAOUZAT NO',
    BLKLST_FLAG: 'N',
    BLKLST_REASON: '',
    ARTICLE_NO: '18',
    bss_article_no: '18',
  },
  {
    ACCOUNT_ID: '1000001261',
    CUSTOMER_ID: '1388',
    ID_TYPE: 'C',
    ID_NO: '28202040',
    expiry_date: '11/16/2026',
    issue_date: '11/16/2021',
    CATEGORY: 'INP',
    NATIONALITY: 'KWT',
    DATE_OF_BIRTH: '1982020400000',
    created_date: '11/9/2008',
    EN_FULL_NAME: 'ALI MOSTAFA ALI M',
    BLKLST_FLAG: 'N',
    BLKLST_REASON: '',
    ARTICLE_NO: '',
    bss_article_no: '0',
  },
];

const Legacy = () => {
  const [categoryFilter, setCategoryFilter] = useState('');
  const [nationalityFilter, setNationalityFilter] = useState('');

  // Get unique categories and nationalities for filter dropdowns
  const uniqueCategories = Array.from(new Set(data.map(row => row.CATEGORY).filter(Boolean))).sort();
  const uniqueNationalities = Array.from(new Set(data.map(row => row.NATIONALITY).filter(Boolean))).sort();

  // Filter data based on selected filters
  const filteredData = data.filter(row => {
    const categoryMatch = categoryFilter ? (row.CATEGORY && row.CATEGORY === categoryFilter) : true;
    const nationalityMatch = nationalityFilter ? (row.NATIONALITY && row.NATIONALITY === nationalityFilter) : true;
    return categoryMatch && nationalityMatch;
  });

  const styles = {
    container: {
      height: '100vh',
      width:'85vw',
      background: 'linear-gradient(135deg, #00215a 0%, #1e3a8a 50%, #3059a0 100%)',
      color: '#ffffff',
      padding: '2rem',
      fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
      overflow: 'auto',
    },
    card: {
      background: 'rgba(255, 255, 255, 0.1)',
      backdropFilter: 'blur(20px)',
      borderRadius: '20px',
      padding: '2rem',
      border: '1px solid rgba(255, 255, 255, 0.2)',
      boxShadow: '0 8px 32px rgba(0, 0, 0, 0.3)',
      maxWidth: '100%',
    },
    cardTitle: {
      fontSize: '1.8rem',
      fontWeight: '600',
      marginBottom: '1.5rem',
      color: '#ffffff',
      textAlign: 'center',
    },
    filtersContainer: {
      marginBottom: '1rem',
      display: 'flex',
      gap: '0.5rem',
      justifyContent: 'flex-end',
    },
    select: {
      padding: '0.25rem 0.5rem',
      borderRadius: '6px',
      border: 'none',
      fontSize: '0.8rem',
      color: '#000000',
    },
    table: {
      width: '100%',
      borderCollapse: 'collapse',
      borderRadius: '12px',
      overflow: 'hidden',
      boxShadow: '0 4px 20px rgba(0, 0, 0, 0.3)',
    },
    tableHeader: {
      background: 'linear-gradient(135deg, #1e40af, #3730a3)',
    },
    tableHeaderCell: {
      padding: '1rem 1.5rem',
      textAlign: 'left',
      fontWeight: '600',
      fontSize: '0.9rem',
      textTransform: 'uppercase',
      letterSpacing: '0.5px',
      color: '#ffffff',
      borderBottom: '2px solid rgba(255, 255, 255, 0.2)',
      whiteSpace: 'nowrap',
    },
    tableRow: {
      background: 'rgba(255, 255, 255, 0.05)',
      borderBottom: '1px solid rgba(255, 255, 255, 0.1)',
      transition: 'all 0.3s ease',
      cursor: 'pointer',
    },
    tableCell: {
      padding: '1rem 1.5rem',
      fontSize: '0.9rem',
      color: '#ffffff',
      verticalAlign: 'middle',
      whiteSpace: 'nowrap',
    },
    tableWrapper: {
      overflowX: 'auto',
      overflowY: 'auto',
      maxWidth: '100%',
      width: '100%',
      borderRadius: '12px',
      boxShadow: '0 4px 20px rgba(0, 0, 0, 0.3)',
      border: '1px solid rgba(255, 255, 255, 0.2)',
      background: 'rgba(255, 255, 255, 0.05)',
      display: 'block',
      position: 'relative',
    },
  };

  return (
    <div style={styles.container}>
      <div style={styles.card}>
        <h2 style={styles.cardTitle}>Legacy Data</h2>
        <div style={styles.filtersContainer}>
          <select
            style={styles.select}
            value={categoryFilter}
            onChange={(e) => setCategoryFilter(e.target.value)}
          >
            <option value="">All Categories</option>
            {uniqueCategories.map((category) => (
              <option key={category} value={category}>
                {category}
              </option>
            ))}
          </select>
          <select
            style={styles.select}
            value={nationalityFilter}
            onChange={(e) => setNationalityFilter(e.target.value)}
          >
            <option value="">All Nationalities</option>
            {uniqueNationalities.map((nationality) => (
              <option key={nationality} value={nationality}>
                {nationality}
              </option>
            ))}
          </select>
        </div>
        <div style={styles.tableWrapper}>
          <table style={{...styles.table, minWidth: '1500px'}}>
            <thead style={styles.tableHeader}>
              <tr>
                <th style={styles.tableHeaderCell}>ACCOUNT ID</th>
                <th style={styles.tableHeaderCell}>CUSTOMER ID</th>
                <th style={styles.tableHeaderCell}>ID TYPE</th>
                <th style={styles.tableHeaderCell}>ID NO</th>
                <th style={styles.tableHeaderCell}>EXPIRY DATE</th>
                <th style={styles.tableHeaderCell}>ISSUE DATE</th>
                <th style={styles.tableHeaderCell}>CATEGORY</th>
                <th style={styles.tableHeaderCell}>NATIONALITY</th>
                <th style={styles.tableHeaderCell}>DATE OF BIRTH</th>
                <th style={styles.tableHeaderCell}>CREATED DATE</th>
                <th style={styles.tableHeaderCell}>FULL NAME</th>
                <th style={styles.tableHeaderCell}>BLACKLIST FLAG</th>
                <th style={styles.tableHeaderCell}>BLACKLIST REASON</th>
                <th style={styles.tableHeaderCell}>ARTICLE NO</th>
                <th style={styles.tableHeaderCell}>BSS ARTICLE NO</th>
              </tr>
            </thead>
            <tbody>
              {filteredData.map((row, idx) => (
                <tr
                  key={idx}
                  style={styles.tableRow}
                  onMouseEnter={(e) => {
                    e.currentTarget.style.background = 'rgba(255, 255, 255, 0.15)';
                    e.currentTarget.style.transform = 'translateY(-1px)';
                  }}
                  onMouseLeave={(e) => {
                    e.currentTarget.style.background = 'rgba(255, 255, 255, 0.05)';
                    e.currentTarget.style.transform = 'translateY(0)';
                  }}
                >
                  <td style={styles.tableCell}>{row.ACCOUNT_ID}</td>
                  <td style={styles.tableCell}>{row.CUSTOMER_ID}</td>
                  <td style={styles.tableCell}>{row.ID_TYPE}</td>
                  <td style={styles.tableCell}>{row.ID_NO}</td>
                  <td style={styles.tableCell}>{row.expiry_date}</td>
                  <td style={styles.tableCell}>{row.issue_date}</td>
                  <td style={styles.tableCell}>{row.CATEGORY}</td>
                  <td style={styles.tableCell}>{row.NATIONALITY}</td>
                  <td style={styles.tableCell}>{row.DATE_OF_BIRTH}</td>
                  <td style={styles.tableCell}>{row.created_date}</td>
                  <td style={styles.tableCell}>{row.EN_FULL_NAME}</td>
                  <td style={styles.tableCell}>{row.BLKLST_FLAG}</td>
                  <td style={styles.tableCell}>{row.BLKLST_REASON}</td>
                  <td style={styles.tableCell}>{row.ARTICLE_NO}</td>
                  <td style={styles.tableCell}>{row.bss_article_no}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default Legacy;
