import { useState } from 'react';

const data = [
  { BILLING_ACCOUNT_ID: '1000041321', CUSTOMER_ID: '0010002', CATEGORY: 'INP', NATIONALITY: 'KWT', ID_TYPE: 'nationalId', ID_NUMBER: '276020700896', ID_ISSUE_DATE: '5/20/2025', ID_EXPIRY_DATE: '8/22/2026', CREATED: '12/3/2008', EN_FULL_NAME: 'AHMAD MOTLAQ JURAID ALMUTAIRI', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1976-02-07 00:00:00', CONTR_RISK_PROFILE: '0', ARTICLE_NO: '' },
  { BILLING_ACCOUNT_ID: '1000041345', CUSTOMER_ID: '0010032', CATEGORY: 'INP', NATIONALITY: 'KWT', ID_TYPE: 'nationalId', ID_NUMBER: '286050301174', ID_ISSUE_DATE: '4/20/2025', ID_EXPIRY_DATE: '5/2/2028', CREATED: '12/3/2008', EN_FULL_NAME: 'HAMAD AWADH MUTEEA ALMUTAIRI', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1986-05-03 00:00:00', CONTR_RISK_PROFILE: '4', ARTICLE_NO: '0' },
  { BILLING_ACCOUNT_ID: '1000041378', CUSTOMER_ID: '0010055', CATEGORY: 'ING', NATIONALITY: 'KWT', ID_TYPE: 'nationalId', ID_NUMBER: '282012101018', ID_ISSUE_DATE: '10/8/2024', ID_EXPIRY_DATE: '10/8/2029', CREATED: '12/3/2008', EN_FULL_NAME: 'KHALED BARRAK SAAD ALJUAIDI ALAZMI', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1982-01-21 00:00:00', CONTR_RISK_PROFILE: '0', ARTICLE_NO: '' },
  { BILLING_ACCOUNT_ID: '1000041493', CUSTOMER_ID: '0010171', CATEGORY: 'INP', NATIONALITY: 'KWT', ID_TYPE: 'nationalId', ID_NUMBER: '272060500794', ID_ISSUE_DATE: '5/20/2025', ID_EXPIRY_DATE: '3/10/2027', CREATED: '12/3/2008', EN_FULL_NAME: 'NASER HADHRAM SAAD ALHAJRI', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1972-06-05 00:00:00', CONTR_RISK_PROFILE: '0', ARTICLE_NO: '' },
  { BILLING_ACCOUNT_ID: '1000041590', CUSTOMER_ID: '0010263', CATEGORY: 'INP', NATIONALITY: 'KWT', ID_TYPE: 'nationalId', ID_NUMBER: '276052800621', ID_ISSUE_DATE: '5/20/2025', ID_EXPIRY_DATE: '10/26/2025', CREATED: '12/3/2008', EN_FULL_NAME: 'MOHAMMAD J S AJRAN', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1976-05-28 00:00:00', CONTR_RISK_PROFILE: '0', ARTICLE_NO: '' },
  { BILLING_ACCOUNT_ID: '1000041624', CUSTOMER_ID: '0010302', CATEGORY: 'INP', NATIONALITY: 'KWT', ID_TYPE: 'nationalId', ID_NUMBER: '272051600558', ID_ISSUE_DATE: '8/26/2020', ID_EXPIRY_DATE: '8/26/2025', CREATED: '12/3/2008', EN_FULL_NAME: 'KHALED SAAFAK FAHAD ALMUTAIRI', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1972-05-16 00:00:00', CONTR_RISK_PROFILE: '1', ARTICLE_NO: '' },
  { BILLING_ACCOUNT_ID: '1000041662', CUSTOMER_ID: '0010338', CATEGORY: 'INP', NATIONALITY: 'KWT', ID_TYPE: 'nationalId', ID_NUMBER: '286100801098', ID_ISSUE_DATE: '5/20/2025', ID_EXPIRY_DATE: '5/8/2029', CREATED: '12/3/2008', EN_FULL_NAME: 'ALI M H A ALHELAL', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1986-10-08 00:00:00', CONTR_RISK_PROFILE: '0', ARTICLE_NO: '' },
  { BILLING_ACCOUNT_ID: '1000041765', CUSTOMER_ID: '0010443', CATEGORY: 'INP', NATIONALITY: 'IRQ', ID_TYPE: 'nationalId', ID_NUMBER: '279052200195', ID_ISSUE_DATE: '5/20/2025', ID_EXPIRY_DATE: '12/19/2025', CREATED: '12/3/2008', EN_FULL_NAME: 'HOSSAIN HANOUN HANOUN', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1979-05-22 00:00:00', CONTR_RISK_PROFILE: '0', ARTICLE_NO: '18' },
  { BILLING_ACCOUNT_ID: '1000041907', CUSTOMER_ID: '0010584', CATEGORY: 'INP', NATIONALITY: 'KWT', ID_TYPE: 'nationalId', ID_NUMBER: '235072400085', ID_ISSUE_DATE: '5/20/2025', ID_EXPIRY_DATE: '10/3/2028', CREATED: '12/4/2008', EN_FULL_NAME: 'HADI DEKHEEL SUWAYED ALDHAFEERI', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1935-01-01 00:00:00', CONTR_RISK_PROFILE: '0', ARTICLE_NO: '' },
  { BILLING_ACCOUNT_ID: '1000041956', CUSTOMER_ID: '0010634', CATEGORY: 'INP', NATIONALITY: '777', ID_TYPE: 'nationalId', ID_NUMBER: '274100201262', ID_ISSUE_DATE: '3/20/2023', ID_EXPIRY_DATE: '3/20/2028', CREATED: '12/4/2008', EN_FULL_NAME: 'FOUZEYAH AWWADH MOTLAQ A ALOTAIBI', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1974-10-02 00:00:00', CONTR_RISK_PROFILE: '0', ARTICLE_NO: '' },
  { BILLING_ACCOUNT_ID: '1000041959', CUSTOMER_ID: '0010637', CATEGORY: 'INP', NATIONALITY: 'KWT', ID_TYPE: 'nationalId', ID_NUMBER: '267101300451', ID_ISSUE_DATE: '9/9/2024', ID_EXPIRY_DATE: '5/21/2029', CREATED: '12/4/2008', EN_FULL_NAME: 'FAWZI HASAN EISSA ALAWADH', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1967-10-13 00:00:00', CONTR_RISK_PROFILE: '0', ARTICLE_NO: '' },
  { BILLING_ACCOUNT_ID: '1000037607', CUSTOMER_ID: '006296', CATEGORY: 'INP', NATIONALITY: 'KWT', ID_TYPE: 'nationalId', ID_NUMBER: '270011701064', ID_ISSUE_DATE: '12/12/2019', ID_EXPIRY_DATE: '11/13/2029', CREATED: '12/3/2008', EN_FULL_NAME: 'SALEH MOHAMMAD FALEH ALOWAIHAN', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1970-01-17 00:00:00', CONTR_RISK_PROFILE: '2', ARTICLE_NO: '' },
  { BILLING_ACCOUNT_ID: '1000037640', CUSTOMER_ID: '006319', CATEGORY: 'INP', NATIONALITY: 'KWT', ID_TYPE: 'nationalId', ID_NUMBER: '288081401652', ID_ISSUE_DATE: '5/20/2025', ID_EXPIRY_DATE: '8/18/2027', CREATED: '12/3/2008', EN_FULL_NAME: 'NASER AWADH MEFLEH A ALSAADI', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1988-08-14 00:00:00', CONTR_RISK_PROFILE: '0', ARTICLE_NO: '' },
  { BILLING_ACCOUNT_ID: '1000041971', CUSTOMER_ID: '0010649', CATEGORY: 'INP', NATIONALITY: 'KWT', ID_TYPE: 'nationalId', ID_NUMBER: '279010601126', ID_ISSUE_DATE: '5/20/2025', ID_EXPIRY_DATE: '9/25/2028', CREATED: '12/4/2008', EN_FULL_NAME: 'KHALED MARZOUQ MOHAMMAD ALDALLOUM', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1979-01-06 00:00:00', CONTR_RISK_PROFILE: '0', ARTICLE_NO: '' },
  { BILLING_ACCOUNT_ID: '1000041979', CUSTOMER_ID: '0010657', CATEGORY: 'INP', NATIONALITY: 'KWT', ID_TYPE: 'nationalId', ID_NUMBER: '265082500845', ID_ISSUE_DATE: '3/19/2023', ID_EXPIRY_DATE: '3/19/2028', CREATED: '12/4/2008', EN_FULL_NAME: 'JASSIM MOHAMMAD IBRAHIM ALNASHI', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1965-08-25 00:00:00', CONTR_RISK_PROFILE: '1', ARTICLE_NO: '' },
  { BILLING_ACCOUNT_ID: '1000041992', CUSTOMER_ID: '0010669', CATEGORY: 'INP', NATIONALITY: 'KWT', ID_TYPE: 'nationalId', ID_NUMBER: '279080600639', ID_ISSUE_DATE: '5/20/2025', ID_EXPIRY_DATE: '11/25/2026', CREATED: '12/4/2008', EN_FULL_NAME: 'ADEL HUSSAIN NASER B BOHAMAD', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1979-08-06 00:00:00', CONTR_RISK_PROFILE: '2', ARTICLE_NO: '' },
  { BILLING_ACCOUNT_ID: '1000042009', CUSTOMER_ID: '0010686', CATEGORY: 'INP', NATIONALITY: 'KWT', ID_TYPE: 'nationalId', ID_NUMBER: '276090400151', ID_ISSUE_DATE: '1/12/2018', ID_EXPIRY_DATE: '12/13/2027', CREATED: '12/4/2008', EN_FULL_NAME: 'ABDULLAH F E AL', BLKLST_FLAG: '', BLKLST_REASON: '', DATE_OF_BIRTH: '1976-09-04 00:00:00', CONTR_RISK_PROFILE: '0', ARTICLE_NO: '' }
];

const Target = () => {
  const [categoryFilter, setCategoryFilter] = useState('');
  const [nationalityFilter, setNationalityFilter] = useState('');

  const uniqueCategories = Array.from(new Set(data.map(row => row.CATEGORY).filter(Boolean))).sort();
  const uniqueNationalities = Array.from(new Set(data.map(row => row.NATIONALITY).filter(Boolean))).sort();

  const filteredData = data.filter(row => {
    const categoryMatch = categoryFilter ? (row.CATEGORY && row.CATEGORY === categoryFilter) : true;
    const nationalityMatch = nationalityFilter ? (row.NATIONALITY && row.NATIONALITY === nationalityFilter) : true;
    return categoryMatch && nationalityMatch;
  });

  const styles = {
    container: { height: '100vh', width:'85vw', background: 'linear-gradient(135deg, #00215a 0%, #1e3a8a 50%, #3059a0 100%)', color: '#ffffff', padding: '2rem', fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif", overflow: 'auto' },
    card: { background: 'rgba(255, 255, 255, 0.1)', backdropFilter: 'blur(20px)', borderRadius: '20px', padding: '2rem', border: '1px solid rgba(255, 255, 255, 0.2)', boxShadow: '0 8px 32px rgba(0, 0, 0, 0.3)', maxWidth: '100%' },
    cardTitle: { fontSize: '1.8rem', fontWeight: '600', marginBottom: '1.5rem', color: '#ffffff', textAlign: 'center' },
    filtersContainer: { marginBottom: '1rem', display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' },
    select: { padding: '0.25rem 0.5rem', borderRadius: '6px', border: 'none', fontSize: '0.8rem', color: '#000000' },
    table: { width: '100%', borderCollapse: 'collapse', borderRadius: '12px', overflow: 'auto', boxShadow: '0 4px 20px rgba(0, 0, 0, 0.3)' },
    tableHeader: { background: 'linear-gradient(135deg, #1e40af, #3730a3)' },
    tableHeaderCell: { padding: '1rem 1.5rem', textAlign: 'left', fontWeight: '600', fontSize: '0.9rem', textTransform: 'uppercase', letterSpacing: '0.5px', color: '#ffffff', borderBottom: '2px solid rgba(255, 255, 255, 0.2)', whiteSpace: 'nowrap' },
    tableRow: { background: 'rgba(255, 255, 255, 0.05)', borderBottom: '1px solid rgba(255, 255, 255, 0.1)', transition: 'all 0.3s ease', cursor: 'pointer' },
    tableCell: { padding: '1rem 1.5rem', fontSize: '0.9rem', color: '#ffffff', verticalAlign: 'middle', whiteSpace: 'nowrap' }
  };

  return (
    <div style={styles.container}>
      <div style={styles.card}>
        <h2 style={styles.cardTitle}>Customer Data</h2>
        <div style={styles.filtersContainer}>
          <select
            style={styles.select}
            value={categoryFilter}
            onChange={(e) => setCategoryFilter(e.target.value)}>
            <option value="">All Categories</option>
            {uniqueCategories.map((category) => (
              <option key={category} value={category}>{category}</option>
            ))}
          </select>
          <select
            style={styles.select}
            value={nationalityFilter}
            onChange={(e) => setNationalityFilter(e.target.value)}>
            <option value="">All Nationalities</option>
            {uniqueNationalities.map((nationality) => (
              <option key={nationality} value={nationality}>{nationality}</option>
            ))}
          </select>
        </div>
        <div style={{ overflowX: 'auto', maxWidth: '100%', WebkitOverflowScrolling: 'touch' }}>
          <table style={{...styles.table, minWidth: '1800px'}}>
            <thead style={styles.tableHeader}>
              <tr>
                <th style={styles.tableHeaderCell}>BILLING ACCOUNT ID</th>
                <th style={styles.tableHeaderCell}>Customer ID</th>
                <th style={styles.tableHeaderCell}>Category</th>
                <th style={styles.tableHeaderCell}>Nationality</th>
                <th style={styles.tableHeaderCell}>ID Type</th>
                <th style={styles.tableHeaderCell}>ID Number</th>
                <th style={styles.tableHeaderCell}>ID Issue Date</th>
                <th style={styles.tableHeaderCell}>ID Expiry Date</th>
                <th style={styles.tableHeaderCell}>Created</th>
                <th style={styles.tableHeaderCell}>Full Name</th>
                <th style={styles.tableHeaderCell}>Blacklist Flag</th>
                <th style={styles.tableHeaderCell}>Blacklist Reason</th>
                <th style={styles.tableHeaderCell}>Date of Birth</th>
                <th style={styles.tableHeaderCell}>Risk Profile</th>
                <th style={styles.tableHeaderCell}>Article No</th>
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
                  }}>
                  <td style={styles.tableCell}>{row.BILLING_ACCOUNT_ID}</td>
                  <td style={styles.tableCell}>{row.CUSTOMER_ID}</td>
                  <td style={styles.tableCell}>{row.CATEGORY}</td>
                  <td style={styles.tableCell}>{row.NATIONALITY}</td>
                  <td style={styles.tableCell}>{row.ID_TYPE}</td>
                  <td style={styles.tableCell}>{row.ID_NUMBER}</td>
                  <td style={styles.tableCell}>{row.ID_ISSUE_DATE}</td>
                  <td style={styles.tableCell}>{row.ID_EXPIRY_DATE}</td>
                  <td style={styles.tableCell}>{row.CREATED}</td>
                  <td style={styles.tableCell}>{row.EN_FULL_NAME}</td>
                  <td style={styles.tableCell}>{row.BLKLST_FLAG}</td>
                  <td style={styles.tableCell}>{row.BLKLST_REASON}</td>
                  <td style={styles.tableCell}>{row.DATE_OF_BIRTH}</td>
                  <td style={styles.tableCell}>{row.CONTR_RISK_PROFILE}</td>
                  <td style={styles.tableCell}>{row.ARTICLE_NO}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default Target;
