import { useState } from 'react';
const LegacyP = () => {
const [lobFilter, setLobFilter] = useState('');
const [statusFilter, setStatusFilter] = useState('');
// Updated legacy products data based on provided image
const legacyProducts = [
{
product_id: 'O145644',
product_name: '504_INTERCO_FO_20MBPS_DIRISI~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be purged',
pending_on: '-'
},
{
product_id: 'O145646',
product_name: '46_WHITEHOUSECOLLOCATION12U~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be purged',
pending_on: '-'
},
{
product_id: 'O145647',
product_name: '457_VIDEOCONF_SERVICE500~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be purged',
pending_on: '-'
},
{
product_id: 'O145648',
product_name: '155_FO_EN_BACKUP_ZONE_ABIDJAN_20_M~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be migrated',
pending_on: 'TT'
},
{
product_id: 'O145652',
product_name: 'LiaisonFibreOptiqueenBack-up–30Mbps~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be purged',
pending_on: '-'
},
{
product_id: 'O145653',
product_name: '506_INTERCO_FO_1MBPS~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be purged',
pending_on: '-'
},
{
product_id: 'O145657',
product_name: '157_AD_YOOMEE_315MBPS~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be migrated',
pending_on: 'TT'
},
{
product_id: 'O145660',
product_name: '49_BLR_P-MP_3MO_ZONEINTERVILLE~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be migrated',
pending_on: 'TT'
},
{
product_id: 'O145663',
product_name: '50_FOENBACKUP-ZONEABIDJAN3MEGA~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be migrated',
pending_on: 'TT'
},
{
product_id: 'O145664',
product_name: '509_SUPPORT2_COLLOCATION_42U~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be purged',
pending_on: '-'
},
{
product_id: 'O145666',
product_name: '51_BLRPOINTMULTIPOINT_ZONEINTRAVI~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be migrated',
pending_on: 'TT'
},
{
product_id: 'O145669',
product_name: '52_VANCO_UPGRADE_8M~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be migrated',
pending_on: 'TT'
},
{
product_id: 'O145672',
product_name: '53_AD_45M_NSIATECH~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be purged',
pending_on: '-'
},
{
product_id: 'O145675',
product_name: '54_CISCO1941_CT~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be purged',
pending_on: '-'
},
{
product_id: 'O144782',
product_name: '3_CI_WIBOXPREMIUMPOS',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be purged',
pending_on: '-'
},
{
product_id: 'O144787',
product_name: '8_FO_P-MP_ZONE_ABJ_10M~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be migrated',
pending_on: 'TT'
},
{
product_id: 'O144791',
product_name: '11_BLR_POINT-MULTIPOINT_ZONE_INTR~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be migrated',
pending_on: 'TT'
},
{
product_id: 'O145005',
product_name: '258_FO_P-MP_ZONEABIDJAN_50M~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be migrated',
pending_on: 'TT'
},
{
product_id: 'O145013',
product_name: '158_APN_DEDIE_100~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be purged',
pending_on: '-'
},
{
product_id: 'O144887',
product_name: '525_F-TEMPORAIRE~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be migrated',
pending_on: 'TT'
},
{
product_id: 'O145029',
product_name: '264_BLR_P-MP_4MO_ZONEINTERVILLE~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be purged',
pending_on: '-'
},
{
product_id: 'O145036',
product_name: '163_FO_POINT_MULTIPOINT_ZONE_ABIDJ~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be purged',
pending_on: '-'
},
{
product_id: 'O145060',
product_name: '169_MULTIPOINT-ZONEABIDJAN-5MEGA~1.0',
lob: 'ISP',
start_date: '1-Aug-25',
end_date: '30-Aug-25',
Rationalization_Status: 'To be migrated',
pending_on: 'TT'
}
];
// Get unique values for filter dropdowns
const uniqueLobs = Array.from(new Set(legacyProducts.map(p => p.lob).filter(Boolean))).sort();
const uniqueStatuses = Array.from(new Set(legacyProducts.map(p => p.Rationalization_Status).filter(Boolean))).sort();
// Filter products based on selected filters
const filteredProducts = legacyProducts.filter(p => {
const lobMatch = lobFilter ? (p.lob && p.lob.toLowerCase() === lobFilter.toLowerCase()) : true;
const statusMatch = statusFilter ? (p.Rationalization_Status && p.Rationalization_Status.toLowerCase() === statusFilter.toLowerCase()) : true;
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
transition: 'transform 0.3s ease, box-shadow 0.3s ease',
marginBottom: '1.5rem',
},
cardHover: {
transform: 'translateY(-3px)',
boxShadow: '0 4px 20px rgba(30, 64, 175, 0.12)',
},
cardTitle: {
fontSize: '2.2rem',
fontWeight: '700',
marginBottom: '1.5rem',
color: '#1e40af',
textAlign: 'center',
textShadow: '0 2px 4px rgba(30, 64, 175, 0.1)',
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
boxShadow: '0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06)',
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
color: '#ffffff',
borderBottom: '2px solid #1e40af',
},
tableRow: {
backgroundColor: '#ffffff',
borderBottom: '1px solid #e5e7eb',
transition: 'background-color 0.2s',
},
tableRowHover: {
backgroundColor: '#f9fafb',
},
tableCell: {
padding: '1rem 1.5rem',
fontSize: '0.875rem',
color: '#374151',
verticalAlign: 'middle',
},
statusCell: (color) => ({
padding: '0.25rem 0.75rem',
backgroundColor: color,
borderRadius: '9999px',
color: '#ffffff',
fontWeight: '500',
textAlign: 'center',
fontSize: '0.75rem',
display: 'inline-block',
}),
complexityCell: (color) => ({
padding: '0.25rem 0.75rem',
backgroundColor: color,
borderRadius: '9999px',
color: '#ffffff',
fontWeight: '500',
textAlign: 'center',
fontSize: '0.75rem',
display: 'inline-block',
}),
dependenciesCell: (color) => ({
padding: '0.25rem 0.75rem',
backgroundColor: color,
borderRadius: '9999px',
color: '#ffffff',
fontWeight: '500',
textAlign: 'center',
fontSize: '0.75rem',
display: 'inline-block',
}),
};
return (
<div style={styles.container}>
<div style={styles.card}>
<h2 style={styles.cardTitle}>Legacy Systems Inventory</h2>
<div style={styles.filtersContainer}>
<select
style={styles.select}
value={lobFilter}
onChange={(e) => setLobFilter(e.target.value)}>
<option value="">All LoBs</option>
{uniqueLobs.map((lob) => (
<option key={lob} value={lob}>
{lob}
</option>
))}
</select>
<select
style={styles.select}
value={statusFilter}
onChange={(e) => setStatusFilter(e.target.value)}>
<option value="">All Statuses</option>
{uniqueStatuses.map((status) => (
<option key={status} value={status}>
{status}
</option>
))}
</select>
</div>
<div style={{ overflowX: 'auto' }}>
<table style={styles.table}>
<thead style={styles.tableHeader}>
<tr>
<th style={styles.tableHeaderCell}>Product_id</th>
<th style={styles.tableHeaderCell}>product_name</th>
<th style={styles.tableHeaderCell}>lob</th>
<th style={styles.tableHeaderCell}>start_date</th>
<th style={styles.tableHeaderCell}>end_date</th>
<th style={styles.tableHeaderCell}>Rationalization_Status</th>
<th style={styles.tableHeaderCell}>pending_on</th>
</tr>
</thead>
<tbody>
{filteredProducts.map((product, idx) => (
<tr key={idx} style={styles.tableRow}>
<td style={styles.tableCell}>{product.product_id}</td>
<td style={styles.tableCell}>{product.product_name}</td>
<td style={styles.tableCell}>{product.lob}</td>
<td style={styles.tableCell}>{product.start_date}</td>
<td style={styles.tableCell}>{product.end_date}</td>
<td style={styles.tableCell}>{product.Rationalization_Status}</td>
<td style={styles.tableCell}>{product.pending_on}</td>
</tr>
))}
</tbody>
</table>
</div>
</div>
</div>
);
};
export default LegacyP;