import React, { useEffect, useState } from 'react';
import TransformationDashboardStatic from './TransformationDashboardStatic';
const TransformationDashboardSmart = () => {
const [useStatic, setUseStatic] = useState(false);
const [loading, setLoading] = useState(true);
const [error, setError] = useState(null);
useEffect(() => {
const checkBackendAvailability = async () => {
try {
// Try to ping the backend
const response = await fetch('/api/health', {
method: 'GET',
headers: {
'Content-Type': 'application/json',
},
// Add timeout to prevent hanging
signal: AbortSignal.timeout(3000)
});
if (response.ok) {
// Backend is available, try to load dynamic dashboard
setUseStatic(false);
} else {
// Backend responded but with error, use static
setUseStatic(true);
}
} catch (error) {
// Network error or timeout, use static dashboard
console.log('Backend unavailable, falling back to static dashboard:', error.message);
setUseStatic(true);
} finally {
setLoading(false);
}
};
checkBackendAvailability();
}, []);
if (loading) {
return (
<div style={{
height: '100vh',
background: 'linear-gradient(135deg, #f8fafc 0%, #e2e8f0 50%, #cbd5e1 100%)',
display: 'flex',
alignItems: 'center',
justifyContent: 'center',
fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
}}>
<div style={{
textAlign: 'center',
padding: '2rem',
background: '#ffffff',
borderRadius: '12px',
boxShadow: '0 4px 20px rgba(30, 64, 175, 0.1)',
}}>
<div style={{
fontSize: '1.5rem',
color: '#1e40af',
marginBottom: '1rem',
}}>
Checking backend availability...
</div>
<div style={{
width: '40px',
height: '40px',
border: '4px solid #e2e8f0',
borderTop: '4px solid #1e40af',
borderRadius: '50%',
animation: 'spin 1s linear infinite',
margin: '0 auto',
}}></div>
</div>
</div>
);
}
if (useStatic) {
return <TransformationDashboardStatic />;
}
// If backend is available, load the dynamic dashboard
// This would typically import and render the original dynamic component
return <TransformationDashboardDynamic />;
};
// Fallback dynamic component that handles its own loading
const TransformationDashboardDynamic = () => {
const [loading, setLoading] = useState(true);
const [error, setError] = useState(null);
const [data, setData] = useState(null);
useEffect(() => {
const fetchData = async () => {
try {
const [overviewRes, activitiesRes, attentionRes] = await Promise.all([
fetch('/api/project_overview'),
fetch('/api/project_activities'),
fetch('/api/attention_areas')
]);
if (!overviewRes.ok || !activitiesRes.ok || !attentionRes.ok) {
throw new Error('Failed to fetch data from backend');
}
const [overview, activities, attentionAreas] = await Promise.all([
overviewRes.json(),
activitiesRes.json(),
attentionRes.json()
]);
setData({ overview, activities, attentionAreas });
setLoading(false);
} catch (error) {
console.error('Failed to load dynamic data:', error);
// Fallback to static if dynamic loading fails
setError(true);
setLoading(false);
}
};
fetchData();
}, []);
if (error) {
// If dynamic loading fails, show static
return <TransformationDashboardStatic />;
}
if (loading) {
return (
<div style={{
height: '100vh',
background: 'linear-gradient(135deg, #f8fafc 0%, #e2e8f0 50%, #cbd5e1 100%)',
display: 'flex',
alignItems: 'center',
justifyContent: 'center',
fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
}}>
<div style={{
textAlign: 'center',
padding: '2rem',
background: '#ffffff',
borderRadius: '12px',
boxShadow: '0 4px 20px rgba(30, 64, 175, 0.1)',
}}>
<div style={{
fontSize: '1.5rem',
color: '#1e40af',
marginBottom: '1rem',
}}>
Loading transformation dashboard...
</div>
<div style={{
width: '40px',
height: '40px',
border: '4px solid #e2e8f0',
borderTop: '4px solid #1e40af',
borderRadius: '50%',
animation: 'spin 1s linear infinite',
margin: '0 auto',
}}></div>
</div>
</div>
);
}
// This would render the original dynamic dashboard with the fetched data
// For now, we'll use the static version as a fallback
return <TransformationDashboardStatic />;
};
// Add CSS for spinner animation
const style = document.createElement('style');
style.textContent = `
@keyframes spin {
0% { transform: rotate(0deg); }
100% { transform: rotate(360deg); }
}
`;
document.head.appendChild(style);
export default TransformationDashboardSmart;