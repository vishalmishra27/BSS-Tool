import React, { useEffect, useState } from 'react';
import TransformationDashboardPage from './TransformationDashboardPage';
import TransformationDashboardStatic from './TransformationDashboardStatic';
const TransformationDashboardSmartFixed = () => {
const [dashboardType, setDashboardType] = useState('loading');
const [error, setError] = useState(null);
useEffect(() => {
const checkBackend = async () => {
try {
console.log('Checking backend connectivity...');
// Test health endpoint
const healthResponse = await fetch('/api/health');
if (!healthResponse.ok) {
console.log('Health check failed, using static dashboard');
setDashboardType('static');
return;
} 
// Test actual data endpoints
const [overviewRes, activitiesRes, attentionRes] = await Promise.all([
fetch('/api/project_overview'),
fetch('/api/project_activities'),
fetch('/api/attention_areas')
]);
console.log('Endpoint responses:', {
overview: overviewRes.status,
activities: activitiesRes.status,
attention: attentionRes.status
});
// Check if all endpoints return 200
if (overviewRes.ok && activitiesRes.ok && attentionRes.ok) {
console.log('All endpoints working, using dynamic dashboard');
setDashboardType('dynamic');
} else {
console.log('Some endpoints failed, using static dashboard');
setDashboardType('static');
}
} catch (error) {
console.error('Backend connection failed:', error);
setDashboardType('static');
setError(error.message);
}
};
checkBackend();
}, []);
if (dashboardType === 'loading') {
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
Checking backend connectivity...
</div>
{error && (
<div style={{ color: '#ef4444', marginTop: '1rem' }}>
Error: {error}
</div>
)}
</div>
</div>
);
}
// Use the original TransformationDashboardPage when backend is available
if (dashboardType === 'dynamic') {
return <TransformationDashboardPage />;
}
// Fallback to static dashboard
return <TransformationDashboardStatic />;
};