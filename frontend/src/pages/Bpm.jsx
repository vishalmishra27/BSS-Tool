import React from 'react';
const Bpm = () => {
return (
<div style={{ padding: '2rem', fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif" }}>
<h1 style={{ color: '#1e3c72', marginBottom: '1.5rem' }}>BPM Dashboard</h1>
<p style={{ color: '#666', marginBottom: '2rem' }}>Business Process Management overview and analytics</p>
<div style={{ 
display: 'grid', 
gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', 
gap: '1.5rem', 
marginTop: '2rem' 
}}>
<div style={{ 
background: '#fff', 
padding: '2rem', 
borderRadius: '12px',
boxShadow: '0 4px 6px rgba(0,0,0,0.1)',
border: '1px solid #e1e4eb'
}}>
<h3 style={{ color: '#1e3c72', marginBottom: '1rem' }}>Process Overview</h3>
<p style={{ color: '#666' }}>View and manage business processes across all departments</p>
<div style={{ marginTop: '1rem', padding: '1rem', background: '#f8f9fa', borderRadius: '8px' }}>
<strong>Active Processes:</strong> 12
</div>
</div>
<div style={{ 
background: '#fff', 
padding: '2rem', 
borderRadius: '12px',
boxShadow: '0 4px 6px rgba(0,0,0,0.1)',
border: '1px solid #e1e4eb'
}}>
<h3 style={{ color: '#1e3c72', marginBottom: '1rem' }}>Performance Metrics</h3>
<p style={{ color: '#666' }}>Track BPM performance indicators and KPIs</p>
<div style={{ marginTop: '1rem', padding: '1rem', background: '#f8f9fa', borderRadius: '8px' }}>
<strong>Efficiency Rate:</strong> 87%
</div>
</div>
<div style={{ 
background: '#fff', 
padding: '2rem', 
borderRadius: '12px',
boxShadow: '0 4px 6px rgba(0,0,0,0.1)',
border: '1px solid #e1e4eb'
}}>
<h3 style={{ color: '#1e3c72', marginBottom: '1rem' }}>Process Optimization</h3>
<p style={{ color: '#666' }}>Optimize and improve processes for better efficiency</p>
<div style={{ marginTop: '1rem', padding: '1rem', background: '#f8f9fa', borderRadius: '8px' }}>
<strong>Improvements:</strong> 5 pending
</div>
</div>
</div>
<div style={{ marginTop: '3rem' }}>
<h2 style={{ color: '#1e3c72', marginBottom: '1rem' }}>Summary</h2>
<div style={{ 
background: '#fff', 
padding: '2rem', 
borderRadius: '12px',
boxShadow: '0 4px 6px rgba(0,0,0,0.1)',
border: '1px solid #e1e4eb'
}}>
<h3 style={{ color: '#1e3c72', marginBottom: '1rem' }}>Business Process Management Summary</h3>
<div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))', gap: '1rem' }}>
<div>
<strong>Total Processes:</strong> 24
</div>
<div>
<strong>Active Projects:</strong> 8
</div>
<div>
<strong>Active Tasks:</strong> 15
</div>
<div>
<strong>Completion Rate:</strong> 92%
</div>
</div>
</div>
</div>
</div>
);
};
export default Bpm;