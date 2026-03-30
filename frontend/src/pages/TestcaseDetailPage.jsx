import { useState, useEffect } from 'react';
import { useParams } from 'react-router-dom';

const TestcaseDetailPage = () => {
const { id } = useParams();
const [task, setTask] = useState(null);
const [loading, setLoading] = useState(true);
const [error, setError] = useState(null);
const [commentInput, setCommentInput] = useState('');
const [comments, setComments] = useState([]);

useEffect(() => {
const fetchTestcase = async () => {
try {
const response = await
fetch('/UAT/' + id);
if (!response.ok) {
throw new Error('Failed to fetch testcase details');
}
const data = await
response.json();
setTask(data);
if (data.activityComments)
{

setComments(data.activityComments);
}
} catch (err) {
setError(err.message);
} finally {
setLoading(false);
}
};
fetchTestcase();
}, [id]);

if (loading) {
return <div style={{ color: 'white', padding: '2rem' }}>Loading testcase details...</div>;
}
if (error) {
return <div style={{ color: 'red', padding: '2rem' }}>Error: {error}</div>;
}
if (!task) {
return ( <div style={{ padding: '2rem', color: 'white' }}>
<h2>Test Case not found</h2>
</div>
);
}
const handleCommentChange = (e) => {

setCommentInput(e.target.value);
};

const handleSendClick = () => {
if (commentInput.trim() !== '') {
setComments(prevComments => [...prevComments, commentInput.trim()]);
setCommentInput('');
}
};

const styles = {
container: {
height: '100vh', background: 'linear-gradient(135deg, #00215a 0%, #1e3a8a 50%, #3059a0 100%)', color: '#ffffff', padding: '2rem', fontFamily: "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif", overflowY: 'auto', overflowX: 'hidden', }, card: {
background: 'rgba(255, 255, 255, 0.1)', backdropFilter: 'blur(20px)', borderRadius: '20px', padding: '2rem', border: '1px solid rgba(255, 255, 255, 0.2)', boxShadow: '0 8px 32px rgba(0, 0, 0, 0.3)', maxWidth: '800px', margin: '0 auto', }, title: {
fontSize: '2rem', fontWeight: '700', marginBottom: '1rem', }, description: {
marginBottom: '1rem', }, detailRow: {
marginBottom: '0.5rem', }, label: {
fontWeight: '600', marginRight: '0.5rem', }, commentBox: {
marginTop: '2rem', }, textarea: {
width: '100%', minHeight: '80px', background: 'rgba(0, 0, 0, 0.3)', border: '1px solid rgba(255, 255, 255, 0.2)', borderRadius: '8px', padding: '0.75rem', color: '#ffffff', fontSize: '0.9rem', resize: 'vertical', outline: 'none', transition: 'border-color 0.3s ease', }, sendButton: {
background: 'linear-gradient(135deg, #3b82f6, #2563eb)', color: '#ffffff', border: 'none', borderRadius: '8px', padding: '0.6rem 1.2rem', fontSize: '0.9rem', fontWeight: '600', cursor: 'pointer', transition: 'all 0.3s ease', marginTop: '0.5rem', float: 'right', }, comment: {
background: 'rgba(255, 255, 255, 0.05)', borderRadius: '8px', padding: '1rem', marginBottom: '1rem', border: '1px solid rgba(255, 255, 255, 0.1)', }, commentText: {
color: 'rgba(255, 255, 255, 0.9)', lineHeight: '1.5', }, };

return ( <div style={styles.container}>
<div style={styles.card}>
<h2
style={styles.title}>{task.testCase}: {task.title}</h2>
<p
style={styles.description}>{task.description}</p>
<div
style={styles.detailRow}><span style={styles.label}>LoB:</span>
{task.lob}</div>
<div
style={styles.detailRow}><span style={styles.label}>Priority:</span>
{task.priority}</div>
<div
style={styles.detailRow}><span style={styles.label}>Validation
Status:</span> {task.validationStatus}</div>
<div
style={styles.detailRow}><span style={styles.label}>Client Status:</span>
{task.jiraStatus}</div>
<div
style={styles.detailRow}><span style={styles.label}>KPMG Status:</span>
{task.kpmgStatus}</div>

<div
style={styles.commentBox}>
<textarea
style={styles.textarea}
placeholder="Add a comment..."
value={commentInput}

onChange={handleCommentChange}
onFocus={(e) => e.target.style.borderColor = 'rgba(59, 130, 246, 0.5)'}
onBlur={(e) => e.target.style.borderColor = 'rgba(255, 255, 255, 0.2)'}
/>
<button

style={styles.sendButton}

onClick={handleSendClick}
onMouseEnter={(e) => {

e.target.style.background = 'linear-gradient(135deg, #2563eb, #1d4ed8)';

e.target.style.transform = 'translateY(-1px)';
}}
onMouseLeave={(e) => {
e.target.style.background = 'linear-gradient(135deg, #3b82f6, #2563eb)';
e.target.style.transform = 'translateY(0)';
}}>
Send
</button>
<div style={{ clear: 'both' }}></div>
</div>

<div>
{comments.map((comment, index) => (
<div key={index} style={styles.comment}>
<div style={styles.commentText}>{comment}</div>
</div>
))}
</div>
</div>
</div>
);
};

export default TestcaseDetailPage;
