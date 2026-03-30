import React from 'react';
import { BarChart, Bar, ResponsiveContainer, XAxis, YAxis, Tooltip } from 'recharts';
import { Activity, ArrowLeft } from 'lucide-react';

const conversionData = [
{ name: 'Jan', conversion: 2.8 }, { name: 'Feb', conversion: 3.1 }, { name: 'Mar', conversion: 2.5 }, { name: 'Apr', conversion: 3.2 }, { name: 'May', conversion: 2.9 }, { name: 'Jun', conversion: 3.0 }, { name: 'Jul', conversion: 3.4 }, ];

const ConversionPage = ({ onBack }) => {
return ( <div className="bg-slate-100 font-sans h-screen overflow-y-auto">
<main className="max-w-screen-xl mx-auto p-4 md:p-8 pt-24">
<div className="flex justify-between items-center mb-6">
<div>
<h1 className="text-3xl font-bold text-gray-800 flex items-center">
<Activity className="mr-3" size={32} />
Conversion Analytics
</h1>
<p className="text-gray-500">Detailed conversion rate and
funnel metrics</p>
</div>
<button
onClick={onBack}
className="px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors flex items-center">

<ArrowLeft size={16} className="mr-2" />
Back to Dashboard
</button>
</div>

<div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
<div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
<h3 className="text-lg font-semibold text-gray-800 mb-2">Conversion Rate</h3>
<p className="text-3xl font-bold text-green-600">2.8%</p>
<p className="text-sm text-green-600">+5.6% from last
month</p>
</div>
<div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
<h3 className="text-lg font-semibold text-gray-800 mb-2">Visitors to Leads</h3>
<p className="text-3xl font-bold text-blue-600">4.2%</p>
<p className="text-sm text-green-600">+2.1% from last
month</p>
</div>
<div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
<h3 className="text-lg font-semibold text-gray-800 mb-2">Leads to Customers</h3>
<p className="text-3xl font-bold text-purple-600">65.8%</p>
<p className="text-sm text-red-600">-1.3% from last
month</p>
</div>
<div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
<h3 className="text-lg font-semibold text-gray-800 mb-2">Bounce Rate</h3>
<p className="text-3xl font-bold text-red-600">42.1%</p>
<p className="text-sm text-green-600">-8.7% from last
month</p>
</div>
</div>

<div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
<h2 className="text-xl font-semibold text-gray-800 mb-4">Conversion Rate Trend</h2>
<div className="h-80">
<ResponsiveContainer width="100%" height="100%">
<BarChart data={conversionData}>
<XAxis dataKey="name" />
<YAxis />
<Tooltip />
<Bar dataKey="conversion" fill="#8884d8" />
</BarChart>
</ResponsiveContainer>
</div>
</div>
</main>
</div>
);
};

export default ConversionPage;
