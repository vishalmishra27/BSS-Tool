import React from 'react';
import { LineChart, Line, ResponsiveContainer, XAxis, YAxis, Tooltip } from 'recharts';
import { ShoppingCart, ArrowLeft } from 'lucide-react';

const ordersData = [
{ name: 'Jan', orders: 400 }, { name: 'Feb', orders: 300 }, { name: 'Mar', orders: 200 }, { name: 'Apr', orders: 278 }, { name: 'May', orders: 189 }, { name: 'Jun', orders: 239 }, { name: 'Jul', orders: 349 }, ];

const OrdersPage = ({ onBack }) => {
return ( <div className="bg-slate-100 font-sans h-screen overflow-y-auto">
<main className="max-w-screen-xl mx-auto p-4 md:p-8 pt-24">
<div className="flex justify-between items-center mb-6">
<div>
<h1 className="text-3xl font-bold text-gray-800 flex items-center">
<ShoppingCart className="mr-3" size={32} />
Orders Analytics
</h1>
<p className="text-gray-500">Detailed order and
transaction metrics</p>
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
<h3 className="text-lg font-semibold text-gray-800 mb-2">Total Orders</h3>
<p className="text-3xl font-bold text-blue-600">1,287</p>
<p className="text-sm text-red-600">-10.2% from last
month</p>
</div>
<div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
<h3 className="text-lg font-semibold text-gray-800 mb-2">Pending Orders</h3>
<p className="text-3xl font-bold text-yellow-600">156</p>
<p className="text-sm text-green-600">-5.4% from last
month</p>
</div>
<div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
<h3 className="text-lg font-semibold text-gray-800 mb-2">Completed Orders</h3>
<p className="text-3xl font-bold text-green-600">1,131</p>
<p className="text-sm text-red-600">-8.9% from last
month</p>
</div>
<div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
<h3 className="text-lg font-semibold text-gray-800 mb-2">Average Order Time</h3>
<p className="text-3xl font-bold text-purple-600">2.3h</p>
<p className="text-sm text-green-600">-12.1% from last
month</p>
</div>
</div>

<div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
<h2 className="text-xl font-semibold text-gray-800 mb-4">Orders Trend</h2>
<div className="h-80">
<ResponsiveContainer width="100%" height="100%">
<LineChart data={ordersData}>
<XAxis dataKey="name" />
<YAxis />
<Tooltip />
<Line type="monotone" dataKey="orders"
stroke="#ff7300" strokeWidth={2} />
</LineChart>
</ResponsiveContainer>
</div>
</div>
</main>
</div>
);
};

export default OrdersPage;
