import React from 'react';
import { LineChart, Line, ResponsiveContainer, XAxis, YAxis, Tooltip } from 'recharts';
import { DollarSign, ArrowLeft } from 'lucide-react';

const incomeData = [
  { name: 'Jan', income: 8450 }, { name: 'Feb', income: 9200 },
  { name: 'Mar', income: 7800 }, { name: 'Apr', income: 10500 },
  { name: 'May', income: 8900 }, { name: 'Jun', income: 11200 },
  { name: 'Jul', income: 9800 },
];

const IncomePage = ({ onBack }) => {
  return (
    <div className="bg-slate-100 font-sans h-screen overflow-y-auto">
      <main className="max-w-screen-xl mx-auto p-4 md:p-8 pt-24">
        <div className="flex justify-between items-center mb-6">
          <div>
            <h1 className="text-3xl font-bold text-gray-800 flex items-center">
              <DollarSign className="mr-3" size={32} />
              Income Analytics
            </h1>
            <p className="text-gray-500">Revenue trends and financial metrics</p>
          </div>
          <button
            onClick={onBack}
            className="px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors flex items-center"
          >
            <ArrowLeft size={16} className="mr-2" />
            Back to Dashboard
          </button>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
          <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
            <h3 className="text-lg font-semibold text-gray-800 mb-2">Total Income</h3>
            <p className="text-3xl font-bold text-green-600">$8,450</p>
            <p className="text-sm text-green-600">+42.8% from last month</p>
          </div>
          <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
            <h3 className="text-lg font-semibold text-gray-800 mb-2">Monthly Growth</h3>
            <p className="text-3xl font-bold text-blue-600">23.4%</p>
            <p className="text-sm text-green-600">Best quarter ever</p>
          </div>
          <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
            <h3 className="text-lg font-semibold text-gray-800 mb-2">Avg Order Value</h3>
            <p className="text-3xl font-bold text-purple-600">$67.89</p>
            <p className="text-sm text-green-600">+8.2% from last month</p>
          </div>
          <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
            <h3 className="text-lg font-semibold text-gray-800 mb-2">Profit Margin</h3>
            <p className="text-3xl font-bold text-orange-600">32.7%</p>
            <p className="text-sm text-green-600">Target: 35%</p>
          </div>
        </div>

        <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
          <h2 className="text-xl font-semibold text-gray-800 mb-4">Income Trend</h2>
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={incomeData}>
                <XAxis dataKey="name" />
                <YAxis />
                <Tooltip />
                <Line type="monotone" dataKey="income" stroke="#10b981" strokeWidth={3} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </main>
    </div>
  );
};

export default IncomePage;

