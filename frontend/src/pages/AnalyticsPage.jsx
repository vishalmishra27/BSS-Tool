import React, { useEffect } from 'react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
} from 'recharts';

const salesData = [
  { name: 'Jan', sales: 4000, revenue: 2400 },
  { name: 'Feb', sales: 3000, revenue: 1398 },
  { name: 'Mar', sales: 2000, revenue: 9800 },
  { name: 'Apr', sales: 2780, revenue: 3908 },
  { name: 'May', sales: 1890, revenue: 4800 },
  { name: 'Jun', sales: 2390, revenue: 3800 },
  { name: 'Jul', sales: 3490, revenue: 4300 },
];

const trafficData = [
  { name: 'Direct', value: 400, color: '#0088FE' },
  { name: 'Organic', value: 300, color: '#00C49F' },
  { name: 'Referral', value: 300, color: '#FFBB28' },
  { name: 'Social', value: 200, color: '#FF8042' },
];

const performanceData = [
  { name: 'Q1', performance: 22 },
  { name: 'Q2', performance: 45 },
  { name: 'Q3', performance: 78 },
  { name: 'Q4', performance: 56 },
];

const ChartCard = ({ title, children }) => (
  <div className="bg-white p-6 rounded-xl shadow-md border border-gray-200">
    <h3 className="text-lg font-semibold text-gray-700 mb-4">{title}</h3>
    <div style={{ width: '100%', height: 300 }}>{children}</div>
  </div>
);

const scrollbarStyles = `
  .custom-scrollbar::-webkit-scrollbar {
    width: 12px;
  }

  .custom-scrollbar::-webkit-scrollbar-track {
    background: #f1f1f1;
  }

  .custom-scrollbar::-webkit-scrollbar-thumb {
    background-color: #a8a8a8;
    border-radius: 10px;
    border: 3px solid #f1f1f1;
  }

  .custom-scrollbar::-webkit-scrollbar-thumb:hover {
    background: #555;
  }
`;

const AnalyticsPage = () => {
  useEffect(() => {
    const styleElement = document.createElement('style');
    styleElement.innerHTML = scrollbarStyles;

    document.head.appendChild(styleElement);
    return () => {
      document.head.removeChild(styleElement);
    };
  }, []);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch('/api/analytics');
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        await response.json();
        alert('Data fetched successfully');
      } catch (error) {
        alert('Failed to fetch data: ' + error.message);
      }
    };
    fetchData();
  }, []);

  return (
    <div className="bg-gray-50 font-sans h-screen overflow-y-auto custom-scrollbar">
      <main className="max-w-7xl mx-auto p-4 md:p-8 pt-24">
        <h1 className="text-3xl font-bold text-gray-800 mb-6">Analytics & Reports</h1>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          <ChartCard title="Sales Over Time">
            <ResponsiveContainer>
              <LineChart data={salesData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey="sales" stroke="#8884d8" activeDot={{ r: 8 }} />
                <Line type="monotone" dataKey="revenue" stroke="#82ca9d" />
              </LineChart>
            </ResponsiveContainer>
          </ChartCard>

          <ChartCard title="Traffic Sources">
            <ResponsiveContainer>
              <PieChart>
                <Pie
                  data={trafficData}
                  dataKey="value"
                  nameKey="name"
                  cx="50%"
                  cy="50%"
                  outerRadius={100}
                  label
                >
                  {trafficData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </ChartCard>

          <div className="lg:col-span-2">
            <ChartCard title="Quarterly Performance">
              <ResponsiveContainer>
                <BarChart data={performanceData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="name" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Bar dataKey="performance" fill="#8884d8" />
                </BarChart>
              </ResponsiveContainer>
            </ChartCard>
          </div>
        </div>
      </main>
    </div>
  );
};

export default AnalyticsPage;
