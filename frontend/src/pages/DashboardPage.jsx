import React, { useEffect, useState } from 'react';
import {
  AreaChart,
  Area,
  LineChart,
  Line,
  BarChart,
  Bar,
  ResponsiveContainer,
  XAxis,
  YAxis,
  Tooltip,
} from 'recharts';
import {
  Users,
  DollarSign,
  Activity,
  ShoppingCart,
  CheckCircle,
  Clock,
  MoreVertical,
  Upload,
  X,
  FileText,
  Database,
  TrendingUp,
  PieChart,
  BarChart3,
  Target,
  Play,
  Trash2,
} from 'lucide-react';

const salesData = [
  { name: 'Jan', uv: 4000, pv: 2400 },
  { name: 'Feb', uv: 3000, pv: 1398 },
  { name: 'Mar', uv: 2000, pv: 9800 },
  { name: 'Apr', uv: 2780, pv: 3908 },
  { name: 'May', uv: 1890, pv: 4800 },
  { name: 'Jun', uv: 2390, pv: 3800 },
  { name: 'Jul', uv: 3490, pv: 4300 },
];

const projectsData = [
  { name: 'Project Alpha', lead: 'Alice Johnson', status: 'Finished', budget: '$5.2K' },
  { name: 'Project Beta', lead: 'Bob Williams', status: 'In Progress', budget: '$12.5K' },
  { name: 'Project Gamma', lead: 'Charlie Brown', status: 'Pending', budget: '$3.1K' },
  { name: 'Project Delta', lead: 'Diana Miller', status: 'Finished', budget: '$8.9K' },
];

const activityFeed = [
  { text: 'New user registered', time: '20 min ago' },
  { text: 'Server #1 overloaded', time: '1 hour ago' },
  { text: 'New order received', time: '3 hours ago' },
  { text: 'Server #2 overloaded', time: '5 hours ago' },
  { text: 'New user registered', time: '8 hours ago' },
];

const cardTypes = [
  {
    id: 1,
    title: 'CBS vs CLM Reconciliation',
    icon: DollarSign,
    description: 'Service Data Reconciliation',
    color: 'from-green-500 to-green-700',
  },
  {
    id: 2,
    title: 'User Analytics',
    icon: Users,
    description: 'User Behavior & Engagement',
    color: 'from-blue-500 to-blue-700',
  },
  {
    id: 3,
    title: 'Sales Performance',
    icon: TrendingUp,
    description: 'Sales Metrics & Trends',
    color: 'from-purple-500 to-purple-700',
  },
  {
    id: 4,
    title: 'Inventory Management',
    icon: ShoppingCart,
    description: 'Stock & Supply Chain',
    color: 'from-orange-500 to-orange-700',
  },
  {
    id: 5,
    title: 'Marketing ROI',
    icon: Target,
    description: 'Campaign Performance',
    color: 'from-pink-500 to-pink-700',
  },
  {
    id: 6,
    title: 'Operational Efficiency',
    icon: Activity,
    description: 'Process Optimization',
    color: 'from-indigo-500 to-indigo-700',
  },
  {
    id: 7,
    title: 'Data Quality',
    icon: Database,
    description: 'Data Integrity Check',
    color: 'from-teal-500 to-teal-700',
  },
  {
    id: 8,
    title: 'Custom KPI',
    icon: BarChart3,
    description: 'Custom Metrics Analysis',
    color: 'from-red-500 to-red-700',
  },
];

const FileUploadModal = ({ isOpen, onClose, selectedCard, onFilesUploaded }) => {
  const [uploadedFiles, setUploadedFiles] = useState([]);
  const [isDragging, setIsDragging] = useState(false);

  const handleFileUpload = (files) => {
    const newFiles = Array.from(files).map((file) => ({
      id: Date.now() + Math.random(),
      name: file.name,
      size: file.size,
      type: file.type,
      file: file,
    }));
    setUploadedFiles((prev) => [...prev, ...newFiles]);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setIsDragging(false);
    const files = e.dataTransfer.files;
    handleFileUpload(files);
  };

  const handleDragOver = (e) => {
    e.preventDefault();
    setIsDragging(true);
  };

  const handleDragLeave = (e) => {
    e.preventDefault();
    setIsDragging(false);
  };

  const removeFile = (fileId) => {
    setUploadedFiles((prev) => prev.filter((file) => file.id !== fileId));
  };

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const handleProceed = () => {
    if (uploadedFiles.length > 0) {
      onFilesUploaded(uploadedFiles, selectedCard);
      setUploadedFiles([]);
      onClose();
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg p-6 w-full max-w-2xl max-h-[90vh] overflow-y-auto">
        <div className="flex justify-between items-center mb-6">
          <div>
            <h2 className="text-2xl font-bold text-gray-800">Upload Datasets</h2>
            <p className="text-gray-600">Upload files for {selectedCard?.title}</p>
          </div>
          <button onClick={onClose} className="text-gray-500 hover:text-gray-700">
            <X size={24} />
          </button>
        </div>

        <div
          className={`border-2 border-dashed rounded-lg p-8 text-center mb-6 transition-colors ${
            isDragging ? 'border-blue-500 bg-blue-50' : 'border-gray-300 hover:border-gray-400'
          }`}
          onDrop={handleDrop}
          onDragOver={handleDragOver}
          onDragLeave={handleDragLeave}
        >
          <Upload size={48} className="mx-auto text-gray-400 mb-4" />
          <p className="text-lg font-medium text-gray-700 mb-2">Drag and drop files here, or click to select</p>
          <p className="text-sm text-gray-500 mb-4">Supports CSV, Excel, JSON, and text files</p>
          <input
            type="file"
            multiple
            accept=".csv,.xlsx,.xls,.json,.txt"
            onChange={(e) => handleFileUpload(e.target.files)}
            className="hidden"
            id="file-upload"
          />
          <label
            htmlFor="file-upload"
            className="bg-blue-600 text-white px-6 py-2 rounded-lg cursor-pointer hover:bg-blue-700 transition-colors"
          >
            Select Files
          </label>
        </div>

        {uploadedFiles.length > 0 && (
          <div className="mb-6">
            <h3 className="text-lg font-semibold text-gray-800 mb-3">
              Uploaded Files ({uploadedFiles.length})
            </h3>
            <div className="space-y-2 max-h-60 overflow-y-auto">
              {uploadedFiles.map((file) => (
                <div key={file.id} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                  <div className="flex items-center space-x-3">
                    <FileText size={20} className="text-blue-600" />
                    <div>
                      <p className="font-medium text-gray-800">{file.name}</p>
                      <p className="text-sm text-gray-500">{formatFileSize(file.size)}</p>
                    </div>
                  </div>
                  <button onClick={() => removeFile(file.id)} className="text-red-500 hover:text-red-700">
                    <Trash2 size={16} />
                  </button>
                </div>
              ))}
            </div>
          </div>
        )}

        <div className="flex justify-end space-x-4">
          <button
            onClick={onClose}
            className="px-6 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleProceed}
            disabled={uploadedFiles.length === 0}
            className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors"
          >
            Proceed to Analysis
          </button>
        </div>
      </div>
    </div>
  );
};

const KPIAnalysisPage = ({ files, cardType, onBack }) => {
  const [isProcessing, setIsProcessing] = useState(false);
  const [analysisResults, setAnalysisResults] = useState(null);

  const executeKPIAnalysis = async () => {
    setIsProcessing(true);

    await new Promise((resolve) => setTimeout(resolve, 3000));

    const mockResults = {
      summary: {
        totalRecords: Math.floor(Math.random() * 10000) + 1000,
        processedFiles: files.length,
        accuracy: (Math.random() * 10 + 90).toFixed(2),
        completionTime: new Date().toLocaleTimeString(),
      },
      metrics: [
        { name: 'Data Quality Score', value: (Math.random() * 20 + 80).toFixed(1), unit: '%' },
        { name: 'Reconciliation Rate', value: (Math.random() * 15 + 85).toFixed(1), unit: '%' },
        { name: 'Error Rate', value: (Math.random() * 5).toFixed(2), unit: '%' },
        {
          name: 'Processing Speed',
          value: (Math.random() * 500 + 1000).toFixed(0),
          unit: 'records/sec',
        },
      ],
      issues: [
        {
          type: 'Warning',
          message: 'Missing values detected in 3 columns',
          count: Math.floor(Math.random() * 50),
        },
        {
          type: 'Error',
          message: 'Data type mismatch in date fields',
          count: Math.floor(Math.random() * 20),
        },
        {
          type: 'Info',
          message: 'Duplicate records found and removed',
          count: Math.floor(Math.random() * 100),
        },
      ],
    };

    setAnalysisResults(mockResults);
    setIsProcessing(false);
  };

  return (
    <div className="bg-slate-100 font-sans h-screen overflow-y-auto">
      <main className="max-w-screen-xl mx-auto p-4 md:p-8 pt-24">
        <div className="flex justify-between items-center mb-6">
          <div>
            <h1 className="text-3xl font-bold text-gray-800">KPI Analysis - {cardType.title}</h1>
            <p className="text-gray-500">Reconciliation and analysis of uploaded datasets</p>
          </div>
          <button
            onClick={onBack}
            className="px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors"
          >
            Back to Dashboard
          </button>
        </div>

        <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200 mb-6">
          <h2 className="text-xl font-semibold text-gray-800 mb-4">Uploaded Files</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {files.map((file) => (
              <div key={file.id} className="flex items-center space-x-3 p-3 bg-gray-50 rounded-lg">
                <FileText size={20} className="text-blue-600" />
                <div>
                  <p className="font-medium text-gray-800">{file.name}</p>
                  <p className="text-sm text-gray-500">{(file.size / 1024).toFixed(1)} KB</p>
                </div>
              </div>
            ))}
          </div>
        </div>

        {!analysisResults && (
          <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200 mb-6 text-center">
            <h2 className="text-xl font-semibold text-gray-800 mb-4">Ready to Execute KPI Analysis</h2>
            <p className="text-gray-600 mb-6">
              Click the button below to start the reconciliation process for your uploaded datasets.
            </p>
            <button
              onClick={executeKPIAnalysis}
              disabled={isProcessing}
              className="px-8 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors flex items-center space-x-2 mx-auto"
            >
              {isProcessing ? (
                <>
                  <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
                  <span>Processing...</span>
                </>
              ) : (
                <>
                  <Play size={20} />
                  <span>Execute KPI Reconciliation</span>
                </>
              )}
            </button>
          </div>
        )}

        {analysisResults && (
          <div className="space-y-6">
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
              <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
                <h3 className="text-lg font-semibold text-gray-800 mb-2">Total Records</h3>
                <p className="text-3xl font-bold text-blue-600">{analysisResults.summary.totalRecords.toLocaleString()}</p>
              </div>
              <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
                <h3 className="text-lg font-semibold text-gray-800 mb-2">Files Processed</h3>
                <p className="text-3xl font-bold text-green-600">{analysisResults.summary.processedFiles}</p>
              </div>
              <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
                <h3 className="text-lg font-semibold text-gray-800 mb-2">Accuracy</h3>
                <p className="text-3xl font-bold text-purple-600">{analysisResults.summary.accuracy}%</p>
              </div>
              <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
                <h3 className="text-lg font-semibold text-gray-800 mb-2">Completed At</h3>
                <p className="text-lg font-semibold text-gray-700">{analysisResults.summary.completionTime}</p>
              </div>
            </div>

            <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
              <h2 className="text-xl font-semibold text-gray-800 mb-4">KPI Metrics</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                {analysisResults.metrics.map((metric, index) => (
                  <div key={index} className="flex justify-between items-center p-4 bg-gray-50 rounded-lg">
                    <span className="font-medium text-gray-800">{metric.name}</span>
                    <span className="text-xl font-bold text-blue-600">
                      {metric.value}
                      {metric.unit}
                    </span>
                  </div>
                ))}
              </div>
            </div>

            <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
              <h2 className="text-xl font-semibold text-gray-800 mb-4">Issues & Warnings</h2>
              <div className="space-y-3">
                {analysisResults.issues.map((issue, index) => (
                  <div
                    key={index}
                    className="flex items-center justify-between p-4 rounded-lg border-l-4 border-l-yellow-500 bg-yellow-50"
                  >
                    <div>
                      <span
                        className={`inline-block px-2 py-1 rounded text-xs font-semibold mr-3 ${
                          issue.type === 'Error'
                            ? 'bg-red-100 text-red-800'
                            : issue.type === 'Warning'
                            ? 'bg-yellow-100 text-yellow-800'
                            : 'bg-blue-100 text-blue-800'
                        }`}
                      >
                        {issue.type}
                      </span>
                      <span className="text-gray-800">{issue.message}</span>
                    </div>
                    <span className="font-semibold text-gray-600">{issue.count}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
};

const StatCard = ({ title, value, change, icon: Icon, chartData, chartType, onClick }) => {
  const chartColor = '#ffffff';
  return (
    <div
      className="bg-gradient-to-br from-[#004fd9] to-[#00338d] p-6 rounded-lg text-white shadow-lg transition-transform hover:-translate-y-1 cursor-pointer"
      onClick={onClick}
    >
      <div className="flex justify-between items-center">
        <div>
          <p className="text-sm uppercase opacity-80">{title}</p>
          <p className="text-3xl font-bold">{value}</p>
          <p className={`text-xs mt-1 ${change.startsWith('+') ? 'text-green-300' : 'text-red-300'}`}>{change}</p>
        </div>
        <div className="w-1/3 h-12">
          <ResponsiveContainer>
            {chartType === 'line' ? (
              <LineChart data={chartData}>
                <Tooltip contentStyle={{ backgroundColor: '#00215a', border: 'none' }} labelStyle={{ display: 'none' }} itemStyle={{ color: 'white' }} />
                <Line type="monotone" dataKey="uv" stroke={chartColor} strokeWidth={2} dot={false} />
              </LineChart>
            ) : (
              <BarChart data={chartData}>
                <Tooltip contentStyle={{ backgroundColor: '#00215a', border: 'none' }} labelStyle={{ display: 'none' }} itemStyle={{ color: 'white' }} />
                <Bar dataKey="uv" fill={chartColor} />
              </BarChart>
            )}
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
};

const KPICard = ({ cardType, onClick }) => {
  const IconComponent = cardType.icon;
  return (
    <div
      className={`bg-gradient-to-br ${cardType.color} p-6 rounded-lg text-white shadow-lg transition-transform hover:-translate-y-1 cursor-pointer`}
      onClick={() => onClick(cardType)}
    >
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold mb-2">{cardType.title}</h3>
          <p className="text-sm opacity-90">{cardType.description}</p>
        </div>
        <IconComponent size={32} className="opacity-80" />
      </div>
      <div className="mt-4 text-xs opacity-75">Click to upload datasets</div>
    </div>
  );
};

const SalesSummaryChart = () => (
  <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
    <h3 className="text-lg font-semibold text-gray-700 mb-1">Sales Summary</h3>
    <p className="text-sm text-gray-500 mb-4">Yearly Sales Report</p>
    <div className="h-72">
      <ResponsiveContainer>
        <AreaChart data={salesData} margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="colorUv" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#004fd9" stopOpacity={0.8} />
              <stop offset="95%" stopColor="#004fd9" stopOpacity={0} />
            </linearGradient>
            <linearGradient id="colorPv" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#4178d9" stopOpacity={0.8} />
              <stop offset="95%" stopColor="#4178d9" stopOpacity={0} />
            </linearGradient>
          </defs>
          <XAxis dataKey="name" />
          <YAxis />
          <Tooltip />
          <Area type="monotone" dataKey="uv" stroke="#00338d" fillOpacity={1} fill="url(#colorUv)" />
          <Area type="monotone" dataKey="pv" stroke="#3059a0" fillOpacity={1} fill="url(#colorPv)" />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  </div>
);

const ProjectsTable = () => {
  const getStatusClass = (status) => {
    if (status === 'Finished') return 'bg-green-100 text-green-800';
    if (status === 'In Progress') return 'bg-yellow-100 text-yellow-800';
    return 'bg-red-100 text-red-800';
  };
  return (
    <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
      <h3 className="text-lg font-semibold text-gray-700 mb-4">Recent Projects</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-sm text-left text-gray-600">
          <thead className="text-xs text-gray-700 uppercase bg-gray-50">
            <tr>
              <th scope="col" className="px-6 py-3">
                Project Name
              </th>
              <th scope="col" className="px-6 py-3">
                Team Lead
              </th>
              <th scope="col" className="px-6 py-3">
                Status
              </th>
              <th scope="col" className="px-6 py-3">
                Budget
              </th>
            </tr>
          </thead>
          <tbody>
            {projectsData.map((project, index) => (
              <tr key={index} className="bg-white border-b">
                <td className="px-6 py-4 font-medium text-gray-900">{project.name}</td>
                <td className="px-6 py-4">{project.lead}</td>
                <td className="px-6 py-4">
                  <span className={`px-2 py-1 rounded-full text-xs font-semibold ${getStatusClass(project.status)}`}>
                    {project.status}
                  </span>
                </td>
                <td className="px-6 py-4">{project.budget}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

const ActivityFeed = () => (
  <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200 h-full">
    <div className="flex justify-between items-center mb-4">
      <h3 className="text-lg font-semibold text-gray-700">Feeds</h3>
      <MoreVertical size={20} className="text-gray-400 cursor-pointer" />
    </div>
    <ul>
      {activityFeed.map((activity, index) => (
        <li key={index} className="flex items-start py-3 border-b border-gray-100 last:border-b-0">
          <div className="mr-3 mt-1">
            <span className="flex items-center justify-center h-8 w-8 rounded-full bg-[#e0e8f9] text-[#004fd9]">
              {index % 2 === 0 ? <CheckCircle size={16} /> : <Clock size={16} />}
            </span>
          </div>
          <div>
            <p className="text-sm text-gray-800">{activity.text}</p>
            <p className="text-xs text-gray-500">{activity.time}</p>
          </div>
        </li>
      ))}
    </ul>
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

const DashboardPage = () => {
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedCard, setSelectedCard] = useState(null);
  const [currentView, setCurrentView] = useState('dashboard');
  const [analysisData, setAnalysisData] = useState(null);

  useEffect(() => {
    const styleElement = document.createElement('style');
    styleElement.innerHTML = scrollbarStyles;

    document.head.appendChild(styleElement);

    return () => {
      document.head.removeChild(styleElement);
    };
  }, []);

  const handleCardClick = (cardType) => {
    setSelectedCard(cardType);
    setIsModalOpen(true);
  };

  const handleFilesUploaded = (files, cardType) => {
    setAnalysisData({ files, cardType });
    setCurrentView('analysis');
  };

  const handleBackToDashboard = () => {
    setCurrentView('dashboard');
    setAnalysisData(null);
  };

  if (currentView === 'analysis' && analysisData) {
    return <KPIAnalysisPage files={analysisData.files} cardType={analysisData.cardType} onBack={handleBackToDashboard} />;
  }

  return (
    <div className="bg-slate-100 font-sans h-screen overflow-y-auto custom-scrollbar">
      <main className="max-w-screen-xl mx-auto p-4 md:p-8 pt-24">
        <div className="flex justify-between items-center mb-6">
          <div>
            <h1 className="text-3xl font-bold text-gray-800">Dashboard</h1>
            <p className="text-gray-500">Welcome back, here's a summary of your activities.</p>
          </div>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
          <StatCard
            title="Total Users"
            value="26.3K"
            change="+14.5%"
            icon={Users}
            chartData={salesData}
            chartType="line"
          />
          <StatCard
            title="Total Income"
            value="$8,450"
            change="+42.8%"
            icon={DollarSign}
            chartData={salesData}
            chartType="bar"
          />
          <StatCard
            title="Total Orders"
            value="1,287"
            change="-10.2%"
            icon={ShoppingCart}
            chartData={salesData.slice().reverse()}
            chartType="line"
          />
          <StatCard
            title="Conversion Rate"
            value="2.8%"
            change="+5.6%"
            icon={Activity}
            chartData={salesData}
            chartType="bar"
          />
        </div>

        <div className="mb-8">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">KPI Reconciliation & Analysis</h2>
          <p className="text-gray-600 mb-6">Click on any card below to upload datasets and perform KPI analysis</p>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
            {cardTypes.map((cardType) => (
              <KPICard key={cardType.id} cardType={cardType} onClick={handleCardClick} />
            ))}
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8 mb-8">
          <div className="lg:col-span-2">
            <SalesSummaryChart />
          </div>
          <div>
            <ActivityFeed />
          </div>
        </div>

        <div>
          <ProjectsTable />
        </div>
      </main>

      <FileUploadModal
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        selectedCard={selectedCard}
        onFilesUploaded={handleFilesUploaded}
      />
    </div>
  );
};

export default DashboardPage;
