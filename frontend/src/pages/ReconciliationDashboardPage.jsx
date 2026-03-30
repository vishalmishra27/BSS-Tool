import { useState } from 'react';
import { X, Upload, FileText, Trash2 } from 'lucide-react';

const ReconciliationDashboardPage = () => {
  const [selectedCard, setSelectedCard] = useState(null);
  const [uploadedFiles, setUploadedFiles] = useState([]);

  const handleCardClick = (card) => {
    setSelectedCard(card);
  };

  const handleFileUpload = (files) => {
    if (files) {
      setUploadedFiles([...uploadedFiles, ...Array.from(files).map(f => ({ id: Date.now() + Math.random(), name: f.name, size: f.size }))]);
    }
  };

  const removeFile = (id) => {
    setUploadedFiles(uploadedFiles.filter(f => f.id !== id));
  };

  const formatFileSize = (bytes) => {
    if (!bytes) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB'];
    const i = Math.floor(Math.log(bytes, k));
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
  };

  if (!selectedCard) {
    return (
      <div className="p-8 bg-gradient-to-br from-blue-50 to-indigo-50 min-h-screen">
        <h1 className="text-3xl font-bold text-blue-900 mb-6">Reconciliation Dashboard</h1>
        <p className="text-gray-600 mb-8">Select a data reconciliation option below:</p>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <button
            onClick={() => handleCardClick({ title: 'CBS vs CLM', color: 'orange' })}
            className="p-6 bg-white rounded-lg shadow-lg hover:shadow-xl transition transform hover:scale-105"
          >
            <h3 className="font-bold text-lg mb-2">CBS vs CLM</h3>
            <p className="text-sm text-gray-600">Billing system reconciliation</p>
          </button>
          <button
            onClick={() => handleCardClick({ title: 'Inventory', color: 'purple' })}
            className="p-6 bg-white rounded-lg shadow-lg hover:shadow-xl transition transform hover:scale-105"
          >
            <h3 className="font-bold text-lg mb-2">Inventory</h3>
            <p className="text-sm text-gray-600">Product inventory audit</p>
          </button>
          <button
            onClick={() => handleCardClick({ title: 'Network', color: 'blue' })}
            className="p-6 bg-white rounded-lg shadow-lg hover:shadow-xl transition transform hover:scale-105"
          >
            <h3 className="font-bold text-lg mb-2">Network</h3>
            <p className="text-sm text-gray-600">Network data reconciliation</p>
          </button>
          <button
            onClick={() => handleCardClick({ title: 'Contracts', color: 'green' })}
            className="p-6 bg-white rounded-lg shadow-lg hover:shadow-xl transition transform hover:scale-105"
          >
            <h3 className="font-bold text-lg mb-2">Contracts</h3>
            <p className="text-sm text-gray-600">Contract OCR analysis</p>
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="p-8 bg-gray-50 min-h-screen">
      <div className="bg-white rounded-lg shadow-lg p-6">
        <div className="flex justify-between items-center mb-6">
          <h2 className="text-2xl font-bold">{selectedCard.title} Reconciliation</h2>
          <button
            onClick={() => setSelectedCard(null)}
            className="text-gray-500 hover:text-gray-700"
          >
            <X size={24} />
          </button>
        </div>

        <div className="mb-6">
          <label htmlFor="file-upload" className="block font-semibold mb-4">Upload Files for {selectedCard.title}:</label>
          <div className="border-2 border-dashed border-gray-300 rounded-lg p-8 text-center">
            <Upload className="mx-auto mb-4 text-gray-400" size={48} />
            <p className="text-gray-600 mb-4">Drag and drop files here or click to select</p>
            <input
              id="file-upload"
              type="file"
              multiple
              accept=".csv,.xlsx,.json,.txt"
              onChange={(e) => handleFileUpload(e.target.files)}
              className="hidden"
            />
            <label
              htmlFor="file-upload"
              className="bg-blue-600 text-white px-6 py-2 rounded-lg cursor-pointer hover:bg-blue-700"
            >
              Select Files
            </label>
          </div>
        </div>

        {uploadedFiles.length > 0 && (
          <div className="mb-6">
            <h3 className="text-lg font-semibold text-gray-800 mb-3">Uploaded Files ({uploadedFiles.length})</h3>
            <div className="space-y-2">
              {uploadedFiles.map((file) => (
                <div key={file.id} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                  <div className="flex items-center space-x-3">
                    <FileText size={20} className="text-blue-600" />
                    <div>
                      <p className="font-medium text-gray-800">{file.name}</p>
                      <p className="text-sm text-gray-500">{formatFileSize(file.size)}</p>
                    </div>
                  </div>
                  <button
                    onClick={() => removeFile(file.id)}
                    className="text-red-500 hover:text-red-700"
                  >
                    <Trash2 size={16} />
                  </button>
                </div>
              ))}
            </div>
          </div>
        )}

        <div className="flex gap-4 justify-end">
          <button
            onClick={() => setSelectedCard(null)}
            className="px-6 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50"
          >
            Back
          </button>
          <button className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700">
            Analyze
          </button>
        </div>
      </div>
    </div>
  );
};

export default ReconciliationDashboardPage;
