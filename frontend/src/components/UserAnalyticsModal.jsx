import { useState } from 'react';
import { FileText, X, Upload, CheckCircle } from 'lucide-react';

const UserAnalyticsModal = ({ isOpen, onClose, onProceedToAnalysis }) => {
  const [uploadedFiles, setUploadedFiles] = useState([]);

  const handleFileUpload = (files) => {
    const pdfFiles = Array.from(files).filter(file => file.type === 'application/pdf');
    const newFiles = pdfFiles.map(file => ({
      id: Date.now() + Math.random(),
      name: file.name,
      size: file.size,
      file: file
    }));
    setUploadedFiles(prev => [...prev, ...newFiles]);
  };

  const removeFile = (fileId) => {
    setUploadedFiles(prev => prev.filter(file => file.id !== fileId));
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
      onProceedToAnalysis(uploadedFiles);
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
            <h2 className="text-2xl font-bold text-gray-800">User Analytics PDF Upload</h2>
            <p className="text-gray-600">Upload PDF files for OCR text extraction</p>
          </div>
          <button onClick={onClose} className="text-gray-500 hover:text-gray-700">
            <X size={24} />
          </button>
        </div>

        <div className="border-2 border-dashed rounded-lg p-8 text-center mb-6 transition-colors hover:border-blue-400">
          <Upload size={48} className="mx-auto text-gray-400 mb-4" />
          <p className="text-lg font-medium text-gray-700 mb-2">
            Drag and drop PDF files here, or click to select
          </p>
          <p className="text-sm text-gray-500 mb-4">
            Supports PDF files only
          </p>
          <input
            type="file"
            multiple
            accept=".pdf"
            onChange={(e) => handleFileUpload(e.target.files)}
            className="hidden"
            id="user-analytics-upload"
          />
          <label
            htmlFor="user-analytics-upload"
            className="bg-blue-600 text-white px-6 py-2 rounded-lg cursor-pointer hover:bg-blue-700 transition-colors">
            Select PDFs
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
                  <button
                    onClick={() => removeFile(file.id)}
                    className="text-red-500 hover:text-red-700">
                    <X size={16} />
                  </button>
                </div>
              ))}
            </div>
          </div>
        )}

        <div className="flex justify-end space-x-4">
          <button
            onClick={onClose}
            className="px-6 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            Cancel
          </button>
          <button
            onClick={handleProceed}
            disabled={uploadedFiles.length === 0}
            className="px-6 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors flex items-center space-x-2">
            <CheckCircle size={16} />
            <span>Proceed to OCR Analysis</span>
          </button>
        </div>
      </div>
    </div>
  );
};

export default UserAnalyticsModal;
