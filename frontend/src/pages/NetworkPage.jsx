import React, { useState, useRef } from 'react';
import {
  Upload,
  FileText,
  Trash2,
  Download,
  Play,
  RotateCcw,
  Network,
  CheckCircle,
  AlertCircle,
  Table2,
  FolderUp,
} from 'lucide-react';

const NetworkPage = () => {
  const [files, setFiles] = useState([]);
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [isDragging, setIsDragging] = useState(false);
  const fileInputRef = useRef(null);

  const handleFiles = (selectedFiles) => {
    const fileArray = Array.from(selectedFiles);
    setFiles((prev) => [...prev, ...fileArray]);
    setError('');
  };

  const removeFile = (index) => {
    setFiles((prev) => prev.filter((_, i) => i !== index));
  };

  const formatFileSize = (bytes) => {
    if (!bytes) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setIsDragging(false);
    if (e.dataTransfer.files.length) {
      handleFiles(e.dataTransfer.files);
    }
  };

  const handleUploadAndProcess = async () => {
    if (!files.length) {
      setError('Please select at least one config file to upload.');
      return;
    }
    setLoading(true);
    setError('');
    setResults(null);

    try {
      const formData = new FormData();
      files.forEach((f) => formData.append('files', f));

      const res = await fetch('/api/network/process', {
        method: 'POST',
        body: formData,
      });

      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.error || `Server error ${res.status}`);
      }

      const data = await res.json();
      if (data.success) {
        setResults(data.results);
      } else {
        throw new Error(data.error || 'Processing failed');
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleDownloadCSV = async (result) => {
    try {
      const res = await fetch('/api/network/download-csv', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          headers: result.headers,
          rows: result.rows,
          filename: result.filename.replace(/\.[^.]+$/, '') + '.csv',
        }),
      });

      if (!res.ok) throw new Error('Download failed');

      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = result.filename.replace(/\.[^.]+$/, '') + '.csv';
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
    } catch (err) {
      setError('Failed to download CSV: ' + err.message);
    }
  };

  const handleReset = () => {
    setFiles([]);
    setResults(null);
    setError('');
    if (fileInputRef.current) fileInputRef.current.value = '';
  };

  const totalRecords = results ? results.reduce((a, r) => a + r.row_count, 0) : 0;
  const totalTags = results ? results.reduce((a, r) => a + r.headers.length, 0) : 0;

  return (
    <div className="bg-slate-100 font-sans min-h-screen overflow-y-auto">
      <main className="max-w-screen-xl mx-auto p-4 md:p-8 pt-8">

        {/* Header */}
        <div className="flex justify-between items-center mb-6">
          <div>
            <h1 className="text-3xl font-bold text-gray-800">Network Configuration Parser</h1>
            <p className="text-gray-500">Enterprise Assurance — Upload config files to extract and analyze tag-value data</p>
          </div>
          {(files.length > 0 || results) && (
            <button
              onClick={handleReset}
              className="px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors flex items-center space-x-2"
            >
              <RotateCcw size={16} />
              <span>Reset</span>
            </button>
          )}
        </div>

        {/* Summary stat cards (shown after processing) */}
        {results && (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
            <div className="bg-gradient-to-br from-[#00338D] to-[#00338d] p-6 rounded-lg text-white shadow-lg">
              <p className="text-sm uppercase opacity-80">Files Processed</p>
              <p className="text-3xl font-bold">{results.length}</p>
              <p className="text-xs mt-1 text-green-300">+{results.length} completed</p>
            </div>
            <div className="bg-gradient-to-br from-[#00338D] to-[#00338d] p-6 rounded-lg text-white shadow-lg">
              <p className="text-sm uppercase opacity-80">Total Records</p>
              <p className="text-3xl font-bold">{totalRecords.toLocaleString()}</p>
              <p className="text-xs mt-1 text-green-300">Extracted successfully</p>
            </div>
            <div className="bg-gradient-to-br from-[#00338D] to-[#00338d] p-6 rounded-lg text-white shadow-lg">
              <p className="text-sm uppercase opacity-80">Tags Identified</p>
              <p className="text-3xl font-bold">{totalTags}</p>
              <p className="text-xs mt-1 text-green-300">Unique configuration tags</p>
            </div>
            <div className="bg-gradient-to-br from-[#00338D] to-[#00338d] p-6 rounded-lg text-white shadow-lg">
              <div className="flex items-center space-x-2">
                <CheckCircle size={20} className="text-green-300" />
                <p className="text-sm uppercase opacity-80">Status</p>
              </div>
              <p className="text-3xl font-bold">Done</p>
              <p className="text-xs mt-1 text-green-300">Processing complete</p>
            </div>
          </div>
        )}

        {/* Upload Card */}
        {!results && (
          <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200 mb-6">
            <h2 className="text-xl font-semibold text-gray-800 mb-4">Upload Configuration Files</h2>

            {/* Drag & Drop Zone */}
            <div
              className={`border-2 border-dashed rounded-lg p-8 text-center mb-6 transition-colors ${
                isDragging ? 'border-blue-500 bg-blue-50' : 'border-gray-300 hover:border-gray-400'
              }`}
              onDrop={handleDrop}
              onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
              onDragLeave={(e) => { e.preventDefault(); setIsDragging(false); }}
            >
              <Upload size={48} className="mx-auto text-gray-400 mb-4" />
              <p className="text-lg font-medium text-gray-700 mb-2">Drag and drop files here, or click to select</p>
              <p className="text-sm text-gray-500 mb-4">Supports .txt, .cfg, .conf, .log files with ! delimited sections</p>
              <input
                ref={fileInputRef}
                type="file"
                multiple
                accept=".txt,.cfg,.conf,.log"
                onChange={(e) => handleFiles(e.target.files)}
                className="hidden"
                id="network-file-upload"
              />
              <label
                htmlFor="network-file-upload"
                className="bg-blue-600 text-white px-6 py-2 rounded-lg cursor-pointer hover:bg-blue-700 transition-colors"
              >
                Select Files
              </label>
            </div>

            {/* Selected Files List */}
            {files.length > 0 && (
              <div className="mb-6">
                <h3 className="text-lg font-semibold text-gray-800 mb-3">
                  Uploaded Files ({files.length})
                </h3>
                <div className="space-y-2 max-h-60 overflow-y-auto">
                  {files.map((file, i) => (
                    <div key={i} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                      <div className="flex items-center space-x-3">
                        <FileText size={20} className="text-blue-600" />
                        <div>
                          <p className="font-medium text-gray-800">{file.name}</p>
                          <p className="text-sm text-gray-500">{formatFileSize(file.size)}</p>
                        </div>
                      </div>
                      <button onClick={() => removeFile(i)} className="text-red-500 hover:text-red-700">
                        <Trash2 size={16} />
                      </button>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Error */}
            {error && (
              <div className="flex items-center space-x-2 p-4 mb-6 rounded-lg border-l-4 border-l-red-500 bg-red-50">
                <AlertCircle size={18} className="text-red-600" />
                <span className="text-red-800 text-sm">{error}</span>
              </div>
            )}

            {/* Action Buttons */}
            <div className="flex justify-end space-x-4">
              {files.length > 0 && (
                <button
                  onClick={handleReset}
                  className="px-6 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors"
                >
                  Cancel
                </button>
              )}
              <button
                onClick={handleUploadAndProcess}
                disabled={loading || !files.length}
                className="px-8 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors flex items-center space-x-2"
              >
                {loading ? (
                  <>
                    <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-white"></div>
                    <span>Processing...</span>
                  </>
                ) : (
                  <>
                    <Play size={20} />
                    <span>Upload & Process</span>
                  </>
                )}
              </button>
            </div>
          </div>
        )}

        {/* Results Section */}
        {results && results.map((result, idx) => (
          <div key={idx} className="bg-white p-6 rounded-lg shadow-md border border-gray-200 mb-6">
            {/* Result Header */}
            <div className="flex justify-between items-center mb-4">
              <div className="flex items-center space-x-3">
                <div className="p-2 bg-blue-100 rounded-lg">
                  <Table2 size={20} className="text-blue-600" />
                </div>
                <div>
                  <h3 className="text-lg font-semibold text-gray-800">{result.filename}</h3>
                  <p className="text-sm text-gray-500">
                    {result.row_count} records extracted across {result.headers.length} tags
                  </p>
                </div>
              </div>
              <button
                onClick={() => handleDownloadCSV(result)}
                className="px-5 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors flex items-center space-x-2"
              >
                <Download size={16} />
                <span>Download CSV</span>
              </button>
            </div>

            {/* Data Table */}
            {result.headers.length > 0 && result.rows.length > 0 ? (
              <div className="overflow-x-auto rounded-lg border border-gray-200">
                <table className="w-full text-sm text-left text-gray-600">
                  <thead className="text-xs text-white uppercase bg-[#00338d]">
                    <tr>
                      <th className="px-4 py-3 font-semibold sticky left-0 bg-[#00338d] z-10">#</th>
                      {result.headers.map((h, i) => (
                        <th key={i} className="px-4 py-3 font-semibold whitespace-nowrap">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {result.rows.map((row, ri) => (
                      <tr key={ri} className={`border-b ${ri % 2 === 0 ? 'bg-white' : 'bg-gray-50'} hover:bg-blue-50 transition-colors`}>
                        <td className={`px-4 py-3 font-medium text-gray-400 sticky left-0 z-10 ${ri % 2 === 0 ? 'bg-white' : 'bg-gray-50'}`}>
                          {ri + 1}
                        </td>
                        {result.headers.map((h, ci) => (
                          <td key={ci} className="px-4 py-3 whitespace-nowrap max-w-xs overflow-hidden text-ellipsis">
                            {row[h] || <span className="text-gray-300">—</span>}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="text-center py-8 text-gray-400">
                <AlertCircle size={32} className="mx-auto mb-2" />
                <p>No data extracted. Please check the file format.</p>
              </div>
            )}
          </div>
        ))}

        {/* Error in results view */}
        {results && error && (
          <div className="flex items-center space-x-2 p-4 mb-6 rounded-lg border-l-4 border-l-red-500 bg-red-50">
            <AlertCircle size={18} className="text-red-600" />
            <span className="text-red-800 text-sm">{error}</span>
          </div>
        )}

      </main>
    </div>
  );
};

export default NetworkPage;
