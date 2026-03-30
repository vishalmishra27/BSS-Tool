import { useState, useEffect } from 'react';
import { FileText, ArrowLeft, Copy, Download, Loader } from 'lucide-react';

const UserAnalyticsPage = ({ files, onBack }) => {
  const [isProcessing, setIsProcessing] = useState(false);
  const [analysisResults, setAnalysisResults] = useState(null);
  const [selectedFile, setSelectedFile] = useState(null);

  useEffect(() => {
    if (files && files.length > 0) {
      setSelectedFile(files[0]);
      processOCR(files[0]);
    }
  }, [files]);

  const processOCR = async (file) => {
    setIsProcessing(true);
    try {
      const formData = new FormData();
      formData.append('file', file.file);

      const response = await fetch('/api/user-analytics/ocr', {
        method: 'POST',
        body: formData
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const result = await response.json();
      setAnalysisResults(result);
    } catch (error) {
      console.error('OCR processing error:', error);
      setAnalysisResults({
        success: false,
        error: error.message,
        extracted_text: '',
        pages: 0
      });
    } finally {
      setIsProcessing(false);
    }
  };

  const handleFileSelect = (file) => {
    setSelectedFile(file);
    processOCR(file);
  };

  const copyToClipboard = () => {
    if (analysisResults?.extracted_text) {
      navigator.clipboard.writeText(analysisResults.extracted_text);
      alert('Text copied to clipboard!');
    }
  };

  const downloadText = () => {
    if (analysisResults?.extracted_text) {
      const blob = new Blob([analysisResults.extracted_text], { type: 'text/plain' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${selectedFile.name.replace('.pdf', '')}_extracted.txt`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }
  };

  const formatFileSize = (bytes) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  return (
    <div className="bg-slate-100 font-sans h-screen overflow-y-auto">
      <main className="max-w-screen-xl mx-auto p-4 md:p-8 pt-24">
        <div className="flex justify-between items-center mb-6">
          <div>
            <h1 className="text-3xl font-bold text-gray-800 flex items-center">
              <FileText className="mr-3" size={32} />
              User Analytics - PDF OCR Analysis
            </h1>
            <p className="text-gray-500">Extract text from uploaded PDF using OCR</p>
          </div>
          <button
            onClick={onBack}
            className="px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors flex items-center"
          >
            <ArrowLeft size={16} className="mr-2" />
            Back to Dashboard
          </button>
        </div>

        {/* File Selection */}
        {files && files.length > 1 && (
          <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200 mb-6">
            <h2 className="text-xl font-semibold text-gray-800 mb-4">Select PDF File</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {files.map((file) => (
                <div
                  key={file.id}
                  onClick={() => handleFileSelect(file)}
                  className={`p-4 border-2 rounded-lg cursor-pointer transition-colors ${
                    selectedFile?.id === file.id
                      ? 'border-blue-500 bg-blue-50'
                      : 'border-gray-200 hover:border-gray-300'
                  }`}
                >
                  <div className="flex items-center space-x-3">
                    <FileText size={24} className="text-blue-600" />
                    <div>
                      <p className="font-medium text-gray-800">{file.name}</p>
                      <p className="text-sm text-gray-500">{formatFileSize(file.size)}</p>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Processing Status */}
        {isProcessing && (
          <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200 mb-6 text-center">
            <div className="flex items-center justify-center space-x-2 mb-4">
              <Loader className="animate-spin" size={24} />
              <span className="text-lg font-medium text-gray-700">Processing PDF with OCR...</span>
            </div>
            <p className="text-gray-500">This may take a few moments depending on the PDF size and content.</p>
          </div>
        )}

        {/* Analysis Results */}
        {analysisResults && !isProcessing && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Left Side: PDF Viewer */}
            <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
              <h2 className="text-xl font-semibold text-gray-800 mb-4">PDF Document</h2>
              <div className="h-[600px] border border-gray-200 rounded-lg overflow-hidden">
                {selectedFile && (
                  <iframe
                    src={`/api/uploads/${selectedFile.name}`}
                    width="100%"
                    height="100%"
                    className="border-0"
                    title="PDF Viewer"
                  />
                )}
              </div>
              <div className="mt-4 text-sm text-gray-600">
                <p>
                  <strong>File:</strong> {selectedFile?.name}
                </p>
                <p>
                  <strong>Size:</strong> {formatFileSize(selectedFile?.size)}
                </p>
                <p>
                  <strong>Pages:</strong> {analysisResults.pages || 'Unknown'}
                </p>
              </div>
            </div>

            {/* Right Side: Extracted Text */}
            <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
              <div className="flex justify-between items-center mb-4">
                <h2 className="text-xl font-semibold text-gray-800">Extracted Text</h2>
                <div className="flex space-x-2">
                  <button
                    onClick={copyToClipboard}
                    className="px-3 py-1 bg-blue-600 text-white rounded hover:bg-blue-700 transition-colors flex items-center text-sm"
                    disabled={!analysisResults.extracted_text}
                  >
                    <Copy size={14} className="mr-1" />
                    Copy
                  </button>
                  <button
                    onClick={downloadText}
                    className="px-3 py-1 bg-green-600 text-white rounded hover:bg-green-700 transition-colors flex items-center text-sm"
                    disabled={!analysisResults.extracted_text}
                  >
                    <Download size={14} className="mr-1" />
                    Download
                  </button>
                </div>
              </div>

              <div className="h-[600px] border border-gray-200 rounded-lg p-4 overflow-y-auto bg-gray-50">
                {analysisResults.success ? (
                  <pre className="whitespace-pre-wrap text-sm text-gray-800 font-mono">
                    {analysisResults.extracted_text || 'No text extracted from this PDF.'}
                  </pre>
                ) : (
                  <div className="text-center text-red-600">
                    <p className="font-medium">OCR Processing Failed</p>
                    <p className="text-sm mt-2">{analysisResults.error}</p>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
};

export default UserAnalyticsPage;
