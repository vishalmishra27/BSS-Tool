import React from 'react';
import { FileText, ArrowLeft } from 'lucide-react';

const PDFAnalysisPage = ({ files, onBack }) => {
  return (
    <div className="bg-slate-100 font-sans h-screen overflow-y-auto">
      <main className="max-w-screen-xl mx-auto p-4 md:p-8 pt-24">
        <div className="flex justify-between items-center mb-6">
          <div>
            <h1 className="text-3xl font-bold text-gray-800 flex items-center">
              <FileText className="mr-3" size={32} />
              PDF Contract Analysis
            </h1>
            <p className="text-gray-500">OCR text extraction and document analysis</p>
          </div>
          <button
            onClick={onBack}
            className="px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors flex items-center"
          >
            <ArrowLeft size={16} className="mr-2" />
            Back to Dashboard
          </button>
        </div>

        <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200 mb-6">
          <h2 className="text-xl font-semibold text-gray-800 mb-4">Uploaded Contracts</h2>
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

        <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200 text-center">
          <h2 className="text-xl font-semibold text-gray-800 mb-4">Ready for OCR Analysis</h2>
          <p className="text-gray-600 mb-6">
            PDF files have been uploaded successfully. The OCR analysis will extract text content and provide structured data for reconciliation.
          </p>
          <button className="px-8 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors flex items-center space-x-2 mx-auto">
            <span>Start OCR Processing</span>
          </button>
        </div>
      </main>
    </div>
  );
};

export default PDFAnalysisPage;

