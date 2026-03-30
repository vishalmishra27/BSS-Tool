import React from 'react';
import { FileText, ArrowLeft } from 'lucide-react';

const FileAnalysisPage = ({ onBack }) => {
  return (
    <div className="bg-slate-100 font-sans h-screen overflow-y-auto">
      <main className="max-w-screen-xl mx-auto p-4 md:p-8 pt-24">
        <div className="flex justify-between items-center mb-6">
          <div>
            <h1 className="text-3xl font-bold text-gray-800 flex items-center">
              <FileText className="mr-3" size={32} />
              File Analysis Results
            </h1>
            <p className="text-gray-500">Data quality and reconciliation analysis complete</p>
          </div>
          <button
            onClick={onBack}
            className="px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors flex items-center"
          >
            <ArrowLeft size={16} className="mr-2" />
            Back to Dashboard
          </button>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
            <h2 className="text-xl font-semibold text-gray-800 mb-6">Analysis Summary</h2>
            <div className="space-y-6">
              <div>
                <div className="flex justify-between mb-2">
                  <span className="text-gray-600">Records Processed</span>
                  <span className="font-bold text-2xl text-blue-600">12,847</span>
                </div>
                <div className="w-full bg-gray-200 rounded-full h-2">
                  <div className="bg-blue-600 h-2 rounded-full" style={{width: '100%'}}></div>
                </div>
              </div>
              <div>
                <div className="flex justify-between mb-2">
                  <span className="text-gray-600">Data Quality Score</span>
                  <span className="font-bold text-2xl text-green-600">92.4%</span>
                </div>
                <div className="w-full bg-gray-200 rounded-full h-2">
                  <div className="bg-green-600 h-2 rounded-full" style={{width: '92%'}}></div>
                </div>
              </div>
              <div>
                <div className="flex justify-between mb-2">
                  <span className="text-gray-600">Reconciliation Rate</span>
                  <span className="font-bold text-2xl text-purple-600">87.1%</span>
                </div>
                <div className="w-full bg-gray-200 rounded-full h-2">
                  <div className="bg-purple-600 h-2 rounded-full" style={{width: '87%'}}></div>
                </div>
              </div>
            </div>
          </div>

          <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
            <h2 className="text-xl font-semibold text-gray-800 mb-6">Key Issues Found</h2>
            <div className="space-y-4">
              <div className="flex items-center p-3 bg-yellow-50 rounded-lg border-l-4 border-yellow-400">
                <div className="flex-shrink-0">
                  <div className="w-2 h-2 bg-yellow-400 rounded-full mx-2"></div>
                </div>
                <div>
                  <p className="font-medium text-yellow-800">Missing values in 4 columns</p>
                  <p className="text-sm text-yellow-700">187 records affected</p>
                </div>
              </div>
              <div className="flex items-center p-3 bg-red-50 rounded-lg border-l-4 border-red-400">
                <div className="flex-shrink-0">
                  <div className="w-2 h-2 bg-red-400 rounded-full mx-2"></div>
                </div>
                <div>
                  <p className="font-medium text-red-800">Data type mismatch</p>
                  <p className="text-sm text-red-700">42 records affected</p>
                </div>
              </div>
              <div className="flex items-center p-3 bg-blue-50 rounded-lg border-l-4 border-blue-400">
                <div className="flex-shrink-0">
                  <div className="w-2 h-2 bg-blue-400 rounded-full mx-2"></div>
                </div>
                <div>
                  <p className="font-medium text-blue-800">Duplicates removed</p>
                  <p className="text-sm text-blue-700">156 records cleaned</p>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className="mt-8 bg-white p-8 rounded-xl shadow-lg border border-gray-200 text-center">
          <h2 className="text-2xl font-bold text-gray-800 mb-4">Analysis Complete ✅</h2>
          <p className="text-gray-600 mb-6 max-w-2xl mx-auto">
            Your files have been successfully processed. Review the summary above and download the detailed report.
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <button className="px-8 py-3 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors font-semibold">
              Download Report
            </button>
            <button className="px-8 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors font-semibold">
              View Full Analysis
            </button>
          </div>
        </div>
      </main>
    </div>
  );
};

export default FileAnalysisPage;

