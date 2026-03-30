import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { CheckCircle, Circle, ArrowRight, GitBranch } from 'lucide-react';

const initialNodes = [];

const workflowConnections = [
  { from: 1, to: 2 }, { from: 2, to: 3 }, { from: 3, to: 4 }, { from: 4, to: 5 }
];

// Helper function to calculate phase start and end dates from checklist items
const calculatePhaseDates = (checklistItems) => {
  if (!checklistItems || checklistItems.length === 0) {
    return { startDate: null, endDate: null };
  }
  let minStartDate = null;
  let maxEndDate = null;

  checklistItems.forEach(item => {
    if (item.start_date) {
      const startDate = new Date(item.start_date);
      if (!isNaN(startDate.getTime()) && (!minStartDate || startDate < minStartDate)) {
        minStartDate = startDate;
      }
    }
    if (item.end_date) {
      const endDate = new Date(item.end_date);
      if (!isNaN(endDate.getTime()) && (!maxEndDate || endDate > maxEndDate)) {
        maxEndDate = endDate;
      }
    }
  });

  return {
    startDate: minStartDate ? minStartDate.toISOString().split('T')[0] : null,
    endDate: maxEndDate ? maxEndDate.toISOString().split('T')[0] : null
  };
};

const scrollbarStyles = `
/* Main page Y scrollbar */
.workflow-main::-webkit-scrollbar { width: 8px; }
.workflow-main::-webkit-scrollbar-track { background: rgba(255, 255, 255, 0.1); border-radius: 10px; }
.workflow-main::-webkit-scrollbar-thumb { background: linear-gradient(45deg, #4178d9, #3059a0); border-radius: 10px; border: 1px solid rgba(255, 255, 255, 0.2); }
.workflow-main::-webkit-scrollbar-thumb:hover { background: linear-gradient(45deg, #3059a0, #00338d); }

/* Horizontal scrollbar */
.workflow-nodes-container::-webkit-scrollbar { height: 6px; }
.workflow-nodes-container::-webkit-scrollbar-track { background: rgba(255, 255, 255, 0.1); border-radius: 8px; margin: 0 20px; }
.workflow-nodes-container::-webkit-scrollbar-thumb { background: linear-gradient(90deg, #60a5fa, #3b82f6); border-radius: 8px; border: 1px solid rgba(255, 255, 255, 0.3); }
.workflow-nodes-container::-webkit-scrollbar-thumb:hover { background: linear-gradient(90deg, #3b82f6, #2563eb); }

/* Checklist scrollbar */
.checklist-container::-webkit-scrollbar { width: 4px; }
.checklist-container::-webkit-scrollbar-track { background: #f1f5f9; border-radius: 6px; }
.checklist-container::-webkit-scrollbar-thumb { background: linear-gradient(180deg, #64748b, #475569); border-radius: 6px; }
.checklist-container::-webkit-scrollbar-thumb:hover { background: linear-gradient(180deg, #475569, #334155); }

/* Firefox scrollbar support */
.workflow-main { scrollbar-width: thin; scrollbar-color: #4178d9 rgba(255, 255, 255, 0.1); }
.workflow-nodes-container { scrollbar-width: thin; scrollbar-color: #60a5fa rgba(255, 255, 255, 0.1); }
`;

const WorkflowPage = () => {
  const navigate = useNavigate();
  const [nodes, setNodes] = useState(initialNodes);
  const [selectedNode, setSelectedNode] = useState(null);
  const [phaseDetails, setPhaseDetails] = useState([]);
  const [filter, setFilter] = useState('pre');
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const styleElement = document.createElement('style');
    styleElement.innerHTML = scrollbarStyles;
    document.head.appendChild(styleElement);
    return () => {
      if (document.head.contains(styleElement)) {
        document.head.removeChild(styleElement);
      }
    };
  }, []);

  // Fetch subworkflow checklist from backend API on component mount
  useEffect(() => {
    const fetchSubworkflowChecklist = async () => {
      try {
        setLoading(true);
        const response = await fetch(`/api/workflow_nodes`);
        if (!response.ok) {
          throw new Error('Failed to fetch workflow nodes');
        }
        const nodesData = await response.json();
        console.log('Fetched workflow nodes:', nodesData);

        const mappedNodes = nodesData.map(node => ({
          id: parseInt(node.id) || node.id,
          label: node.name || 'Unnamed Node',
          status: node.status || 'pending',
          checklist: node.parameters ? node.parameters.map((param, index) => ({
            id: `${node.id}-${index}`,
            text: `${param.key}: ${param.value}`
          })) : []
        }));

        const updateNodeStatuses = async (nodes) => {
          const updatedNodes = [];
          for (const node of nodes) {
            const updatedNode = { ...node };
            try {
              const response = await fetch(`/api/checklist/${updatedNode.id}`);
              if (response.ok) {
                const checklistItems = await response.json();
                if (checklistItems && checklistItems.length > 0) {
                  const allCompleted = checklistItems.every(item => item.status === 'completed');
                  const anyCompleted = checklistItems.some(item => item.status === 'completed');

                  if (allCompleted) {
                    updatedNode.status = 'completed';
                  } else if (anyCompleted) {
                    updatedNode.status = 'current';
                  } else {
                    updatedNode.status = 'pending';
                  }
                }
              }
            } catch (error) {
              console.error('Error fetching checklist items:', updatedNode.id, error);
            }
            updatedNodes.push(updatedNode);
          }
          return updatedNodes;
        };

        updateNodeStatuses(mappedNodes).then(updatedNodes => {
          setNodes(updatedNodes);
        });
      } catch (error) {
        console.error('Error fetching workflow nodes:', error);
        setNodes([
          { id: 1, label: 'Initiation & Planning', status: 'completed', checklist: [] },
          { id: 2, label: 'Requirements Analysis', status: 'current', checklist: [] },
          { id: 3, label: 'Design & Architecture', status: 'pending', checklist: [] },
          { id: 4, label: 'Development', status: 'pending', checklist: [] },
          { id: 5, label: 'Testing & QA', status: 'pending', checklist: [] }
        ]);
      } finally {
        setLoading(false);
      }
    };
    fetchSubworkflowChecklist();
  }, []);

  useEffect(() => {
    if (selectedNode) {
      handleNodeClick(selectedNode);
    }
  }, []);

  const getFilteredNodes = () => {
    if (filter === 'all') return nodes;
    switch (filter) {
      case 'pre':
        return nodes.slice(0, 6);
      case 'migration':
        return nodes.slice(6, 8);
      case 'post':
        return nodes.slice(-1);
      default:
        return nodes;
    }
  };

  const filteredNodes = getFilteredNodes();

  const handleNodeClick = (node) => {
    console.log('handleNodeClick called with node id:', node.id);

    if (selectedNode?.id === node.id) {
      setSelectedNode(null);
      setPhaseDetails([]);
      return;
    }

    setSelectedNode({ ...node, checklist: [] });
    setPhaseDetails([]);

    if (node) {
      const mappedPhaseDetails = [];
      mappedPhaseDetails.push({ label: 'Phase id', value: node.id });
      mappedPhaseDetails.push({ label: 'Phase Name', value: node.label });

      fetch(`/api/phase/${node.id}`)
        .then(response => {
          if (!response.ok) throw new Error('Failed to fetch phase details');
          return response.json();
        })
        .then(phaseData => {
          console.log('Fetched phase data:', phaseData);
          if (phaseData && phaseData.assignee) {
            mappedPhaseDetails.push({ label: 'Assigned To', value: phaseData.assignee });
          } else if (phaseData && phaseData.assigned_to) {
            mappedPhaseDetails.push({ label: 'Assigned To', value: phaseData.assigned_to });
          }
          if (phaseData && phaseData.start) {
            mappedPhaseDetails.push({ label: 'Start Date', value: phaseData.start });
          }
          if (phaseData && phaseData['End date']) {
            mappedPhaseDetails.push({ label: 'End Date', value: phaseData['End date'] });
          }
        })
        .catch(error => {
          console.error('Error fetching phase details:', error);
        })
        .finally(() => {
          if (node.parameters && node.parameters.length > 0) {
            node.parameters.forEach(param => {
              if (param.key === 'assigned_to' || param.key === 'assignee') return;
              let label = param.key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
              if (param.value && param.value.trim() !== '') {
                mappedPhaseDetails.push({ label: label, value: param.value });
              }
            });
          }

          const statusColorMap = {
            pending: 'bg-orange-400 text-white',
            overdue: 'bg-red-600 text-white',
            completed: 'bg-green-600 text-white'
          };
          const statusValue = node.status || '';
          const statusColorClass = statusColorMap[statusValue.toLowerCase()] || 'bg-gray-400 text-white';
          mappedPhaseDetails.push({
            label: 'Status',
            value: statusValue,
            colorClass: statusColorClass,
            textColorClass: statusColorClass.includes('text-') ? statusColorClass.split(' ').find(c => c.startsWith('text-')) : 'text-white'
          });

          setPhaseDetails(mappedPhaseDetails);
        });

      fetch(`/api/checklist/${node.id}`)
        .then(response => {
          if (!response.ok) throw new Error('Failed to fetch checklist items');
          return response.json();
        })
        .then(checklistItems => {
          const phaseDates = calculatePhaseDates(checklistItems);

          setPhaseDetails(prevDetails => {
            const updatedDetails = [...prevDetails];
            const startDateExists = updatedDetails.some(item => item.label === 'Start Date');
            if (!startDateExists) {
              updatedDetails.splice(2, 0, { label: 'Start Date', value: phaseDates.startDate || '—' });
            }
            const endDateExists = updatedDetails.some(item => item.label === 'End Date');
            if (!endDateExists) {
              updatedDetails.splice(3, 0, { label: 'End Date', value: phaseDates.endDate || '—' });
            }
            return updatedDetails;
          });

          const checklist = checklistItems.map((item, index) => ({
            id: `${node.id}-${index}`,
            text: item.item_text,
            status: item.status || 'pending',
            ch_id: item.ch_id
          }));

          let updatedStatus = node.status;
          if (checklistItems && checklistItems.length > 0) {
            const allCompleted = checklistItems.every(item => item.status === 'completed');
            const anyCompleted = checklistItems.some(item => item.status === 'completed');
            if (allCompleted) {
              updatedStatus = 'completed';
            } else if (anyCompleted) {
              updatedStatus = 'current';
            } else {
              updatedStatus = 'pending';
            }
          }

          setSelectedNode(prev => ({ ...prev, checklist, status: updatedStatus }));
          setNodes(prevNodes => prevNodes.map(n => n.id === node.id ? { ...n, status: updatedStatus } : n));
        })
        .catch(error => {
          console.error('Error fetching checklist items:', error);
          setSelectedNode(prev => ({ ...prev, checklist: [] }));
        });
    } else {
      setPhaseDetails([]);
    }
  };

  const handleSubmitAndProceed = () => {
    if (!selectedNode) return;
    const allCompleted = selectedNode.checklist.every(item => item.status === 'completed');
    if (!allCompleted) {
      alert('Please complete all checklist items before proceeding.');
      return;
    }
    const fromNodeId = selectedNode.id;
    const connection = workflowConnections.find(c => c.from === fromNodeId);

    if (connection) {
      const toNodeId = connection.to;
      setNodes(prevNodes => prevNodes.map(node => {
        if (node.id === fromNodeId) return { ...node, status: 'completed' };
        if (node.id === toNodeId) return { ...node, status: 'current' };
        return node;
      }));
    } else {
      setNodes(prevNodes => prevNodes.map(node => node.id === fromNodeId ? { ...node, status: 'completed' } : node));
    }
    setSelectedNode(null);
  };

  const getNodeColor = (status) => {
    if (status === 'completed') return 'bg-gradient-to-r from-green-500 to-green-600';
    if (status === 'current') return 'bg-gradient-to-r from-yellow-500 to-yellow-600';
    return 'bg-gradient-to-r from-gray-400 to-gray-500 text-gray-800';
  };

  if (loading) {
    return (
      <div className="h-screen w-full bg-gradient-to-br from-[#00215a] via-[#1e3a8a] to-[#3059a0] overflow-y-auto workflow-main flex items-center justify-center">
        <div className="text-center">
          <div className="text-white text-2xl font-bold mb-4">Loading project milestones...</div>
          <div className="text-white/80">Please wait while we fetch the latest data</div>
        </div>
      </div>
    );
  }

  return (
    <div className="h-screen w-full bg-gradient-to-br from-[#00215a] via-[#1e3a8a] to-[#3059a0] overflow-y-auto workflow-main">
      <div className="container mx-auto px-6 py-8">
        <div className="mb-8">
          <div className="flex justify-between items-center mb-6">
            <div>
              <h1 className="text-4xl font-bold text-white mb-3 tracking-tight">Project Milestones</h1>
              <p className="text-white/80 text-lg">Click a stage to view its details and checklist</p>
            </div>
            <div className="relative">
              <select
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                className="bg-white/10 backdrop-blur-lg text-white border border-white/20 rounded-lg py-2 px-4 pr-8 focus:outline-none focus:ring-2 focus:ring-blue-500 appearance-none cursor-pointer">
                <option value="pre" className="bg-[#00215a] text-white">Pre-Migration</option>
                <option value="migration" className="bg-[#00215a] text-white">Migration</option>
                <option value="post" className="bg-[#00215a] text-white">Post-Migration</option>
                <option value="all" className="bg-[#00215a] text-white">All Phases</option>
              </select>
              <div className="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-white">
                <svg className="fill-current h-4 w-4" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20">
                  <path d="M9.293 12.95l.707.707L15.657 8l-1.414-1.414L10 10.828 5.757 6.586 4.343 8z"/>
                </svg>
              </div>
            </div>
          </div>
        </div>

        <div className="mb-10">
          <div className="workflow-nodes-container overflow-x-auto pb-4">
            <div className="flex items-center justify-center gap-6 min-w-max px-6">
              {filteredNodes.map((node, index) => (
                <div key={node.id}>
                  <div
                    className={`relative w-32 h-16 rounded-lg flex items-center justify-center transition-all duration-300 transform cursor-pointer hover:scale-105 ${selectedNode?.id === node.id ? 'bg-white text-gray-900 font-bold' : getNodeColor(node.status) + ' text-white font-bold'} flex-shrink-0 shadow-md`}
                    onClick={() => handleNodeClick(node)}>
                    <div className="text-center px-2 py-1 w-full">
                      <p className="text-xs leading-snug font-semibold truncate px-1">{node.label}</p>
                      <div className="mt-1 flex justify-center">
                        <span className={`inline-block w-1.5 h-1.5 rounded-full ${
                          selectedNode?.id === node.id ? 'bg-green-500' : node.status === 'completed' ? 'bg-white'
                            : node.status === 'current' ? 'bg-white animate-pulse' : 'bg-white/50'
                        }`}></span>
                      </div>
                    </div>
                    <div className={`absolute -top-1 -right-1 w-5 h-5 rounded-full flex items-center justify-center text-xs font-bold ${
                      node.status === 'completed' ? 'bg-green-600 text-white' : node.status === 'current' ? 'bg-yellow-600 text-white' : 'bg-gray-600 text-white'
                    }`}>
                      {node.status === 'completed' ? '✓' : node.status === 'current' ? '●' : index + 1}
                    </div>
                  </div>
                  {index < filteredNodes.length - 1 && (
                    <div className="flex items-center justify-center">
                      <div className="w-8 h-1 bg-gradient-to-r from-[#4178d9] to-[#60a5fa] relative flex-shrink-0 rounded-full">
                        <ArrowRight size={16} className="text-white absolute right-[-8px] top-1/2 -translate-y-1/2" />
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className={`transition-all duration-700 ease-in-out ${selectedNode ? 'max-h-[2000px] opacity-100 transform translate-y-0' : 'max-h-0 opacity-0 transform -translate-y-4'} overflow-hidden`}>
          {selectedNode && (
            <div className="max-w-7xl mx-auto mb-8">
              <div className="text-center mb-8">
                <h2 className="text-2xl font-bold text-white mb-2">{selectedNode.label}</h2>
                <div className="w-24 h-1 bg-gradient-to-r from-blue-400 to-purple-500 mx-auto rounded-full"></div>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
                <div className="bg-white/95 backdrop-blur-lg rounded-2xl p-8 shadow-lg border border-white/20">
                  <div className="flex items-center mb-6">
                    <div className="w-8 h-8 bg-gradient-to-r from-blue-500 to-purple-600 rounded-lg flex items-center justify-center mr-3">
                      <span className="text-white text-sm font-bold">📋</span>
                    </div>
                    <h3 className="text-xl font-semibold text-gray-800">Phase Details</h3>
                  </div>

                  <div className="space-y-4">
                    {phaseDetails.length > 0 ? phaseDetails.map((item, index) => (
                      <div key={item.label || index} className="flex justify-between items-center p-3 rounded-lg bg-gray-50 hover:bg-gray-100 transition-colors">
                        <span className="text-gray-600 font-medium">{item.label}</span>
                        {item.label === 'Status' ? (
                          <span className={`font-semibold px-3 py-1 rounded-full ${item.colorClass || ''} ${item.textColorClass || 'text-white'}`}>
                            {item.value}
                          </span>
                        ) : (
                          <span className="font-semibold px-3 py-1 rounded-full bg-white">
                            {item.value}
                          </span>
                        )}
                      </div>
                    )) : (
                      <div className="text-gray-400 italic text-center p-3">No phase details available</div>
                    )}
                  </div>
                </div>

                <div className="bg-white/95 backdrop-blur-lg rounded-2xl p-8 shadow-lg border border-white/20 flex flex-col">
                  <div className="flex items-center mb-6">
                    <div className="w-8 h-8 bg-gradient-to-r from-green-500 to-blue-600 rounded-lg flex items-center justify-center mr-3">
                      <span className="text-white text-sm font-bold">✓</span>
                    </div>
                    <h3 className="text-xl font-semibold text-gray-800">{selectedNode.label}: Checklist</h3>
                  </div>

                  <div className="checklist-container space-y-2 overflow-y-auto max-h-80 flex-grow pr-2">
                    {selectedNode.checklist.map((item) => {
                      const isChecked = item.status === 'completed';
                      return (
                        <div
                          key={item.id}
                          className={`flex items-center space-x-4 p-4 rounded-xl transition-all duration-200 ${
                            isChecked ? 'bg-green-50 border-2 border-green-200' : 'bg-gray-50 border-2 border-transparent'
                          }`}>
                          <div className="flex-shrink-0">
                            {isChecked ? (
                              <CheckCircle className="text-green-500 w-6 h-6" />
                            ) : (
                              <Circle className="text-gray-300 w-6 h-6" />
                            )}
                          </div>
                          <span className={`text-sm font-medium transition-all ${
                            isChecked ? 'line-through text-green-600' : 'text-gray-800'
                          }`}>
                            {item.text}
                          </span>
                        </div>
                      );
                    })}
                  </div>

                  <div className="mt-6 pt-6 border-t border-gray-200">
                    <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
                      <div className="flex flex-col sm:flex-row gap-3">
                        <button
                          className="flex items-center gap-2 text-sm text-green-600 hover:text-green-700 hover:bg-green-50 px-4 py-2 rounded-lg transition-all duration-200 font-medium border border-green-200 hover:border-green-300"
                          onClick={() => navigate(`/workflow?phase=${selectedNode.id}`)}>
                          <GitBranch size={16} />
                          Go to Workflow
                        </button>
                      </div>
                      {selectedNode.status !== 'completed' ? (
                        <button
                          className="bg-gradient-to-r from-blue-600 to-blue-700 hover:from-blue-700 hover:to-blue-800 text-white px-6 py-3 rounded-lg text-sm font-semibold transition-all duration-200 shadow-md hover:shadow-lg transform hover:-translate-y-0.5 w-full sm:w-auto"
                          onClick={handleSubmitAndProceed}>
                          Submit & Proceed →
                        </button>
                      ) : (
                        <button
                          className="bg-gray-400 text-white px-6 py-3 rounded-lg text-sm font-semibold w-full sm:w-auto cursor-not-allowed"
                          disabled>
                          Phase Completed
                        </button>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default WorkflowPage;
