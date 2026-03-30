import { useState, useRef, useEffect } from 'react';
import { Plus, Minus, RotateCcw, SkipForward, ChevronDown, RefreshCw } from 'lucide-react';
import { Popover, Box, Typography, Divider } from '@mui/material';

const Flowchart = () => {
  const [nodes, setNodes] = useState([]);
  const [connections, setConnections] = useState([
    { from: 1, to: 2 },
    { from: 2, to: 3 }
  ]);
  const [currentNodeIndex, setCurrentNodeIndex] = useState(0);
  const [draggedNode, setDraggedNode] = useState(null);
  const [dragOffset, setDragOffset] = useState({ x: 0, y: 0 });
  const [hoveredNode, setHoveredNode] = useState(null);
  const [isConnecting, setIsConnecting] = useState(false);
  const [connectionStart, setConnectionStart] = useState(null);
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 });
  const [isDraggingConnection, setIsDraggingConnection] = useState(false);
  const [connectionDragStart, setConnectionDragStart] = useState(null);
  const containerRef = useRef(null);
  const [newNodeShape, setNewNodeShape] = useState('rectangle');
  const [isLegendOpen, setIsLegendOpen] = useState(true);
  const [anchorEl, setAnchorEl] = useState(null);
  const [selectedNode, setSelectedNode] = useState(null);

  useEffect(() => {
    const workflowItems = localStorage.getItem('workflowCheckedItems');
    if (workflowItems) {
      try {
        const checkedItems = JSON.parse(workflowItems);
        if (checkedItems.length > 0) {
          loadWorkflowItems(checkedItems);
          localStorage.removeItem('workflowCheckedItems');
        }
      } catch (error) {
        console.error('Error loading workflow items:', error);
      }
    }
  }, []);

  useEffect(() => {
    const fetchFlowchartData = async () => {
      try {
        const workflowId = 'wf1';
        const stageId = 'stg1';
        const response = await fetch(`/api/workflow_nodes/${workflowId}/${stageId}`);
        if (!response.ok) {
          throw new Error('Failed to fetch flowchart nodes');
        }
        const nodesData = await response.json();

        const transformedNodes = nodesData.map((node, index) => ({
          id: node.id,
          x: 400 + index * 250,
          y: 300,
          label: node.name,
          status: 'pending',
          type: 'sequential',
          shape: 'rectangle',
          isFromWorkflow: true
        }));

        const transformedConnections = [];
        for (let i = 0; i < transformedNodes.length - 1; i++) {
          transformedConnections.push({ from: transformedNodes[i].id, to: transformedNodes[i + 1].id });
        }
        setNodes(transformedNodes);
        setConnections(transformedConnections);
      } catch (error) {
        console.error('Error fetching flowchart nodes:', error);
      }
    };
    fetchFlowchartData();
  }, []);

  const loadWorkflowItems = (checkedItems) => {
    const containerRect = containerRef.current?.getBoundingClientRect();
    const containerWidth = containerRect ? containerRect.width : window.innerWidth;
    const containerHeight = containerRect ? containerRect.height : window.innerHeight;

    const controlPanelWidth = 320;
    const controlPanelHeight = 200;
    const availableWidth = containerWidth - controlPanelWidth - 100;
    const availableHeight = containerHeight - controlPanelHeight - 100;

    const startX = controlPanelWidth + 50;
    const startY = controlPanelHeight + 50;
    const centerX = startX + availableWidth / 2;
    const centerY = startY + availableHeight / 2;

    const newNodes = checkedItems.map((item, index) => {
      const nodesPerRow = Math.ceil(Math.sqrt(checkedItems.length));
      const row = Math.floor(index / nodesPerRow);
      const col = index % nodesPerRow;

      const nodeSpacingX = 180;
      const nodeSpacingY = 100;
      const totalGridWidth = (nodesPerRow - 1) * nodeSpacingX;
      const totalGridHeight = (Math.ceil(checkedItems.length / nodesPerRow) - 1) * nodeSpacingY;

      const x = centerX - totalGridWidth / 2 + col * nodeSpacingX;
      const y = centerY - totalGridHeight / 2 + row * nodeSpacingY;

      return {
        id: Date.now() + index,
        x: Math.max(startX, Math.min(x, containerWidth - 150)),
        y: Math.max(startY, Math.min(y, containerHeight - 150)),
        label: item.text.length > 20 ? item.text.substring(0, 20) + '...' : item.text,
        fullText: item.text,
        parentNode: item.parentNode,
        status: 'current',
        type: 'sequential',
        shape: 'rectangle',
        isFromWorkflow: true
      };
    });

    const newConnections = [];
    for (let i = 0; i < newNodes.length - 1; i++) {
      newConnections.push({
        from: newNodes[i].id,
        to: newNodes[i + 1].id
      });
    }
    setNodes(newNodes);
    setConnections(newConnections);
    setCurrentNodeIndex(0);
  };

  const resetToDefault = () => {
    setNodes([
      { id: 1, x: 400, y: 300, label: 'Level 1', status: 'current', type: 'sequential', shape: 'rectangle' },
      { id: 2, x: 650, y: 300, label: 'Level 2', status: 'pending', type: 'sequential', shape: 'rectangle' },
      { id: 3, x: 900, y: 300, label: 'Level 3', status: 'pending', type: 'sequential', shape: 'rectangle' }
    ]);
    setConnections([
      { from: 1, to: 2 },
      { from: 2, to: 3 }
    ]);
    setCurrentNodeIndex(0);
  };

  const getNodeColor = (status, type) => {
    if (status === 'completed') return 'bg-green-500 hover:bg-green-600';
    if (status === 'current') return 'bg-yellow-500 hover:bg-yellow-600';
    if (type === 'parallel') {
      return status === 'current' ? 'bg-purple-500 hover:bg-purple-600' : 'bg-purple-400 hover:bg-purple-500';
    }
    return 'bg-[#4178d9] hover:bg-[#3059a0]';
  };

  const handleNodeClickForPopup = (event, node) => {
    if (!isConnecting && !event.ctrlKey && !event.metaKey && !event.altKey) {
      setAnchorEl(event.currentTarget);
      setSelectedNode(node);
    }
  };

  const handleClosePopup = () => {
    setAnchorEl(null);
    setSelectedNode(null);
  };

  const isPopupOpen = Boolean(anchorEl);
  const popupId = isPopupOpen ? 'node-details-popover' : undefined;

  const updateNodeLabels = (nodeList) =>
    nodeList.map((node, index) => ({
      ...node,
      label: node.isFromWorkflow ? node.label : `Level ${index + 1}`
    }));

  const addNode = () => {
    // Intentionally left blank to disable adding nodes from frontend
  };

  const deleteNode = (nodeId) => {
    if (nodes.length <= 1) return;
    const nodeIndex = nodes.findIndex((node) => node.id === nodeId);
    const newNodes = nodes.filter((node) => node.id !== nodeId);
    const newConnections = connections.filter((conn) => conn.from !== nodeId && conn.to !== nodeId);

    setConnections(newConnections);
    if (nodeIndex <= currentNodeIndex && currentNodeIndex > 0) {
      setCurrentNodeIndex(currentNodeIndex - 1);
    } else if (currentNodeIndex >= newNodes.length) {
      setCurrentNodeIndex(newNodes.length - 1);
    }
    const updatedNodes = updateNodeLabels(newNodes).map((node, index) => ({
      ...node,
      status: index < currentNodeIndex ? 'completed' : index === currentNodeIndex ? 'current' : 'pending'
    }));
    setNodes(updatedNodes);
  };

  const toggleNodeType = (nodeId) => {
    setNodes((prevNodes) =>
      prevNodes.map((node) =>
        node.id === nodeId
          ? { ...node, type: node.type === 'sequential' ? 'parallel' : 'sequential' }
          : node
      )
    );
  };

  const nextNode = () => {
    if (currentNodeIndex < nodes.length - 1) {
      const newIndex = currentNodeIndex + 1;
      setCurrentNodeIndex(newIndex);
      setNodes((prevNodes) =>
        prevNodes.map((node, index) => ({
          ...node,
          status: index < newIndex ? 'completed' : index === newIndex ? 'current' : 'pending'
        }))
      );
    }
  };

  const resetFlow = () => {
    setCurrentNodeIndex(0);
    setNodes((prevNodes) =>
      prevNodes.map((node, index) => ({
        ...node,
        status: index === 0 ? 'current' : 'pending'
      }))
    );
  };

  const handleCombinedNodeClick = (e, node) => {
    if (isConnecting || e.ctrlKey || e.metaKey) {
      if (connectionStart && connectionStart !== node.id) {
        const newConnection = { from: connectionStart, to: node.id };
        const connectionExists = connections.some(
          (conn) =>
            (conn.from === newConnection.from && conn.to === newConnection.to) ||
            (conn.from === newConnection.to && conn.to === newConnection.from)
        );
        if (!connectionExists) setConnections((prev) => [...prev, newConnection]);

        setConnectionStart(null);
        if (isConnecting) setIsConnecting(false);
      } else {
        setConnectionStart(node.id);
      }
      return;
    }
    if (!e.target.closest('.delete-btn') && !e.target.closest('.type-btn')) {
      handleNodeClickForPopup(e, node);
    }
  };

  const handleMouseDown = (e, node) => {
    if (e.target.closest('.delete-btn') || e.target.closest('.type-btn')) return;
    if (e.altKey) {
      setIsDraggingConnection(true);
      setConnectionDragStart(node.id);
      return;
    }
    if (!isConnecting && !e.ctrlKey && !e.metaKey) {
      const rect = containerRef.current.getBoundingClientRect();
      setDraggedNode(node.id);
      setDragOffset({ x: e.clientX - rect.left - node.x, y: e.clientY - rect.top - node.y });
    }
  };

  const handleMouseMove = (e) => {
    const rect = containerRef.current?.getBoundingClientRect();
    if (rect) setMousePos({ x: e.clientX - rect.left, y: e.clientY - rect.top });
    if (!draggedNode || isDraggingConnection) return;
    const nodeWidth = nodeDimensions[nodes.find((n) => n.id === draggedNode).shape].width;
    const nodeHeight = nodeDimensions[nodes.find((n) => n.id === draggedNode).shape].height;
    const newX = Math.max(25, Math.min(rect.width - nodeWidth, e.clientX - rect.left - dragOffset.x));
    const newY = Math.max(25, Math.min(rect.height - nodeHeight, e.clientY - rect.top - dragOffset.y));
    setNodes((prevNodes) =>
      prevNodes.map((node) => (node.id === draggedNode ? { ...node, x: newX, y: newY } : node))
    );
  };

  const handleMouseUp = (e) => {
    if (isDraggingConnection) {
      const targetElement = document.elementFromPoint(e.clientX, e.clientY);
      const nodeElement = targetElement?.closest('[data-node-id]');
      if (nodeElement) {
        const targetNodeId = parseInt(nodeElement.getAttribute('data-node-id'));
        if (targetNodeId && targetNodeId !== connectionDragStart) {
          const newConnection = { from: connectionDragStart, to: targetNodeId };
          const connectionExists = connections.some(
            (conn) =>
              (conn.from === newConnection.from && conn.to === newConnection.to) ||
              (conn.from === newConnection.to && conn.to === newConnection.from)
          );
          if (!connectionExists) setConnections((prev) => [...prev, newConnection]);
        }
      }
      setIsDraggingConnection(false);
      setConnectionDragStart(null);
    }
    setDraggedNode(null);
    setDragOffset({ x: 0, y: 0 });
  };

  useEffect(() => {
    if (draggedNode) {
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('mouseup', handleMouseUp);
      return () => {
        document.removeEventListener('mousemove', handleMouseMove);
        document.removeEventListener('mouseup', handleMouseUp);
      };
    }
  }, [draggedNode, dragOffset]);

  const getNodeCenter = (node) => {
    if (!node) return { x: 0, y: 0 };
    const dims = nodeDimensions[node.shape];
    return { x: node.x + dims.width / 2, y: node.y + dims.height / 2 };
  };

  const renderConnections = () => {
    const connectionLines = connections.map((conn, index) => {
      const fromNode = nodes.find((n) => n.id === conn.from);
      const toNode = nodes.find((n) => n.id === conn.to);
      if (!fromNode || !toNode) return null;
      const fromCenter = getNodeCenter(fromNode);
      const toCenter = getNodeCenter(toNode);
      return (
        <line
          key={`connection-${index}`}
          x1={fromCenter.x}
          y1={fromCenter.y}
          x2={toCenter.x}
          y2={toCenter.y}
          stroke="#4178d9"
          strokeWidth="2"
          markerEnd="url(#arrowhead)"
        />
      );
    });
    if ((isConnecting && connectionStart) || (isDraggingConnection && connectionDragStart)) {
      const startNodeId = connectionStart || connectionDragStart;
      const startNode = nodes.find((n) => n.id === startNodeId);
      if (startNode) {
        const startCenter = getNodeCenter(startNode);
        connectionLines.push(
          <line
            key="temp-connection"
            x1={startCenter.x}
            y1={startCenter.y}
            x2={mousePos.x}
            y2={mousePos.y}
            stroke="#004fd9"
            strokeWidth="2"
            strokeDasharray="5,5"
          />
        );
      }
    }
    return connectionLines;
  };

  const nodeDimensions = {
    rectangle: { width: 96, height: 48, class: 'w-24 h-12' },
    square: { width: 80, height: 80, class: 'w-20 h-20' },
    circle: { width: 80, height: 80, class: 'w-20 h-20' }
  };

  const shapeStyles = {
    rectangle: 'rounded-lg',
    square: 'rounded-none',
    circle: 'rounded-full'
  };

  const popupDetails = [
    { label: 'Assignee', value: 'john doe', color: '#b3cde0' },
    { label: 'Reporter', value: 'jane smith', color: '#b3cde0' },
    { label: 'Requirement Type', value: 'Enhancement' },
    { label: 'Reference SRS', value: 'Prepaid Voice SRS' },
    { label: 'Phase Detected', value: 'Integration Testing' }
  ];

  return (
    <div className="h-screen w-full bg-gradient-to-br from-[#00215a] to-[#3059a0] overflow-hidden relative z-0">
      <div className="absolute top-4 left-4 z-20 bg-[#00215a]/50 backdrop-blur-md rounded-lg p-4 shadow-xl">
        <div className="flex flex-wrap gap-3 mb-4">
          <button
            onClick={addNode}
            className="flex items-center gap-2 px-4 py-2 bg-[#004fd9] hover:bg-[#4178d9] text-white rounded-lg transition-colors"
          >
            <Plus size={16} />
            Add Node
          </button>
          <button
            onClick={() => {
              setIsConnecting(!isConnecting);
              setConnectionStart(null);
            }}
            className={`flex items-center gap-2 px-4 py-2 ${
              isConnecting ? 'bg-[#4178d9]' : 'bg-[#00338d] hover:bg-[#004fd9]'
            } text-white rounded-lg transition-colors`}
          >
            🔗 {isConnecting ? 'Cancel' : 'Connect'}
          </button>
          <button
            onClick={nextNode}
            disabled={currentNodeIndex >= nodes.length - 1}
            className="flex items-center gap-2 px-4 py-2 bg-[#004fd9] hover:bg-[#4178d9] disabled:bg-[#3059a0] text-white rounded-lg transition-colors"
          >
            <SkipForward size={16} />
            Next
          </button>
          <button
            onClick={resetFlow}
            className="flex items-center gap-2 px-4 py-2 bg-[#3059a0] hover:bg-[#4178d9] text-white rounded-lg transition-colors"
          >
            <RotateCcw size={16} />
            Reset
          </button>
          <button
            onClick={resetToDefault}
            className="flex items-center gap-2 px-4 py-2 bg-red-600 hover:bg-red-700 text-white rounded-lg transition-colors"
          >
            <RefreshCw size={16} />
            Reset to Default
          </button>
        </div>
        <div className="mb-4">
          <label className="text-white mr-2 text-sm">Node Shape:</label>
          <select
            value={newNodeShape}
            onChange={(e) => setNewNodeShape(e.target.value)}
            className="rounded p-1 bg-[#00215a] text-white border-none focus:ring-2 focus:ring-[#4178d9]"
          >
            <option value="rectangle">Rectangle</option>
            <option value="square">Square</option>
            <option value="circle">Circle</option>
          </select>
        </div>
        <div className="text-white/80 text-sm">
          <p>Current: {nodes[currentNodeIndex]?.label}</p>
          <p>Progress: {currentNodeIndex + 1}/{nodes.length}</p>
          {isConnecting && <p className="text-blue-300">Click nodes to connect</p>}
        </div>
      </div>

      <div className="absolute top-4 right-4 z-20 bg-[#00215a]/50 backdrop-blur-md rounded-lg p-4 shadow-xl">
        <div
          className="flex justify-between items-center cursor-pointer"
          onClick={() => setIsLegendOpen(!isLegendOpen)}
        >
          <h3 className="text-white font-semibold">Legend & Controls</h3>
          <ChevronDown
            size={20}
            className={`text-white transition-transform duration-300 ${isLegendOpen ? 'rotate-180' : ''}`}
          />
        </div>
        <div className={`overflow-hidden transition-all duration-300 ${isLegendOpen ? 'max-h-96 mt-4' : 'max-h-0'}`}>
          <div className="space-y-2 text-sm mb-4">
            <div className="text-white/90 font-medium">Controls:</div>
            <div className="text-white/70 text-xs">• Click node for details</div>
            <div className="text-white/70 text-xs">• Ctrl+Click: Connect nodes</div>
            <div className="text-white/70 text-xs">• Alt+Drag: Connect nodes</div>
          </div>

          <div className="space-y-2 text-sm">
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 bg-green-500 rounded"></div>
              <span className="text-white/80">Completed</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 bg-yellow-500 rounded"></div>
              <span className="text-white/80">Current</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 bg-[#4178d9] rounded"></div>
              <span className="text-white/80">Pending</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-4 h-4 bg-purple-500 rounded"></div>
              <span className="text-white/80">Parallel</span>
            </div>
          </div>
        </div>
      </div>

      <div
        ref={containerRef}
        className="w-full h-full relative"
        style={{ userSelect: 'none' }}
        onMouseMove={handleMouseMove}
      >
        <svg className="absolute inset-0 w-full h-full pointer-events-none">
          <defs>
            <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="9" refY="3.5" orient="auto">
              <polygon points="0 0, 10 3.5, 0 7" fill="#4178d9" />
            </marker>
          </defs>
          {renderConnections()}
        </svg>

        {nodes.map((node, index) => (
          <div
            key={node.id}
            aria-describedby={popupId}
            className={`absolute cursor-move transition-all duration-300 transform hover:scale-105 shadow-lg text-white ${getNodeColor(
              node.status,
              node.type
            )} ${nodeDimensions[node.shape].class} ${shapeStyles[node.shape]} ${
              hoveredNode === node.id ? 'ring-4 ring-white/30' : ''
            } ${isConnecting || connectionStart === node.id ? 'cursor-pointer' : 'cursor-move'} ${
              connectionStart === node.id ? 'ring-4 ring-yellow-400' : ''
            } ${isDraggingConnection ? 'cursor-crosshair' : ''} ${
              node.isFromWorkflow ? 'ring-2 ring-green-400' : ''
            }`}
            style={{
              left: Math.max(0, Math.min(node.x, window.innerWidth - nodeDimensions[node.shape].width)),
              top: Math.max(0, Math.min(node.y, window.innerHeight - nodeDimensions[node.shape].height)),
              zIndex: draggedNode === node.id ? 1000 : 10
            }}
            data-node-id={node.id}
            onMouseDown={(e) => handleMouseDown(e, node)}
            onClick={(e) => handleCombinedNodeClick(e, node)}
            onMouseEnter={() => setHoveredNode(node.id)}
            onMouseLeave={() => setHoveredNode(null)}
          >
            <div className="w-full h-full flex flex-col items-center justify-center font-semibold text-sm relative">
              <div className="text-center px-1">
                {node.isFromWorkflow ? (
                  <div className="text-xs leading-tight">{node.label}</div>
                ) : (
                  <>
                    <div className="text-xl font-bold">{index + 1}</div>
                    <div className="text-xs">{node.label}</div>
                  </>
                )}
              </div>
              <button
                className="type-btn absolute -top-2 -left-2 w-6 h-6 bg-[#00338d] hover:bg-[#004fd9] rounded-full flex items-center justify-center text-white text-xs transition-colors"
                onClick={(e) => {
                  e.stopPropagation();
                  toggleNodeType(node.id);
                }}
                title="Toggle parallel/sequential"
              >
                {node.type === 'parallel' ? '||' : '→'}
              </button>
              {nodes.length > 1 && (
                <button
                  className="delete-btn absolute -top-2 -right-2 w-6 h-6 bg-red-600 hover:bg-red-700 rounded-full flex items-center justify-center text-white text-xs transition-colors"
                  onClick={(e) => {
                    e.stopPropagation();
                    deleteNode(node.id);
                  }}
                >
                  <Minus size={12} />
                </button>
              )}
            </div>
          </div>
        ))}

        <Popover
          id={popupId}
          open={isPopupOpen}
          anchorEl={anchorEl}
          onClose={handleClosePopup}
          anchorOrigin={{
            vertical: 'bottom',
            horizontal: 'center'
          }}
          transformOrigin={{
            vertical: 'top',
            horizontal: 'center'
          }}
          PaperProps={{
            sx: {
              borderRadius: 2,
              backdropFilter: 'blur(10px)',
              background: 'linear-gradient(to bottom, rgba(0, 51, 141, 0.8), rgba(0, 33, 90, 0.8))',
              boxShadow: '0 8px 32px 0 rgba(0, 0, 0, 0.1)'
            }
          }}
        >
          <Box sx={{ p: 2, minWidth: 280, color: 'white' }}>
            <Typography
              variant="h6"
              sx={{
                mb: 1.5,
                fontSize: '1rem',
                fontWeight: '600',
                borderBottom: '1px solid rgba(255,255,255,0.2)',
                pb: 1
              }}
            >
              {selectedNode?.isFromWorkflow ? 'Workflow Item Details' : 'Node Details'}
            </Typography>

            {popupDetails.map((item, index) => (
              <Box key={item.label}>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', py: 1.5 }}>
                  <Typography sx={{ fontSize: '0.8rem', color: 'rgba(255,255,255,0.7)' }}>
                    {item.label}
                  </Typography>
                  <Typography sx={{ fontSize: '0.8rem', fontWeight: '500', color: item.color || 'inherit' }}>
                    {item.value}
                  </Typography>
                </Box>
                {index < popupDetails.length - 1 && (
                  <Divider sx={{ bgcolor: 'rgba(255, 255, 255, 0.1)' }} />
                )}
              </Box>
            ))}

            {selectedNode?.isFromWorkflow && (
              <>
                <Divider sx={{ bgcolor: 'rgba(255, 255, 255, 0.1)' }} />
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', py: 1.5 }}>
                  <Typography sx={{ fontSize: '0.8rem', color: 'rgba(255,255,255,0.7)' }}>
                    Full Text
                  </Typography>
                  <Typography sx={{ fontSize: '0.8rem', fontWeight: '500', color: '#b3cde0' }}>
                    {selectedNode.fullText}
                  </Typography>
                </Box>
                <Divider sx={{ bgcolor: 'rgba(255, 255, 255, 0.1)' }} />
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', py: 1.5 }}>
                  <Typography sx={{ fontSize: '0.8rem', color: 'rgba(255,255,255,0.7)' }}>
                    Source Stage
                  </Typography>
                  <Typography sx={{ fontSize: '0.8rem', fontWeight: '500', color: '#b3cde0' }}>
                    {selectedNode.parentNode}
                  </Typography>
                </Box>
              </>
            )}
          </Box>
        </Popover>
      </div>
    </div>
  );
};

export default Flowchart;
