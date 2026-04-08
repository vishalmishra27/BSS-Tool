"""
Pydantic models for API request/response shapes.
"""

from typing import Any, Dict, List, Optional
from pydantic import BaseModel


# -- Requests --

class SendMessageRequest(BaseModel):
    content: str
    documents: Optional[List[Dict[str, Any]]] = None  # Uploaded doc metadata from Node.js


class RenameChatRequest(BaseModel):
    title: str


# -- Responses --

class StarterAction(BaseModel):
    label: str
    prompt: str


class StarterActionsResponse(BaseModel):
    actions: List[StarterAction]

class ChatResponse(BaseModel):
    id: str
    title: str
    created_at: str
    updated_at: str
    is_archived: bool = False
    projectId: Optional[str] = None


class ChatListResponse(BaseModel):
    chats: List[ChatResponse]


class MessageResponse(BaseModel):
    id: str
    role: str
    content: Optional[str] = None
    tool_name: Optional[str] = None
    tool_args: Optional[List[Dict[str, Any]]] = None
    tool_call_id: Optional[str] = None
    created_at: Optional[str] = None


class MessagesResponse(BaseModel):
    messages: List[MessageResponse]
    facts: List[str] = []


class HealthResponse(BaseModel):
    status: str
    active_states: int


# -- User models --

class CreateUserRequest(BaseModel):
    email: str
    name: str
    role: str = "auditor"  # "admin" | "auditor" | "viewer"
    metadata: Optional[Dict[str, Any]] = None


class UpdateUserRequest(BaseModel):
    email: Optional[str] = None
    name: Optional[str] = None
    role: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class UserResponse(BaseModel):
    id: str
    email: str
    name: str
    role: str
    metadata: Optional[Dict[str, Any]] = None
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None


# -- Project models --

class CreateProjectRequest(BaseModel):
    name: str
    createdBy: str
    description: Optional[str] = None
    status: str = "active"  # "active" | "archived" | "completed"
    metadata: Optional[Dict[str, Any]] = None


class UpdateProjectRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class ProjectResponse(BaseModel):
    id: str
    createdBy: str
    name: str
    description: Optional[str] = None
    status: str = "active"
    metadata: Optional[Dict[str, Any]] = None
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None


# -- Document models --

class CreateDocumentRequest(BaseModel):
    projectId: str
    name: str
    blobPath: str
    blobUrl: Optional[str] = None
    type: Optional[str] = None
    size: Optional[int] = None
    uploadedBy: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class UpdateDocumentRequest(BaseModel):
    name: Optional[str] = None
    blobPath: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class DocumentResponse(BaseModel):
    id: str
    projectId: str
    name: str
    blobPath: str
    blobUrl: Optional[str] = None
    type: Optional[str] = None
    size: Optional[int] = None
    uploadedBy: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None


# -- Embedding models --

class CreateEmbeddingRequest(BaseModel):
    projectId: str
    text: str
    embedding: List[float]
    source: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class VectorSearchRequest(BaseModel):
    projectId: str
    queryVector: List[float]
    topK: int = 10
