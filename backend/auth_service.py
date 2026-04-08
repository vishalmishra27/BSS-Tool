"""
Auth Service — JWT-based authentication with role-based access control.

Role hierarchy (descending privilege):
  programme_director  – full access, bulk ops, audit log, agent, all modules
  engagement_manager  – full access, bulk ops, agent, approve workflows
  bss_consultant      – read/write all modules, agent (single-row), workflow assign/comment/upload
  qa_manager          – UAT full access, defect mgmt, read other modules
  data_analyst        – reconciliation full access, read other modules
  client_sponsor      – read-only: dashboard + milestones + UAT summary
  client_it_lead      – read-only: dashboard + reconciliation + product journey
  client_operations   – UAT sign-off + product config read + milestone read
"""

import os
import jwt
import bcrypt
import datetime
from functools import wraps
from flask import request, jsonify

JWT_SECRET = os.getenv('JWT_SECRET', 'kpmg-bss-secret-change-in-prod')
JWT_ALGORITHM = 'HS256'
JWT_EXPIRY_HOURS = 72

# ─── Role Permission Matrix ────────────────────────────────────────────────────
ROLE_PERMISSIONS = {
    'programme_director': {
        'modules': ['dashboard', 'milestones', 'status', 'product', 'uat', 'reconciliation', 'pdf', 'workflow', 'audit_log'],
        'can_use_agent': True,
        'agent_bulk': True,
        'workflow_assign': True,
        'workflow_comment': True,
        'workflow_upload': True,
        'can_approve_phases': True,
        'read_only': False,
    },
    'engagement_manager': {
        'modules': ['dashboard', 'milestones', 'status', 'product', 'uat', 'reconciliation', 'pdf', 'workflow', 'audit_log'],
        'can_use_agent': True,
        'agent_bulk': True,
        'workflow_assign': True,
        'workflow_comment': True,
        'workflow_upload': True,
        'can_approve_phases': True,
        'read_only': False,
    },
    'bss_consultant': {
        'modules': ['dashboard', 'milestones', 'status', 'product', 'uat', 'reconciliation', 'pdf', 'workflow'],
        'can_use_agent': True,
        'agent_bulk': False,
        'workflow_assign': True,
        'workflow_comment': True,
        'workflow_upload': True,
        'can_approve_phases': False,
        'read_only': False,
    },
    'qa_manager': {
        'modules': ['dashboard', 'milestones', 'uat', 'product', 'reconciliation'],
        'can_use_agent': True,
        'agent_bulk': False,
        'workflow_assign': False,
        'workflow_comment': True,
        'workflow_upload': True,
        'can_approve_phases': False,
        'read_only': False,
    },
    'data_analyst': {
        'modules': ['dashboard', 'reconciliation', 'pdf', 'product', 'uat'],
        'can_use_agent': False,
        'agent_bulk': False,
        'workflow_assign': False,
        'workflow_comment': True,
        'workflow_upload': True,
        'can_approve_phases': False,
        'read_only': False,
    },
    'client_sponsor': {
        'modules': ['dashboard', 'milestones', 'uat'],
        'can_use_agent': False,
        'agent_bulk': False,
        'workflow_assign': False,
        'workflow_comment': False,
        'workflow_upload': False,
        'can_approve_phases': False,
        'read_only': True,
    },
    'client_it_lead': {
        'modules': ['dashboard', 'reconciliation', 'product'],
        'can_use_agent': False,
        'agent_bulk': False,
        'workflow_assign': False,
        'workflow_comment': False,
        'workflow_upload': False,
        'can_approve_phases': False,
        'read_only': True,
    },
    'client_operations': {
        'modules': ['dashboard', 'milestones', 'uat', 'product'],
        'can_use_agent': False,
        'agent_bulk': False,
        'workflow_assign': False,
        'workflow_comment': True,
        'workflow_upload': False,
        'can_approve_phases': False,
        'read_only': True,
    },
}


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def check_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())


def create_token(user: dict) -> str:
    payload = {
        'sub': str(user['id']),
        'username': user['username'],
        'full_name': user['full_name'],
        'role': user['role'],
        'organisation': user['organisation'],
        'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=JWT_EXPIRY_HOURS),
        'iat': datetime.datetime.utcnow(),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_token(token: str) -> dict:
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])


def get_permissions(role: str) -> dict:
    return ROLE_PERMISSIONS.get(role, ROLE_PERMISSIONS['client_sponsor'])


def require_auth(f):
    """Decorator: require a valid JWT. Injects `current_user` into kwargs."""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get('Authorization', '')
        if not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Authentication required'}), 401
        token = auth_header.split(' ', 1)[1]
        try:
            payload = decode_token(token)
            kwargs['current_user'] = payload
            return f(*args, **kwargs)
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Session expired. Please log in again.'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Invalid token'}), 401
    return decorated


def require_role(*allowed_roles):
    """Decorator: require auth + one of the allowed roles."""
    def decorator(f):
        @wraps(f)
        @require_auth
        def decorated(*args, current_user, **kwargs):
            if current_user['role'] not in allowed_roles:
                return jsonify({'error': 'Access denied — insufficient permissions'}), 403
            return f(*args, current_user=current_user, **kwargs)
        return decorated
    return decorator
