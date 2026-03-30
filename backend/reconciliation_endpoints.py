from flask import Blueprint, request, jsonify
from reconciliation_service import ReconciliationService

reconciliation_bp = Blueprint('reconciliation', __name__)
reconciliation_service = ReconciliationService()

@reconciliation_bp.route('/upload-cbs-clm-files', methods=['POST'])
def upload_files():
    """Upload CBS and CLM files"""
    return jsonify({"message": "File upload endpoint ready"}), 200

@reconciliation_bp.route('/load-reconciliation-data', methods=['POST'])
def load_data():
    """Load uploaded files into database"""
    return jsonify({"message": "Data loading endpoint ready"}), 200