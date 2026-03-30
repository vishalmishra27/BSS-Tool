import os
import pandas as pd
import sqlite3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReconciliationService:
    def __init__(self, db_path='reconciliation.db'):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Initialize database with required tables"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cbs_service (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customerinfo_servicecode TEXT,
                account_link_code_n TEXT,
                customerinfo_servicename TEXT,
                customerinfo_status TEXT,
                customerinfo_activationdate TEXT,
                customerinfo_deactivationdate TEXT,
                customerinfo_contracttype TEXT,
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS clm_service (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customerinfo_servicecode TEXT,
                account_link_code_n TEXT,
                customerinfo_servicename TEXT,
                customerinfo_status TEXT,
                customerinfo_activationdate TEXT,
                customerinfo_deactivationdate TEXT,
                customerinfo_contracttype TEXT,
                upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS reconciliation_kpi_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kpi_name TEXT,
                kpi_value INTEGER,
                kpi_percentage REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()

    def load_cbs_data(self, file_path):
        """Load CBS data from file"""
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                raise ValueError("Unsupported file format")
            conn = sqlite3.connect(self.db_path)
            df.to_sql('cbs_service', conn, if_exists='replace', index=False)
            conn.close()
            logger.info(f"Successfully loaded {len(df)} CBS records")
            return True, len(df)
        except Exception as e:
            logger.error(f"Error loading CBS data: {str(e)}")
            return False, 0

    def load_clm_data(self, file_path):
        """Load CLM data from file"""
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                raise ValueError("Unsupported file format")
            conn = sqlite3.connect(self.db_path)
            df.to_sql('clm_service', conn, if_exists='replace', index=False)
            conn.close()
            logger.info(f"Successfully loaded {len(df)} CLM records")
            return True, len(df)
        except Exception as e:
            logger.error(f"Error loading CLM data: {str(e)}")
            return False, 0

    def execute_reconciliation(self):
        """Execute the reconciliation SQL script"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM reconciliation_kpi_results")
            cursor.execute("SELECT COUNT(*) FROM cbs_service")
            total_cbs = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM clm_service")
            total_clm = cursor.fetchone()[0]
            queries = [
                ("Total Records in CBS", f"SELECT {total_cbs}"),
                ("Total Records in CLM", f"SELECT {total_clm}"),
                ("Matched Records", """
                    SELECT COUNT(*) FROM cbs_service cbs
                    INNER JOIN clm_service clm ON cbs.customerinfo_servicecode = clm.customerinfo_servicecode
                """),
                ("Records in CBS Only", """
                    SELECT COUNT(*) FROM cbs_service cbs
                    LEFT JOIN clm_service clm ON cbs.customerinfo_servicecode = clm.customerinfo_servicecode
                    WHERE clm.customerinfo_servicecode IS NULL
                """),
                ("Records in CLM Only", """
                    SELECT COUNT(*) FROM clm_service clm
                    LEFT JOIN cbs_service cbs ON cbs.customerinfo_servicecode = clm.customerinfo_servicecode
                    WHERE cbs.customerinfo_servicecode IS NULL
                """),
                ("Status Mismatches", """
                    SELECT COUNT(*) FROM cbs_service cbs
                    INNER JOIN clm_service clm ON cbs.customerinfo_servicecode = clm.customerinfo_servicecode
                    WHERE cbs.customerinfo_status != clm.customerinfo_status
                """),
                ("Contract Type Mismatches", """
                    SELECT COUNT(*) FROM cbs_service cbs
                    INNER JOIN clm_service clm ON cbs.customerinfo_servicecode = clm.customerinfo_servicecode
                    WHERE cbs.customerinfo_contracttype != clm.customerinfo_contracttype
                """),
                ("Activation Date Mismatches", """
                    SELECT COUNT(*) FROM cbs_service cbs
                    INNER JOIN clm_service clm ON cbs.customerinfo_servicecode = clm.customerinfo_servicecode
                    WHERE cbs.customerinfo_activationdate != clm.customerinfo_activationdate
                """),
                ("Deactivation Date Mismatches", """
                    SELECT COUNT(*) FROM cbs_service cbs
                    INNER JOIN clm_service clm ON cbs.customerinfo_servicecode = clm.customerinfo_servicecode
                    WHERE cbs.customerinfo_deactivationdate != clm.customerinfo_deactivationdate
                """),
                ("Reconciliation Success Rate", """
                    SELECT COUNT(*) FROM cbs_service cbs
                    INNER JOIN clm_service clm ON cbs.customerinfo_servicecode = clm.customerinfo_servicecode
                    WHERE cbs.customerinfo_status = clm.customerinfo_status
                    AND cbs.customerinfo_contracttype = clm.customerinfo_contracttype
                    AND cbs.customerinfo_activationdate = clm.customerinfo_activationdate
                    AND cbs.customerinfo_deactivationdate = clm.customerinfo_deactivationdate
                """)
            ]
            total_records = max(total_cbs, total_clm)
            for kpi_name, query_text in queries:
                cursor.execute(query_text)
                value = cursor.fetchone()[0]
                if kpi_name in ["Total Records in CBS", "Total Records in CLM"]:
                    percentage = 100.0
                elif total_records > 0:
                    percentage = round((value / total_records) * 100, 2)
                else:
                    percentage = 0.0
                cursor.execute(
                    """
                    INSERT INTO reconciliation_kpi_results (kpi_name, kpi_value, kpi_percentage)
                    VALUES (?, ?, ?)
                    """,
                    (kpi_name, value, percentage)
                )
            conn.commit()
            cursor.execute("SELECT kpi_name, kpi_value, kpi_percentage FROM reconciliation_kpi_results ORDER BY id")
            results = cursor.fetchall()
            conn.close()
            logger.info("Reconciliation executed successfully")
            return True, results
        except Exception as e:
            logger.error(f"Error executing reconciliation: {str(e)}")
            return False, []

    def get_reconciliation_results(self):
        """Get all reconciliation results"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT kpi_name, kpi_value, kpi_percentage
                FROM reconciliation_kpi_results
                ORDER BY id
            """)
            results = cursor.fetchall()
            conn.close()
            formatted_results = [
                {'kpi_name': row[0], 'kpi_value': row[1], 'kpi_percentage': row[2]}
                for row in results
            ]
            return formatted_results
        except Exception as e:
            logger.error(f"Error getting reconciliation results: {str(e)}")
            return []
