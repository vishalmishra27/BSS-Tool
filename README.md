# BSS Migration Assurance Tool

## Prerequisites
- Node.js 18+
- Python 3.10+
- PostgreSQL 14+

---

## 1. Database Setup

```bash
# Create database
psql -U postgres -c "CREATE DATABASE bss_tool;"

# Run schema + seed data
psql -U postgres -d bss_tool -f backend/schema.sql
```

---

## 2. Backend Setup

```bash
cd backend

# Create virtual environment
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure DB (edit if needed)
# Default: host=localhost, port=5432, db=bss_tool, user=postgres, password=postgres
# Override with environment variables:
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=bss_tool
export DB_USER=postgres
export DB_PASSWORD=postgres

# Start backend
python app.py
# Backend runs on http://localhost:3001
```

---

## 3. Frontend Setup

```bash
cd frontend

# Install dependencies
npm install

# Start dev server
npm run dev
# Frontend runs on http://localhost:5173
```

---

## 4. Open the app

Navigate to: **http://localhost:5173**

---

## Routes

| URL | Module |
|-----|--------|
| `/dashboard` | Transformation Dashboard |
| `/milestones` | Project Milestones |
| `/status` | Status Tracker |
| `/product-dashboard` | Product Journey |
| `/uat` | UAT Management |
| `/reconciliation` | KPI Reconciliation |
| `/workflow` | Workflow Tracker |
| `/bpm` | BPM |
| `/migration` | Migration Summary |
| `/pdf-analysis` | PDF Analysis |

---

## Project Structure

```
bss-tool/
├── backend/
│   ├── app.py                    # Flask entry point
│   ├── schema.sql                # DB schema + seed data
│   ├── requirements.txt
│   ├── reconciliation_service.py
│   └── reconciliation_endpoints.py
└── frontend/
    ├── src/
    │   ├── App.jsx               # Router + Sidebar
    │   ├── main.jsx
    │   ├── index.css
    │   ├── pages/                # All page components
    │   ├── components/           # Shared components
    │   └── context/              # React context providers
    ├── package.json
    └── vite.config.js
```
