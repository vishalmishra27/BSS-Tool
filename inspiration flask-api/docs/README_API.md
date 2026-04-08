# RCM Analysis Flask API

Flask REST API for four RCM (Risk Control Matrix) analysis tools:
1. **AI Suggest** - AI-powered gap analysis and suggestions
2. **Control Assessment** - OnGround check with policy/SOP validation
3. **Deduplication** - Semantic duplicate detection using LLM
4. **TOD Test** - Test of Design using `rcm_tester.py` with evidence folder

---

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Start the Server
```bash
python api.py
```

The server will start at `http://localhost:5002` (port 5002)

### 3. Test the API
Use curl or any HTTP client to test the endpoints (see examples below).

---

## API Endpoints

All endpoints support:
- **POST**: Run the analysis (returns JSON with full Excel data embedded)
- **GET**: Retrieve latest JSON results (includes entire Excel file as JSON)
- **GET?download=1**: Download the Excel output file (.xlsx)

### Base URL
```
http://localhost:5002
```

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | API documentation |
| `/api/health` | GET | Health check |
| `/api/ai-suggest` | POST/GET | AI gap analysis |
| `/api/control-assessment` | POST/GET | Control assessment |
| `/api/deduplication` | POST/GET | Duplicate detection |
| `/api/tod-test` | POST/GET | TOD testing (RCM + evidence) |

---

## Usage Examples

### 1. AI Suggest Endpoint

**Run Analysis (POST)**
```bash
curl -X POST http://localhost:5002/api/ai-suggest \
  -H "Content-Type: application/json" \
  -d '{}'
```

With custom parameters:
```bash
curl -X POST http://localhost:5002/api/ai-suggest \
  -H "Content-Type: application/json" \
  -d '{
    "rcm_file_path": "/Users/rishi/Downloads/Sample_Data/output.xlsx",
    "industry": "Banking & Financial Services"
  }'
```

**Get JSON Results (GET)**
```bash
curl http://localhost:5002/api/ai-suggest
```

**Download Excel File (GET)**
```bash
curl -O -J http://localhost:5002/api/ai-suggest?download=1
```

**Response Example**
```json
{
  "status": "success",
  "timestamp": "20260211_143025",
  "excel_output": "/Users/rishi/Downloads/Sample_Data/AI_Analysis_20260211_143025.xlsx",
  "json_output": "/Users/rishi/Downloads/Sample_Data/AI_Analysis_20260211_143025.json",
  "text_output": "/Users/rishi/Downloads/Sample_Data/AI_Analysis_20260211_143025.txt",
  "input_file": "/Users/rishi/Downloads/Sample_Data/output.xlsx",
  "industry": "Banking & Financial Services",
  "download_url": "/api/ai-suggest?download=1",
  "excel_data": {
    "file_path": "/Users/rishi/Downloads/Sample_Data/AI_Analysis_20260211_143025.xlsx",
    "sheets": {
      "Sheet1": {
        "columns": ["Control Id", "Process", "Risk Title", "..."],
        "row_count": 45,
        "data": [
          {
            "Control Id": "C001",
            "Process": "Procure to Pay",
            "Risk Title": "Unauthorized Access",
            "...": "..."
          },
          "... (all rows as JSON objects)"
        ]
      }
    }
  }
}
```
**Note**: The `excel_data` field contains the entire Excel file converted to JSON format, with all sheets and their data.

---

### 2. Control Assessment Endpoint

**Run Assessment (POST)**
```bash
curl -X POST http://localhost:5002/api/control-assessment \
  -H "Content-Type: application/json" \
  -d '{
    "rcm_file_path": "/Users/rishi/Downloads/Sample_Data/output.xlsx",
    "policy_paths": [
      "/Users/rishi/Downloads/Sample_Data/Policy_Procure_to_Pay.pdf"
    ],
    "sop_paths": [
      "/Users/rishi/Downloads/Sample_Data/SOP_Procure_to_Pay.pdf"
    ]
  }'
```

**Get JSON Results (GET)**
```bash
curl http://localhost:5002/api/control-assessment
```

**Download Excel File (GET)**
```bash
curl -O -J http://localhost:5002/api/control-assessment?download=1
```

---

### 3. Deduplication Endpoint

**Run Deduplication (POST)**
```bash
curl -X POST http://localhost:5002/api/deduplication \
  -H "Content-Type: application/json" \
  -d '{
    "rcm_input": "/Users/rishi/Downloads/Sample_Data/output.xlsx",
    "input_is_folder": false
  }'
```

**Get JSON Results (GET)**
```bash
curl http://localhost:5002/api/deduplication
```

**Download Excel File (GET)**
```bash
curl -O -J http://localhost:5002/api/deduplication?download=1
```

---

### 4. TOD Test Endpoint

**Run TOD Test (POST)**
```bash
curl -X POST http://localhost:5002/api/tod-test \
  -H "Content-Type: application/json" \
  -d '{
    "rcm_path": "/Users/rishi/Downloads/Sample_Data/output.csv",
    "evidence_folder": "/Users/rishi/KPMG/11FebN/evidence_2",
    "azure_endpoint": "https://<your-endpoint>.openai.azure.com",
    "azure_api_key": "<your-key>",
    "azure_deployment": "gpt-4o-mini",
    "max_workers": 5
  }'
```

**Get JSON Results (GET)**
```bash
curl http://localhost:5002/api/tod-test
```

**Download Excel File (GET)**
```bash
curl -O -J http://localhost:5002/api/tod-test?download=1
```

---

## Python Client Example

```python
import requests

# 1. Run AI Suggest Analysis
response = requests.post(
    'http://localhost:5002/api/ai-suggest',
    json={
        'rcm_file_path': '/Users/rishi/Downloads/Sample_Data/output.xlsx',
        'industry': 'Banking & Financial Services'
    }
)

result = response.json()
print(f"Status: {result['status']}")
print(f"Excel Output: {result['excel_output']}")

# 2. Get JSON Results
response = requests.get('http://localhost:5002/api/ai-suggest')
json_data = response.json()

# 3. Download Excel File
response = requests.get('http://localhost:5002/api/ai-suggest?download=1')
with open('analysis_results.xlsx', 'wb') as f:
    f.write(response.content)
print("Excel file downloaded!")
```

---

## Request/Response Flow

### Step 1: POST - Run Analysis
```
POST /api/ai-suggest
Body: {"rcm_file_path": "...", "industry": "..."}

↓

Response: {
  "status": "success",
  "excel_output": "/path/to/output.xlsx",
  "summary": {...},
  "download_url": "/api/ai-suggest?download=1"
}
```

### Step 2a: GET - Retrieve JSON
```
GET /api/ai-suggest

↓

Response: {
  "status": "success",
  "summary": {...},
  ...
}
```

### Step 2b: GET - Download Excel
```
GET /api/ai-suggest?download=1

↓

Response: Excel file (application/vnd.openxmlformats-officedocument.spreadsheetml.sheet)
```

---

## Error Handling

### Error Response Format
```json
{
  "status": "error",
  "error": "Error message here",
  "traceback": "Full Python traceback..."
}
```

### Common Errors

**404 - No Results Found**
```json
{
  "error": "No AI Suggest analysis has been run yet. Please POST first."
}
```

**500 - Internal Server Error**
```json
{
  "status": "error",
  "error": "File not found: /path/to/file.xlsx",
  "traceback": "..."
}
```

---

## Configuration

### Default File Paths

The API uses these default paths (from the .py files):
- **RCM File**: `/Users/rishi/Downloads/Sample_Data/output.xlsx`
- **Policy PDF**: `/Users/rishi/Downloads/Sample_Data/Policy_Procure_to_Pay.pdf`
- **SOP PDF**: `/Users/rishi/Downloads/Sample_Data/SOP_Procure_to_Pay.pdf`
- **Industry**: `Banking & Financial Services`

You can override these by passing them in the POST request body.

### Output Files

Generated files are saved in the same directory as the input RCM file with timestamp:
- `AI_Analysis_YYYYMMDD_HHMMSS.xlsx`
- `Control_Assessment_YYYYMMDD_HHMMSS.xlsx`
- `Duplicates_YYYYMMDD_HHMMSS.xlsx`
- `TOD_Results_YYYYMMDD_HHMMSS.xlsx`
- `TOD_Report_YYYYMMDD_HHMMSS.txt`

---

## Testing

### 1. Manual Testing with curl

```bash
# Health check
curl http://localhost:5002/api/health

# Run AI analysis with defaults
curl -X POST http://localhost:5002/api/ai-suggest \
  -H "Content-Type: application/json" \
  -d '{}'

# Download result
curl -O -J http://localhost:5002/api/ai-suggest?download=1
```

### 2. Automated Testing

You can create your own test scripts using the curl examples above or use the Python client examples provided.

---

## File Structure

```
11FebN/
├── api.py                    # Flask API server
├── requirements.txt          # Python dependencies
├── README_API.md             # This file
│
├── AiSuggest.py              # AI analysis module
├── ControlAssesment.py       # Control assessment module
├── DeDupli.py                # Deduplication module
├── rcm_tester.py             # TOD testing module
│
└── (other files)
```

---

## Deployment Notes

### Development (Current Setup)
```bash
python api.py
```

### Production (Recommended)

1. **Use Gunicorn**
```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5002 api:app
```

2. **Use Docker**
```dockerfile
FROM python:3.9
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5002", "api:app"]
```

3. **Add to Production**
- Add authentication/authorization
- Use Redis/database for result storage
- Add rate limiting
- Configure CORS for specific origins
- Add request validation
- Implement proper logging
- Use environment variables for secrets
- Add background task queue (Celery) for long-running jobs

---

## Troubleshooting

### Issue: "Module not found"
```bash
# Make sure you're in the correct directory
cd /Users/rishi/KPMG/11FebN

# Install dependencies
pip install -r requirements.txt
```

### Issue: "Port already in use"
```bash
# Find and kill process on port 5002
lsof -ti:5002 | xargs kill -9

# Or use a different port
# Edit api.py: app.run(port=5003)
```

### Issue: "File not found"
```bash
# Check the file paths in your POST request
# Make sure the paths are absolute, not relative
# Example: /Users/rishi/Downloads/... (not ~/Downloads/...)
```

### Issue: "Analysis takes too long"
- The analysis runs synchronously and can take time for large files
- Consider implementing background tasks with Celery for production
- Check console output for progress

---

## Support

For issues or questions:
1. Check the console output from `python api.py`
2. Review the traceback in error responses
3. Test endpoints manually using curl or the Python examples above

---

## License

Internal KPMG Tool - February 2026
