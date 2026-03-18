# MBSync Order Checker — Flask App

## Setup
```bash
pip install -r requirements.txt
python run.py
```
Then open http://localhost:5000

## File Structure
```
run.py                        ← entry point
requirements.txt
order_app/
  __init__.py                 ← app factory
  routes.py                   ← upload + checklist routes
  mbsync_parser.py            ← PDF parser
  templates/
    index.html                ← upload page
    checklist.html            ← order checklist
```

## Usage
1. Open the app, upload an MBSync PDF
2. Use the filter buttons (All / Dry / Frozen / Refrigerated / Manual / Pending)
3. Enter received quantities by typing or clicking +1
4. Click **Summary** to see what's missing or over
5. Export a CSV from the summary modal