import io
import json
import uuid
import os
import tempfile

from flask import Blueprint, render_template, request, jsonify, session

from .mbsync_parser import parse_mbsync_pdf

main = Blueprint("main", __name__)

# Use a temp directory for order data (avoids cookie size limits).
# On Render/Railway this persists for the lifetime of the dyno — long enough
# for a receiving shift. For true persistence across redeploys, swap _TEMP_DIR
# for a fixed path on a mounted disk (Render) or an object store.
_TEMP_DIR = tempfile.mkdtemp(prefix="mbsync_")


# ── File helpers ────────────────────────────────────────────────────────────

def _order_path(token):
    return os.path.join(_TEMP_DIR, f"{token}_order.json")

def _progress_path(token):
    return os.path.join(_TEMP_DIR, f"{token}_progress.json")


def _save_order(header, items):
    token = str(uuid.uuid4())
    with open(_order_path(token), "w") as f:
        json.dump({"header": header, "items": items}, f)
    return token


def _load_order(token):
    if not token:
        return None, None
    path = _order_path(token)
    if not os.path.exists(path):
        return None, None
    with open(path) as f:
        data = json.load(f)
    return data["header"], data["items"]


def _save_progress(token, received):
    """Persist { wrin: received_value } dict to disk."""
    with open(_progress_path(token), "w") as f:
        json.dump(received, f)


def _load_progress(token):
    """Load saved progress. Returns {} if nothing saved yet."""
    if not token:
        return {}
    path = _progress_path(token)
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


# ── Routes ──────────────────────────────────────────────────────────────────

@main.route("/", methods=["GET"])
def index():
    return render_template("index.html")


@main.route("/upload", methods=["POST"])
def upload():
    if "pdf" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files["pdf"]
    if not file.filename.lower().endswith(".pdf"):
        return jsonify({"error": "Please upload a PDF file"}), 400

    try:
        pdf_bytes = file.read()
        header, items = parse_mbsync_pdf(io.BytesIO(pdf_bytes))
    except Exception as e:
        return jsonify({"error": f"Failed to parse PDF: {str(e)}"}), 500

    token = _save_order(header, items)
    session["order_token"] = token

    return jsonify({"success": True, "item_count": len(items)})


@main.route("/checklist")
def checklist():
    token = session.get("order_token")
    header, items = _load_order(token)

    if not items:
        return render_template(
            "index.html",
            error="No order data found. Please upload a PDF first."
        )

    # Load any previously saved progress so the template can seed the JS state
    progress = _load_progress(token)

    return render_template(
        "checklist.html",
        header=header,
        items=items,
        progress=progress,
    )


@main.route("/save", methods=["POST"])
def save_progress():
    """
    Called by the client whenever a quantity changes (debounced).
    Body JSON: { "received": { "wrin1": 3.0, "wrin2": 5.0, ... } }
    Returns:   { "ok": true }
    """
    token = session.get("order_token")
    if not token:
        return jsonify({"error": "No active session"}), 400

    data = request.get_json(silent=True)
    if data is None or "received" not in data:
        return jsonify({"error": "Invalid payload"}), 400

    _save_progress(token, data["received"])
    return jsonify({"ok": True})


@main.route("/load", methods=["GET"])
def load_progress():
    """
    Polling endpoint — lets the client re-sync its in-memory state with the
    server (e.g. after a tab restore or network blip).
    Returns: { "received": { ... } }
    """
    token = session.get("order_token")
    return jsonify({"received": _load_progress(token)})