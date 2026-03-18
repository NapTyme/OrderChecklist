import io
import json
import uuid
import os
import tempfile

from flask import Blueprint, render_template, request, jsonify, session

from .mbsync_parser import parse_mbsync_pdf

main = Blueprint("main", __name__)

# Use a temp directory for order data (avoids cookie size limits)
_TEMP_DIR = tempfile.mkdtemp(prefix="mbsync_")


def _save_order(header, items):
    token = str(uuid.uuid4())
    path = os.path.join(_TEMP_DIR, f"{token}.json")
    with open(path, "w") as f:
        json.dump({"header": header, "items": items}, f)
    return token


def _load_order(token):
    if not token:
        return None, None
    path = os.path.join(_TEMP_DIR, f"{token}.json")
    if not os.path.exists(path):
        return None, None
    with open(path) as f:
        data = json.load(f)
    return data["header"], data["items"]


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
        return render_template("index.html", error="No order data found. Please upload a PDF first.")

    return render_template("checklist.html", header=header, items=items)