#!/bin/bash
cd "$(dirname "$0")"
pip3 install -q -r requirements.txt
python3 -m uvicorn main:app --host 0.0.0.0 --port 8765 --reload
