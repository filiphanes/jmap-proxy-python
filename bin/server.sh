#!/bin/sh

uvicorn server:app --host 0.0.0.0 --port 5000 --loop uvloop --log-level info --workers 1