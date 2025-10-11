all:
	@echo "Installing dependencies..."
	@cd backend && pip install -r requirements.txt
	@cd frontend && npm install
	@echo "Starting backend and frontend..."
	@trap 'kill %1; kill %2' INT; \
	cd backend && PYTHONPATH=.. python -m uvicorn main:app --reload --host 127.0.0.1 --port 8000 & \
	cd frontend && npm run dev & \
	wait
