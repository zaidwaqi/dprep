test:
	@rm -rf tests/reports/*
	@pytest -v --tb=line || true
	