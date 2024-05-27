test:
	@rm -rf tests/reports/*
	@pytest -v --tb=line || true

hot-reload:
	@pip uninstall ndpprep -y
	@python -m build
	@pip install dist/ndpprep-*.tar.gz
