rm -rf dist build *.egg-info
python -m build
python -m pip uninstall -y stillpoint
python -m pip install dist/*.whl
