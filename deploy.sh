set -x
set -e
rm -Rf dist/
python -m build
python -m twine upload --repository pypi dist/*

