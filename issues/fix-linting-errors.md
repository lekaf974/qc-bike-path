# Fix linting errors causing CI failure

CI is failing because of linting/style errors (trailing whitespace, missing trailing newlines, variable naming (N806), and not using pytest.raises for exception checks). See logs from https://github.com/lekaf974/qc-bike-path/actions/runs/17536022789/job/49799390759 for details. Please fix these issues to restore CI.