import logging
log = logging.getLogger(__name__)

import nox
from nox_utils import PY_VERSIONS


@nox.session(python=PY_VERSIONS, name="pre-commit-all", tags=["repo", "pre-commit"])
def run_pre_commit_all(session: nox.Session):
    session.install("pre-commit")
    session.run("pre-commit")

    log.info("Running all pre-commit hooks")
    session.run("pre-commit", "run")


@nox.session(python=PY_VERSIONS, name="pre-commit-update", tags=["repo", "pre-commit"])
def run_pre_commit_autoupdate(session: nox.Session):
    session.install(f"pre-commit")

    log.info("Running pre-commit autoupdate")
    session.run("pre-commit", "autoupdate")