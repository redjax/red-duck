import logging

log = logging.getLogger(__name__)

from nox_utils import DEFAULT_PYTHON

import nox

PROJECT_NAME: str = "red_duck"


@nox.session(python=[DEFAULT_PYTHON], name="vulture-check", tags=["quality"])
def run_vulture_check(session: nox.Session):
    session.install(f"vulture")

    log.info("Checking for dead code with vulture")
    try:
        session.run("vulture", f"src/{PROJECT_NAME}", "--min-confidence", "100")
    except Exception as exc:
        log.info(
            f"\nNote: For some reason, this always 'fails' with exit code 3. Vulture still works when running in a Nox session, it seems this error can be ignored."
        )


@nox.session(python=[DEFAULT_PYTHON], name="bandit-check", tags=["quality"])
def run_bandit_check(session: nox.Session):
    session.install(f"bandit")

    log.info("Checking code security with bandit")
    try:
        session.run("bandit", "-r", f"src/{PROJECT_NAME}")
    except Exception as exc:
        log.warning(
            f"\nNote: For some reason, this always 'fails' with exit code 1. Bandit still works when running in a Nox session, it seems this error can be ignored."
        )


@nox.session(python=[DEFAULT_PYTHON], name="bandit-baseline", tags=["quality"])
def run_bandit_baseline(session: nox.Session):
    session.install(f"bandit")

    log.info("Getting bandit baseline")
    try:
        session.run(
            "bandit",
            "-r",
            f"src/{PROJECT_NAME}",
            "-f",
            "json",
            "-o",
            "bandit_baseline.json",
        )
    except Exception as exc:
        log.warning(
            f"\nNote: For some reason, this always 'fails' with exit code 1. Bandit still works when running in a Nox session, it seems this error can be ignored."
        )


@nox.session(python=[DEFAULT_PYTHON], name="detect-secrets", tags=["quality"])
def scan_for_secrets(session: nox.Session):
    session.install("detect-secrets")

    log.info("Scanning project for secrets")
    session.run("detect-secrets", "scan")


@nox.session(python=[DEFAULT_PYTHON], name="radon-code-complexity", tags=["quality"])
def radon_code_complexity(session: nox.Session):
    session.install("radon")

    log.info("Getting code complexity score")
    session.run(
        "radon",
        "cc",
        f"src/{PROJECT_NAME}",
        "-s",
        "-a",
        "--total-average",
        "-nc",
        # "-j",
        # "-O",
        # "radon_complexity_results.json",
    )


@nox.session(python=[DEFAULT_PYTHON], name="radon-raw", tags=["quality"])
def radon_raw(session: nox.Session):
    session.install("radon")

    log.info("Running radon raw scan")
    session.run(
        "radon",
        "raw",
        f"src/{PROJECT_NAME}",
        "-s",
        # "-j",
        # "-O",
        # "radon_raw_results.json"
    )


@nox.session(python=[DEFAULT_PYTHON], name="radon-maintainability", tags=["quality"])
def radon_maintainability(session: nox.Session):
    session.install("radon")

    log.info("Running radon maintainability scan")
    session.run(
        "radon",
        "mi",
        f"src/{PROJECT_NAME}",
        "-n",
        "C",
        "-x",
        "F",
        "-s",
        # "-j",
        # "-O",
        # "radon_maitinability_results.json",
    )


@nox.session(python=[DEFAULT_PYTHON], name="radon-halstead", tags=["quality"])
def radon_halstead(session: nox.Session):
    session.install("radon")

    log.info("Running radon Halstead metrics scan")
    session.run(
        "radon",
        "hal",
        "src",
        "red_duck",
        "-f",
        # "-j",
        # "-O",
        # "radon_halstead_results.json",
    )


@nox.session(python=[DEFAULT_PYTHON], name="xenon", tags=["quality"])
def xenon_scan(session: nox.Session):
    session.install("xenon")

    log.info("Scanning complexity with xenon")
    try:
        session.run("xenon", "-b", "B", "-m", "C", "-a", "C", f"src/{PROJECT_NAME}")
    except Exception as exc:
        log.warning(
            f"\nNote: For some reason, this always 'fails' with exit code 1. Xenon still works when running in a Nox session, it seems this error can be ignored."
        )
