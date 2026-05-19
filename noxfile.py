import nox


@nox.session(python=["3.12"])
def tests(session):
    session.install("-e", ".")
    session.install("mock", "pytest", "pytest-cov")
    session.run("pytest", *session.posargs)


@nox.session(python=["3.12"])
def ruff(session):
    session.install("ruff==0.4.4")
    session.run("ruff", "check", "tests", "just_bin_it", "bin")
