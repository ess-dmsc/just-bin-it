import nox


@nox.session(python=["3.8", "3.9", "3.10"])
def tests(session):
    session.install("-r", "requirements-dev.txt")
    session.run("pytest", *session.posargs)


@nox.session(python=["3.9"])
def ruff(session):
    session.install("-r", "requirements-dev.txt")
    session.run("ruff", "tests", "just_bin_it")
