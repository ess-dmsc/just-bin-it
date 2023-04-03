import nox


# An example nox task definition that runs on many supported Python versions:
@nox.session(
    python=["3.8", "3.9", "3.10"]
)
def tests(session):
    session.install("-r", "requirements-dev.txt")

    session.run("pytest", *session.posargs)


@nox.session(
    python=["3.10", "3.9"]
)
def flake8(session):
    session.install("-r", "requirements-dev.txt")

    session.run("flake8", "tests", "just_bin_it")
