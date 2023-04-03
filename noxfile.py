import nox


# An example nox task definition that runs on many supported Python versions:
@nox.session(
    python=["3.10", "3.9"]
)
def test(session):
    session.install("-r", "requirements-dev.txt")

    session.run("pytest", *session.posargs)
