import pytest

import dep

#### Input tests


def test_doc(monkeypatch):

    monkeypatch.setattr("sys.argv", ["dep", "--doc", "hjk"])

    with pytest.raises(SystemExit) as exc_info:
        dep.Main()

    assert str(exc_info.value) == "Confluence document must be of type '.doc'"


def test_json(monkeypatch):

    monkeypatch.setattr("sys.argv", ["dep", "--json", "arg1_value"])

    with pytest.raises(SystemExit) as exc_info:
        dep.Main()

    assert str(exc_info.value) == "Could not find either confluence or json file. exiting . . ."


def test_no_doc(monkeypatch):

    monkeypatch.setattr("sys.argv", ["dep"])

    with pytest.raises(SystemExit) as exc_info:
        dep.Main()

    assert str(exc_info.value) == "Could not find either confluence or json file. exiting . . ."


def test_no_table(monkeypatch):

    monkeypatch.setattr("sys.argv", ["dep", "--json", "tests/test_input/arg_input_tests/input.json"])

    with pytest.raises(SystemExit) as exc_info:
        dep.Main()

    assert str(exc_info.value) == "Table structures must be defined either in a json or confluence file. exiting . . ."


def test_dryrun(monkeypatch):

    test_path = "tests/test_input/"
    test_dir = "import_confluence_oms"
    idoc = f"{test_path}{test_dir}/*.doc"

    monkeypatch.setattr("sys.argv", ["dep", "--doc", idoc, "--dryrun"])

    with pytest.raises(SystemExit) as exc_info:
        main = dep.Main()
        main.run()

    assert str(exc_info.value) == "Dry run complete."
