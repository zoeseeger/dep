import os
import glob
import pytest

import dep

#### Output tests

test_path = "tests/test_input/"
solution_path = "tests/solutions/"
tests = [
    ("import_confluence_oms", True, False),
    ("new_table_confluence_ilp", True, False),
    ("new_table_confluence_json_employee_sales", True, True),
    ("new_table_json_employee_sales", False, True),
]


def clean():
    """Remove var files from last run."""

    file_list = glob.glob(f"{test_path}**/vars.json", recursive=True)

    for f in file_list:
        os.remove(f)


def paths(dir):
    """Return set of test_paths for a test."""

    # test input
    ijson = f"{test_path}{dir}/input.json"
    idoc = f"{test_path}{dir}/*.doc"

    # test output
    out = f"{test_path}{dir}/dep-output"
    tvar = f"{test_path}{dir}/dep-output/vars.json"
    tddl = f"{test_path}{dir}/dep-output/*.sql"
    tdag = f"{test_path}{dir}/dep-output/*.py"
    tmd = f"{test_path}{dir}/dep-output/*.md"

    # solution output
    svar = f"{solution_path}{dir}/vars.json"
    sddl = f"{solution_path}{dir}/*.sql"
    sdag = f"{solution_path}{dir}/*.py"
    smd = f"{solution_path}{dir}/*.md"

    return ijson, idoc, out, tvar, tddl, tdag, tmd, svar, sddl, sdag, smd


@pytest.mark.parametrize("test_dir, confluence_input, json_input", tests)
def test_vars(test_dir, confluence_input, json_input, monkeypatch):

    clean()

    # solution
    svar = f"{solution_path}{test_dir}/vars.json"
    monkeypatch.setattr("sys.argv", ["dep", "--json", svar])
    solution_obj = dep.Main()

    # test
    idoc = f"{test_path}{test_dir}/*.doc"
    ijson = f"{test_path}{test_dir}/input.json"

    # specify input parameters for test run
    command_line_vars = ["dep"]
    if confluence_input:
        command_line_vars.extend(["--doc", idoc])
    if json_input:
        command_line_vars.extend(["--json", ijson])

    # run
    monkeypatch.setattr("sys.argv", command_line_vars)
    test_obj = dep.Main()

    # equalities
    assert solution_obj.options.jira_id == test_obj.options.jira_id
    assert solution_obj.options.action == test_obj.options.action
    assert solution_obj.options.user == test_obj.options.user
    assert solution_obj.options.user_email == test_obj.options.user_email
    assert solution_obj.options.description == test_obj.options.description
    assert solution_obj.options.subject_area == test_obj.options.subject_area
    assert solution_obj.options.schedule == test_obj.options.schedule
    assert solution_obj.options.staging_table == test_obj.options.staging_table
    assert solution_obj.options.ddl_file == test_obj.options.ddl_file
    assert solution_obj.options.dag_file == test_obj.options.dag_file
    assert solution_obj.options.doc_file == test_obj.options.doc_file
    assert solution_obj.options.debug == test_obj.options.debug
    assert len(test_obj.options.table_structures) == len(
        solution_obj.options.table_structures
    )
    assert len(test_obj.options.view_structures) == len(
        solution_obj.options.view_structures
    )

    # tables
    for i, test_table in enumerate(test_obj.options.table_structures):
        solution_table = solution_obj.options.table_structures[i]
        assert solution_table.schema == test_table.schema
        assert solution_table.table_name == test_table.table_name
        assert solution_table.schema_table == test_table.schema_table
        assert solution_table.import_method == test_table.import_method
        assert solution_table.table_description == test_table.table_description
        assert solution_table.columns == test_table.columns
        assert solution_table.types == test_table.types
        assert solution_table.nullables == test_table.nullables
        assert solution_table.descriptions == test_table.descriptions
        assert solution_table.source_columns == test_table.source_columns
        assert solution_table.source_alias == test_table.source_alias
        assert solution_table.sources == test_table.sources
        assert solution_table.indexes == test_table.indexes
        assert solution_table.primary_keys == test_table.primary_keys
        assert solution_table.foreign_keys == test_table.foreign_keys

    # views
    for i, test_table in enumerate(test_obj.options.view_structures):
        solution_table = solution_obj.options.view_structures[i]
        assert solution_table.schema == test_table.schema
        assert solution_table.view_name == test_table.view_name
        assert solution_table.foundation_table_name == test_table.foundation_table_name
        assert solution_table.columns == test_table.columns
        assert solution_table.descriptions == test_table.descriptions
        assert solution_table.source_columns == test_table.source_columns
        assert solution_table.source_alias == test_table.source_alias
        assert solution_table.sources == test_table.sources
        assert solution_table.materialized == test_table.materialized
