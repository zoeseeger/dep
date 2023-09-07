from src.functions import writeLinesFile


class DocInput:
    """Class defining input of the doc unit test file."""

    def __init__(self, options:object):

        self.jira_id = options.jira_id
        self.action = options.action
        self.user = options.user
        self.doc_file = options.doc_file
        self.output_dir = options.output_dir
        self.table_structures = options.table_structures
        self.view_structures = options.view_structures

        # defined in subsequent functions
        self.lines = []
        self.table = None
        self.schema = None

        self.run()

    def run(self):
        """Determine input for DDL."""

        self.header()

        # ddl test
        for self.table in self.table_structures:
            self.ddlTableColumnsTest()

            if self.table.final_table and self.table.import_method == "incremental":
                self.ddlTableParametersTest()

        for self.view in self.view_structures:
            self.ddlViewColumnsTest()

        # dag unit tests
        self.lines.append("## DAG Unit Tests")

        for self.table in self.table_structures:
            # cannot directly compare method to source
            if self.action == "import":
                # dont worry about tmp
                if self.table.import_method == "fivetran legacy":
                    self.dagTableImportFivetranTest()
                elif not self.table.schema == "tmp":
                    self.dagTableImportTest()
            else:
                self.dagTableTest()

        for self.view in self.view_structures:
            self.dagViewTest()

        writeLinesFile(self.output_dir, self.doc_file, self.lines)

    def ddlTableColumnsTest(self):
        """Show columns of table unit test."""

        self.lines.extend([
            f"### Structure of {self.table.schema}.{self.table.table_name}",
            f"```SQL",
            f"SELECT",
            f"    column_name",
            f"    , data_type",
            f"FROM",
            f"    information_schema.columns",
            f"WHERE",
            f"    table_schema = '{self.table.schema}'",
            f"    AND table_name = '{self.table.table_name}';",
            f"```",
            f""
        ])

    def ddlViewColumnsTest(self):
        """Show columns of table unit test."""

        self.lines.extend([
            f"### Structure of {self.view.schema}.{self.view.view_name}",
            f"```SQL",
            f"SELECT",
            f"    a.attname AS column_name",
            f"    , pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type",
            f"FROM",
            f"    pg_attribute a",
            f"JOIN pg_class t ON",
            f"    a.attrelid = t.oid",
            f"JOIN pg_namespace s ON",
            f"    t.relnamespace = s.oid",
            f"WHERE",
            f"    a.attnum > 0",
            f"    AND NOT a.attisdropped",
            f"    AND t.relname = '{self.view.view_name}'",
            f"    AND s.nspname = '{self.view.schema}'",
            f"ORDER BY",
            f"    a.attnum;",
            f"```",
            f""
        ])

    def ddlTableParametersTest(self):
        """Entry in adm.table_parameters unit test."""

        self.lines.extend([
            f"### Table parameters for {self.table.schema}.{self.table.table_name}",
            f"```SQL",
            f"SELECT * FROM adm.table_parameters ",
            f"WHERE table_name = '{self.table.schema}.{self.table.table_name}';",
            f"```",
            f""
        ])

    def dagTableTest(self):
        """DAG unit test for table - compare column rows to source"""

        if self.table.sources:
            source = next(iter(self.table.sources.values()))["source_table"]
        else:
            source = 'xxx'

        self.lines.extend([
            f"### Rows of {self.table.schema}.{self.table.table_name} compared to source",
            f"```SQL",
            f"WITH {self.table.schema} AS (",
            f"    SELECT count(*)",
            f"    FROM {self.table.schema}.{self.table.table_name}",
            f"),",
            f"src AS (",
            f"    SELECT count(*)",
            f"    FROM {source}",
        ])

        # date condition for intermediate tables
        if self.table.schema in ["tmp", "staging"]:
            self.lines.extend([
                f"    WHERE record_created <= xxx",
                f"    AND record_created > xxx",
            ])

        self.lines.extend([
            f")",
            f"SELECT",
            f"    {self.table.schema}.count = src.count AS same_count",
            f"    , {self.table.schema}.count AS count",
            f"    , src.count AS count",
            f"FROM ",
            f"    {self.table.schema}, src",
            f"```",
            f""
        ])

    def dagTableImportTest(self):
        """Test DAG for import as cannot directly compare glinda table to source table."""

        if self.table.sources:
            source = next(iter(self.table.sources.values()))["source_table"]
        else:
            source = 'xxx'

        self.lines.extend([
            f"### Rows of {self.table.schema}.{self.table.table_name}",
            f"```SQL",
            f"SELECT count(*)",
            f"FROM {self.table.schema}.{self.table.table_name}",
            f"```",
            f"",
            f"```SQL",
            f"SELECT count(*)",
            f"FROM {source}",
            f"```"
        ])

    def dagTableImportFivetranTest(self):
        """Test DAG for import to PSA as cannot directly compare glinda table to source table."""

        if self.table.sources:
            source = next(iter(self.table.sources.values()))["source_table"]
            try:
                print(source)
                print(source.split('.')[1])
                source_table = source.split('.')[1]
            except IndexError:
                source_table = 'xxx_table_name'
        else:
            source = 'xxx_source_schema_table'

        self.lines.extend([
            f"### Rows of {self.table.schema}.{self.table.table_name}",
            f"Source",
            f"```SQL",
            f"",
            f"SELECT count(*)",
            f"FROM xxx_source_schema.{source_table}",
            f"",
            f"```",
            f"",
            f"Landing",
            f"```SQL",
            f"",
            f"SELECT count(*) FROM {source}",
            f"WHERE \"_fivetran_deleted\" IS FALSE",
            f"",
            f"```",
            f"",
            f"PSA",
            f"```SQL",
            f"",
            f"SELECT count(*) FROM (",
            f"    SELECT DISTINCT ON (",
            f"        {', '.join(self.table.business_keys)}",
            f"        ) *",
            f"    FROM {self.table.schema}.{self.table.table_name}",
            f"    ORDER BY",
            f"        {', '.join(self.table.business_keys)}, load_datetime desc",
            f") a",
            f"WHERE",
            f"    record_deleted IS FALSE",
            f"",
            f"```"
        ])

    def dagViewTest(self):
        """DAG unit test for view - compare column rows to source."""

        if self.view.foundation_table_name:
            source = f"{self.view.schema}.{self.view.foundation_table_name}"
        else:
            source = 'xxx'

        self.lines.extend([
            f"### Rows of {self.view.schema}.{self.view.view_name} compared to source.",
            f"```SQL",
            f"WITH {self.view.schema} AS (",
            f"    SELECT count(*)",
            f"    FROM {self.view.schema}.{self.view.view_name}",
            f"),",
            f"src AS (",
            f"    SELECT count(*)",
            f"    FROM {source}",
            f")",
            f"SELECT",
            f"    {self.view.schema}.count = src.count AS same_count",
            f"    , {self.view.schema}.count AS count",
            f"    , src.count AS count",
            f"FROM ",
            f"    {self.view.schema}, src",
            f"```",
            f""
        ])

    def header(self):
        """Header for unit test md document."""

        self.lines.extend([
            f"# Unit Test - {self.jira_id}",
            f"",
            f"## Overview",
            f"The test cases cover the following Jira ticket [{self.jira_id}](https://jira.mecca.com.au/browse/{self.jira_id}).",
            f"",
            f"## DDL Unit Tests"
        ])