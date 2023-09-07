import math
import datetime

from src.functions import makeOutputDirectory, writeLinesFile


class DdlInput:
    """Class defining input of the DDL."""

    def __init__(self, options:object):

        self.user = options.user
        self.action = options.action
        self.table_structures = options.table_structures
        self.view_structures = options.view_structures
        self.ddl_file = options.ddl_file
        self.output_dir = options.output_dir
        self.description = options.description
        self.subject_area = options.subject_area
        self.schedule = options.schedule

        # defined in subsequent functions
        self.lines = []
        self.drop = []
        self.table = None
        self.view = None

        # make file
        self.run()

    def run(self):
        """Determine input for DDL."""

        # new schema lines
        if self.action == "import":
            for table in self.table_structures:
                if table.schema != "tmp":
                    self.newSchema(table.schema)
                    break

        # main table: definition > indexes > comments > permissions
        # tmp & staging: definition > permissions
        for self.table in self.table_structures:
            self.buildTable()
            self.tableIndexes()         # not tmp or stg
            self.tableDescription()     # not tmp or stg
            self.columnDescriptions()   # not tmp or stg
            self.addToTableParameters() # not tmp or stg or full load
            self.defaultTablePermissions()

        # view: definition > comments > permissions
        for self.view in self.view_structures:
            self.buildView()
            self.viewDescription()
            self.columnDescriptions(table=False)
            self.defaultViewPermissions()

        self.writeDrop()

        # write lines to file
        makeOutputDirectory(self.output_dir)
        writeLinesFile(self.output_dir, self.ddl_file, self.lines)

    def newSchema(self, schema):
        """New schema and associated role lines."""

        self.header("Create schema")

        self.lines.extend([
            f"CREATE SCHEMA {schema};",
            f"ALTER SCHEMA {schema} OWNER TO role_master;",
            f"COMMENT ON SCHEMA {schema} IS '{self.description}.';",
            f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} TO role_master;",
            f"",
            f"CREATE ROLE role_select_{schema} WITH",
            f"    NOLOGIN",
            f"    NOSUPERUSER",
            f"    INHERIT",
            f"    NOCREATEDB",
            f"    NOCREATEROLE",
            f"    NOREPLICATION;",
            f"",
            f"GRANT role_connect_glinda TO role_select_{schema};",
            f"COMMENT ON ROLE role_select_{schema} IS 'Access role to all datasources in {schema} schema';",
            f"GRANT USAGE ON SCHEMA {schema} TO role_select_{schema};",
            f"",
        ])

    def buildTable(self):
        """DDL for a table."""

        # start
        self.header(f'Create {self.table.schema} schema table')
        self.lines.append(f"CREATE TABLE {self.table.schema}.{self.table.table_name} (",)

        self.addColumnsTable()

        if self.table.primary_keys:
            self.lines.append(f"    , CONSTRAINT {self.table.table_name}_pk PRIMARY KEY ({', '.join(self.table.primary_keys)})")
        if self.table.foreign_keys:
            for fk in self.table.foreign_keys:
                self.lines.append(f"    , CONSTRAINT {fk}_pk FOREIGN KEY ({fk}) REFERENCES xxx({fk})")

        # end
        self.lines.append(');')
        self.lines.append('')

        # drop self.table for debugging
        self.drop.append(f"-- DROP TABLE IF EXISTS {self.table.schema}.{self.table.table_name};")

    def addColumnsTable(self):
        """Add columns to self.table."""

        columns_to_quote = ["limit"]

        # iterate through list of lists
        for i, column in enumerate(self.table.columns):

            line = ""
            if i > 0:
                line += "    , "
            else:
                line += "    "
            if column in columns_to_quote:
                line += f'"{column}"'
            else:
                line += f'{column}'


            if self.table.types:
                line += " " + self.table.types[i]
            else:
                line += " xxx"

            if self.table.nullables:
                if self.table.nullables[i]:
                    line += f" {self.table.nullables[i]}"
            else:
                line += " xxx"

            if column in ['record_created', 'record_updated']:
                line += " DEFAULT now()"

            self.lines.append(line)

    def tableIndexes(self):
        """DDL indexes."""

        if self.table.schema in ['staging', 'tmp']: return
        if not self.table.indexes: return

        # remove indexes that are primary keys
        i = 1
        for index in self.table.indexes:
            if not index in self.table.primary_keys:
                if ',' in index:
                    if index.count(',') < 2:
                        # include col names
                        col_names = index.replace(' ', '').replace(',', '_')
                    else:
                        col_names = "composite" + str(i)
                        i += 1
                    self.lines.append(f"CREATE INDEX {self.table.table_name}_{col_names}_ix ON {self.table.schema}.{self.table.table_name} USING btree ({index});")
                else:
                    self.lines.append(f"CREATE INDEX {self.table.table_name}_{index}_ix ON {self.table.schema}.{self.table.table_name} USING btree ({index});")
        self.lines.append("")

    def tableDescription(self):
        """DDL table definition."""

        if self.table.schema in ['staging', 'tmp']: return

        self.lines.extend([
            f"-- Comments --;",
            f"COMMENT ON TABLE {self.table.schema}.{self.table.table_name} IS 'Description: {self.table.table_description}.",
            f"Subject Area: {self.subject_area}.",
            f"Updated: {self.schedule}.",
            f"{datetime.datetime.today().strftime('%Y-%m-%d')} - {self.user} - Created the table.';",
            f""
        ])

    def columnDescriptions(self, table=True):
        """DDL column descriptions."""

        if self.table.schema in ['staging', 'tmp']: return

        if table:
            object = self.table
            name = object.table_name
        else:
            object = self.view
            name = object.view_name

        added = False

        for i, column in enumerate(object.columns):
            desc = object.descriptions[i].replace("'","''")
            if desc:
                added = True
                self.lines.append(f"COMMENT ON COLUMN {object.schema}.{name}.{column} IS '{desc}.';")

        if added:
            self.lines.append("")

    def addToTableParameters(self):
        """Add table to adm.table_parameters."""

        if self.table.schema in ['staging', 'tmp']: return

        if self.table.import_method == "full": return

        self.lines.extend([
            f"-- Table parameters --;",
            f"INSERT INTO adm.table_parameters(table_name, increment_from, overlap) values('{self.table.schema}.{self.table.table_name}', '1997-12-21', '0 minutes');",
            f"",
        ])

    def defaultTablePermissions(self):
        """Default permissions for a table."""

        if self.table.schema == 'datamarts':
            role_schema = 'dtm'
        else:
            role_schema = self.table.schema

        self.lines.extend([
            f"-- Permissions --;",
            f"ALTER TABLE {self.table.schema}.{self.table.table_name} OWNER to role_master;",
        ])

        if not self.table.schema == "tmp":
            self.lines.append(f"GRANT SELECT ON {self.table.schema}.{self.table.table_name} TO role_select_{role_schema};")

        self.lines.append("")

        self.drop.append(f"-- DELETE FROM adm.table_parameters WHERE \"table_name\" = '{self.table.schema}.{self.table.table_name}';")

    def buildView(self):
        """DDL for a view."""

        self.header(f'Create view in {self.view.schema} schema')

        if self.view.materialized:
            text = "MATERIALIZED "
        else:
            text = ""

        self.lines.extend([
            f"CREATE {text}VIEW",
            f"    {self.view.schema}.{self.view.view_name}",
            f"AS SELECT",
        ])

        self.addColumnsView()

        self.lines.append("FROM")

        for i, (_, dict_) in enumerate(self.view.sources.items()):
            alias = dict_["alias"]
            table_name = dict_["source_table"]
            if i == 0:
                self.lines.append(f"    {table_name} {alias}")
                first_alias = alias
            else:
                self.lines.extend([
                    f"LEFT JOIN",
                    f"    {table_name} {alias} ON {alias}.xxx = {first_alias}.xxx",
                ])

        self.lines.append(";")

        self.drop.append(f"-- DROP {text}VIEW IF EXISTS {self.view.schema}.{self.view.view_name};")

    def addColumnsView(self):
        """Add columns to view."""

        columns = self.view.columns
        aliases = self.view.source_alias
        source_columns = self.view.source_columns

        for i, column in enumerate(columns):

            alias = 'xxx'

            if i > 0:
                space = "    , "
            else:
                space = "    "

            # get alias
            if aliases and aliases[i]:
                alias = aliases[i]

            # use different column name
            if (source_columns and source_columns[i]) and source_columns[i] != columns[i]:
                self.lines.append(f"{space}{alias}.{source_columns[i]} AS {column}")
            else:
                self.lines.append(f"{space}{alias}.{column}")

    def viewDescription(self):
        """DDL table definition."""

        if self.view.materialized:
            text = "MATERIALIZED "
        else:
            text = ""

        self.lines.extend([
            f"-- Comments --;",
            f"COMMENT ON {text}VIEW {self.view.schema}.{self.view.view_name} IS 'Description: {self.table.table_description}.",
            f"Subject Area: {self.subject_area}.",
            f"Updated: {self.schedule}.",
            f"{datetime.datetime.today().strftime('%Y-%m-%d')} - {self.user} - Created the {text.lower()}view.';",
            f""
        ])

    def defaultViewPermissions(self):
        """Default permissions for a view."""

        if self.view.materialized:
            text = "MATERIALIZED "
        else:
            text = ""

        self.lines.extend([
            f"-- Permissions --;",
            f"ALTER {text}VIEW {self.view.schema}.{self.view.view_name} OWNER TO role_master;",
            f"GRANT SELECT ON {self.view.schema}.{self.view.view_name} TO role_select_{self.view.schema};",
            f""
        ])

    def header(self, text):
        """Section header."""

        n = 65
        line = '-' * n
        s = n - len(text)
        h = f"{' '*math.ceil(s/2)}{text}{' '*math.floor(s/2)}"

        self.lines.extend([
            f"/* {line} */",
            f"/* {h} */",
            f"/* {line} */",
        ])

    def writeDrop(self):
        """Write commented out drop/delete lines for debugging purposes."""

        self.lines.append("-- Debugging lines --;")
        for line in reversed(self.drop):
            self.lines.append(line)