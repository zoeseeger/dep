import os
import re
import sys
import json
import copy
import shutil
from collections import OrderedDict

from src.functions import makeOutputDirectory, findFirstFile, doc2Html, doc2HtmlWin, readHtmlAndHeatSoupWin, readHtmlAndHeatSoup, getUnderHeaderHtml, getTableUnderHeaderHtml
from src.table import Table, View


class Options:
    """Handles options for run dependent vars."""

    def __init__(self, output_dir, debug=False):
        """Default user dict used if no json input file or missing keys."""

        self.jira_id = "GID-xxxx"
        self.action = "new_table" # new_table or import
        self.output_dir = output_dir

        # strings from confluence
        self.user = "xxx"
        self.user_email = "xxx.xxx@mecca.com.au"
        self.description = "xxx description"
        self.subject_area = "xxx business/function"
        self.schedule = "xxx refresh frequency"

        # specific for new_table
        self.staging_table = False

        # defined in subsequent functions
        self.input_file = None
        self.ddl_file = None
        self.dag_file = None
        self.doc_file = None
        self.confluence_doc = None
        self.temp_file = None

        # table data - read from confluence/json
        self.table_structures = []
        self.view_structures = []

        # intermediate data
        self.soup = None
        self.json_data = None

        self.debug = debug

    def convertDocToHtmlSoup(self, doc_file):
        """Find file with doc extension and convert to HTML."""

        # check extension
        if not doc_file.endswith('.doc'):
            sys.exit("Confluence document must be of type '.doc'")

        # get first doc file
        self.confluence_doc = findFirstFile(doc_file)

        # convert to html
        if self.confluence_doc:
            self.temp_file = doc2HtmlWin(self.confluence_doc, self.debug)
            self.soup = readHtmlAndHeatSoupWin(self.temp_file)
            if not self.debug:
                os.remove(self.temp_file)
                tmp_folder = self.temp_file.replace('.html', '_files')
                if os.path.exists(tmp_folder):
                    shutil.rmtree(tmp_folder)

        # convert to html
        # if self.confluence_doc:
        #     self.temp_file = doc2Html(self.confluence_doc, self.debug)
        #     self.soup = readHtmlAndHeatSoup(self.temp_file)
            # from bs4 import BeautifulSoup
            # self.soup = BeautifulSoup(html, "html.parser")

    def parseHtml(self):
        """Get vars from HTML."""

        if not self.confluence_doc: return

        heading = self.soup.h1.text.strip().lower()

        # action & schema and table name
        if heading.startswith("sys_"):
            self.action = "import"
            schema = heading.split()[0] # first word should be schema name
        else:
            self.action = "new_table"
            # dtm_table_name or dwh.table_name
            if '.' in heading:
                schema, table_name = heading.split('.')
            elif heading.startswith('dtm_'):
                schema, table_name = 'datamarts', heading
            else:
                sys.exit(f'Could not determine table schema from confluence title: {heading}.')

        # headers
        headers = self.soup.find_all(['h1', 'h2'])

        # table structures and top level vars
        for head in headers:

            head_text = head.text.lower().replace('\n', ' ').replace('  ', ' ').strip()

            ### new_table/import
            if head_text == "overview":

                overview = ' '.join(getUnderHeaderHtml(head))

                if overview:
                    self.description = overview.split('. ')[0]

                try:
                    p = re.compile("Working in (.*) I want")
                    self.subject_area = p.search(overview).group(1).strip().title()
                except AttributeError:
                    pass

            elif head_text == "status":
                self.jira_id = getUnderHeaderHtml(head)[0].split()[0]

            elif head_text == "created by":
                try:
                    creator = getUnderHeaderHtml(head)[0].replace(')', '')
                    self.user, self.user_email = creator.split(' (')
                    self.user = self.user.lower().replace(' ','')
                except IndexError:
                    pass

            ### new_table
            elif self.action == "new_table":

                if head_text == "update method":
                    update_method = getUnderHeaderHtml(head)
                    for line in update_method:
                        if "Schedule" in line:
                            self.schedule = line.split(':')[1].strip()

                elif head_text == "dfd":
                    text = getUnderHeaderHtml(head)
                    for line in text:
                        if "staging" in line:
                            self.staging_table = True

                elif head_text == "table structure":
                    self.table_structures.append(Table(schema, table_name))
                    table_header, table_structure = getTableUnderHeaderHtml(head)
                    print(f"Items found for TABLE {schema}.{table_name}:")
                    self.table_structures[-1].tableStructureToAttributes(table_header, table_structure)

                elif head_text == "view structure":
                    self.view_structures.append(View(schema, table_name))
                    view_header, view_structure = getTableUnderHeaderHtml(head)
                    print(f"Items found for VIEW {schema}.{table_name}:")
                    self.view_structures[-1].viewDataToDict(view_header, view_structure)

                elif head_text == "materialized view structure":
                    self.view_structures.append(View(schema, table_name, materialized=True))
                    view_header, view_structure = getTableUnderHeaderHtml(head)
                    print(f"Items found for MATERIALIZED VIEW {schema}.{table_name}:")
                    self.view_structures[-1].viewDataToDict(view_header, view_structure)

            elif self.action == "import":

                if head_text == "import method":

                    # schedule
                    for line in getUnderHeaderHtml(head):
                        if 'Schedule' in line:
                            self.schedule = line.replace('Schedule', '').replace(':', '').strip()
                            break

                # table header
                elif f"{schema}." in head_text:

                    # new table
                    schema, table_name = head_text.split('.')
                    self.table_structures.append(Table(schema, table_name))

                    # for each sibling
                    for sib in head.find_next_siblings():

                        sib_text = sib.text.lower().replace('\n', ' ').replace('  ', ' ').strip()

                        if sib_text == "description":
                            table_description = ' '.join(getUnderHeaderHtml(sib)).split('. ')[0]
                            if table_description.endswith('.'):
                                table_description = table_description[:-1]
                            self.table_structures[-1].updateAttribute("table_description", table_description)

                        elif sib_text == "granularity":
                            self.table_structures[-1].updateAttribute("primary_keys", getUnderHeaderHtml(sib))

                        elif sib_text == "business key":
                            self.table_structures[-1].updateAttribute("business_keys", getUnderHeaderHtml(sib))

                        if sib_text == "indexes":
                            self.table_structures[-1].updateAttribute("indexes", getUnderHeaderHtml(sib))

                        elif sib_text in ("foreign keys", "foreign key"):
                            self.table_structures[-1].updateAttribute("foreign_keys", getUnderHeaderHtml(sib))

                        elif sib_text == "table structure":
                            table_header, table_structure = getTableUnderHeaderHtml(sib)
                            print(f"Items found for TABLE {head.text}:")
                            self.table_structures[-1].tableStructureToAttributes(table_header, table_structure)

                        if sib.name in ("h1", "h2"):
                            break

        # primary keys and indexes - need tables initiated
        for head in headers:

            head_text = head.text.lower().replace('\n', ' ').replace('  ', ' ').strip()

            if self.action == "new_table":

                if head_text == "granularity" or head_text == "primary keys":
                    self.table_structures[-1].updateAttribute("primary_keys", getUnderHeaderHtml(head))

                elif head_text == "indexes" or head_text == "index":
                    self.table_structures[-1].updateAttribute("indexes", getUnderHeaderHtml(head))

            elif self.action == "import":

                if head_text == "import method":

                    # import method from table
                    table_header, table_structure = getTableUnderHeaderHtml(head)
                    self.importMethodToDict(table_header, table_structure)

        # set table description as self.description else for import is the schema description
        if self.action == "new_table":
            self.table_structures[-1].updateAttribute("table_description", self.description)

    def importMethodToDict(self, header: list, structure: list):
        """Add import method to table dicts."""

        # column positions
        name_pos = None
        import_pos = None
        for i, header in enumerate(header):
            if "method" in header.lower().strip():
                import_pos = i
            elif "table name" in header.lower().strip():
                name_pos = i

        if name_pos is None:
            print("Could not locate `name` column in confluence doc. continuing . . .")
            return

        if import_pos is None:
            print("Could not locate import `method` column in `import method` table in confluence doc. continuing . . .")
            return

        # iterate through list of lists
        for i in range(len(structure)):

            name = structure[i][name_pos]

            # find table dict
            for table_obj in self.table_structures:
                if table_obj.table_name == name:
                    table_obj.updateAttribute("import_method", structure[i][import_pos].lower().replace("load", "").strip())

    def defineVarsFromInput(self):
        """Depending on options dict, determine names of other vals."""

        # abide by naming convensions
        self.jira_id = self.jira_id.strip().replace(' ', '').upper()

        if self.table_structures:
            schema = self.table_structures[-1].schema
            table_name = self.table_structures[-1].table_name
        else:
            schema = 'xxx_schema'
            table_name = 'xxx_table_name'

        if self.action == "new_table":
            if schema == 'datamarts':
                file_name_base = table_name
            else:
                file_name_base = f"{schema}_{table_name}"
            self.dag_file = f"{file_name_base}.py"

        elif self.action == "import":
            file_name_base = schema
            self.dag_file = f"import_{file_name_base}.py"

        # file names
        self.ddl_file = f"{self.jira_id}_{file_name_base}_ddl.sql"
        self.doc_file = f"Unit-Test-{self.jira_id}_{file_name_base}.md"

    def readInputJson(self, input_file:str):
        """Get dict from json input if exists, else return empty dict."""

        # read input
        try:
            with open(input_file, 'r') as f:
                self.json_data = json.load(f)
            self.input_file = input_file

        # failed to read input
        except FileNotFoundError:
            print(f"Could not find json input file ({input_file}) in current directory. continuing . . .")
            self.input_file = None

        except json.decoder.JSONDecodeError as e:
            sys.exit("Expected input file as type json. Could not read file.\n    exiting . . .")

    def updateOptionsFromJson(self):
        """Overwrite default options with user options for those that exist."""

        if not self.input_file: return

        self.jira_id 		    = self.json_data.get("jira_id") or self.jira_id
        self.action             = self.json_data.get("action") or self.action
        self.output_dir         = self.json_data.get("output_dir") or self.output_dir
        self.user 			    = self.json_data.get("user") or self.user
        self.user_email 	    = self.json_data.get("user_email") or self.user_email
        self.description        = self.json_data.get("description") or self.description
        self.subject_area       = self.json_data.get("subject_area") or self.subject_area
        self.schedule           = self.json_data.get("schedule") or self.schedule
        self.staging_table 	    = self.json_data.get("staging_table") or self.staging_table
        self.ddl_file 	        = self.json_data.get("ddl_file") or self.ddl_file
        self.dag_file 	        = self.json_data.get("dag_file") or self.dag_file
        self.doc_file 	        = self.json_data.get("doc_file") or self.doc_file
        self.confluence_doc 	= self.json_data.get("confluence_doc") or self.confluence_doc
        self.debug 	            = self.json_data.get("debug") or self.debug

        # tables
        if self.json_data.get("table_structures"):
            self.table_structures = []
            for table in self.json_data.get("table_structures"):
                try:
                    self.table_structures.append(Table(
                        schema = table["schema"],
                        table_name = table["table_name"],
                        import_method = table["import_method"],
                        columns = table["columns"],
                        types = table["types"],
                        nullables = table["nullables"],
                        descriptions = table["descriptions"],
                        source_columns = table["source_columns"],
                        source_alias = table["source_alias"],
                        sources = OrderedDict(table["sources"]),
                        indexes = table.get("indexes", []),
                        primary_keys = table.get("primary_keys", []),
                        foreign_keys = table.get("foreign_keys", []),
                        table_description = table.get("table_description"),
                        business_keys = table.get("business_keys"),
                    ))
                except Exception as e:
                    sys.exit(f"Table field missing from input file. exiting . . .\n{e}")

        # views
        if self.json_data.get("view_structures"):
            self.view_structures = []
            for view in self.json_data.get("view_structures"):
                try:
                    self.view_structures.append(View(
                        schema = view["schema"],
                        foundation_table_name = view["foundation_table_name"],
                        view_name = view["view_name"],
                        columns = view["columns"],
                        descriptions = view["descriptions"],
                        source_columns = view["source_columns"],
                        source_alias = view["source_alias"],
                        sources = view["sources"],
                        materialized = view.get("materialized", False),
                    ))
                except Exception as e:
                    sys.exit(f"View field missing from input file. exiting . . .\n{e}")

    def checkInput(self):
        """Check at least one input given."""

        if not self.confluence_doc and not self.input_file:
            print("")
            sys.exit("Could not find either confluence or json file. exiting . . .")

        if not self.table_structures:
            print("")
            sys.exit("Table structures must be defined either in a json or confluence file. exiting . . .")

    def stagingTableStructure(self):
        """Extract staging table from main table and add to table_structures."""

        if not self.staging_table or self.action == "import": return

        stg_table = copy.deepcopy(self.table_structures[-1])
        main_table = copy.deepcopy(self.table_structures[-1])

        stg_table.convertStructureToStgTable()
        main_table.convertStructureToMainFromStg(stg_table.schema_table)

        # new tables
        self.table_structures = [stg_table, main_table]

    def tmpTableStructure(self):
        """Extract tmp tables from sys tables and add to table_structures."""

        if self.action == "new_table": return

        new_tables = []
        for table in self.table_structures:

            if "fivetran" in table.import_method:
                new_tables.append(table)
                continue

            tmp_table = copy.deepcopy(table)
            main_table = copy.deepcopy(table)

            tmp_table.convertStructureToTmpTable()
            main_table.convertStructureToMainFromTmp(tmp_table.schema_table)

            new_tables.extend([tmp_table, main_table])

        # new tables
        self.table_structures = new_tables

    def writeVarsToFile(self):
        """Write the class attr values to json."""

        # remove no longer needed vars - also breaks deep copy
        del self.soup
        del self.json_data
        del self.temp_file
        del self.input_file
        del self.confluence_doc

        # class_dict = vars(self)
        class_dict = copy.deepcopy(vars(self))

        # convert table classes to dicts
        for i in range(len(class_dict["table_structures"])):
            class_dict["table_structures"][i] = vars(class_dict["table_structures"][i])
            class_dict["table_structures"][i].pop("base_schema_table")
            class_dict["table_structures"][i].pop("final_table")
        for i in range(len(class_dict["view_structures"])):
            class_dict["view_structures"][i] = vars(class_dict["view_structures"][i])

        i = 0
        suffix = ""
        while os.path.exists(f"{self.output_dir}/vars{suffix}.json"):
            i += 1
            suffix = i

        if self.debug:
            filename = "vars.json"
        else:
            filename = f"vars{suffix}.json"

        makeOutputDirectory(f"{self.output_dir}")
        with open(f"{self.output_dir}/{filename}", "w") as w:
            json.dump(class_dict, w, indent=4)
