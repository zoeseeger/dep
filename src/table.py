import sys
from collections import OrderedDict


class Table:
    """Class for tables."""

    def __init__(
        self,
        schema:str,
        table_name:str,
        import_method:str="incremental", # "full" & soft delete OR "incremental" & upsert
        insert_method:str="upsert", # upsert or scd2
        columns:list=None,
        types:list=None,
        nullables:list=None,
        descriptions:list=None, # column descriptions
        source_columns:list=None,
        source_alias:list=None,
        sources:dict=None,
        indexes:list=None,
        primary_keys:list=None,
        foreign_keys:list=None,
        table_description:str="xxx table description",
        base_schema_table:str=None,
        final_table:bool=True
    ):
        """Table attributes."""

        self.schema = schema
        self.table_name = table_name
        self.schema_table = f"{schema}.{table_name}"
        self.import_method = import_method
        self.insert_method = insert_method
        self.table_description = table_description
        # not for user
        self.base_schema_table = base_schema_table
        self.final_table = final_table

        # lists length of columns not empty
        self.columns = columns or []
        self.types = types or []
        self.nullables = nullables or []
        self.descriptions = descriptions or []
        self.source_columns = source_columns or []
        self.source_alias = source_alias or []

        # can be any lenth or empty
        self.sources = sources or OrderedDict()
        self.indexes = indexes or []
        self.primary_keys = primary_keys or []
        self.foreign_keys = foreign_keys or []

    def updateAttribute(self, property, value):
        """Set attribute of class instance outside of class."""

        setattr(self, property, value)

    def tableStructureToAttributes(self, header:list, structure:list, main_source:str=None, view:bool=None):
        """Call static method structureDataToAttributes."""

        Table.structureDataToAttributes(self, header, structure, main_source, view)

    @staticmethod
    def structureDataToAttributes(
        class_obj:object,
        header:list,
        structure:list,
        main_source:str=None,
        view=False,
    ):
        """Move lists to attr dict and determine aliases."""

        # contents
        name_pos = None
        type_pos = None
        null_pos = None
        desc_pos = None
        src_pos = None
        for i, header in enumerate(header):
            if "type" in header.lower():
                type_pos = i
                print("    Data type")
            elif "null" in header.lower():
                null_pos = i
                print("    Nullable")
            elif "description" in header.lower():
                desc_pos = i
                print("    Description")
            elif "source" in header.lower():
                src_pos = i
                print("    Source")
            elif "name" in header.lower() or "column" in header.lower():
                name_pos = i
                print("    Column name")

        if name_pos is None:
            sys.exit(
                f"Could not locate `name` column in confluence doc for table {class_obj.schema_table}.\n    continuing . . ."
            )

        # iterate through list of lists
        for i in range(len(structure)):

            # print(class_obj.table_name, structure[i][name_pos])
            class_obj.columns.append(structure[i][name_pos])

            if not view:

                if type_pos is not None:
                    class_obj.types.append(structure[i][type_pos])
                else:
                    class_obj.types.append("")

                if null_pos is not None:
                    class_obj.nullables.append(structure[i][null_pos])
                else:
                    class_obj.nullables.append("")

            if desc_pos is not None:
                class_obj.descriptions.append(structure[i][desc_pos])
            else:
                class_obj.descriptions.append("")

            if src_pos is not None:

                # get column
                source_table = structure[i][src_pos]
                column = ""
                if "/" in source_table:
                    source_table, column = [val.strip() for val in structure[i][src_pos].split("/")]

                # fix shorthand
                if source_table == "*":
                    source_table = class_obj.schema + '.' + class_obj.foundation_table_name

            elif main_source:
                source_table = class_obj.foundation_table_name
                column = ""

            else:
                source_table = ""
                column = ""

            # alias
            alias = Table.getTableAliasAndAddToSources(class_obj, source_table)

            # if column == orig column save as none
            if column == structure[i][name_pos]:
                column = ""

            class_obj.source_columns.append(column)
            class_obj.source_alias.append(alias)

    @staticmethod
    def getTableAliasAndAddToSources(class_obj:object, source_table_index:str):
        """SQL alias for table and add source table to self.sources."""

        # empty cell in table
        if not source_table_index:
            return ""

        # if already in sources
        if source_table_index in class_obj.sources.keys():
            return class_obj.sources[source_table_index]["alias"]

        # index of source table
        idx = ""
        if "[" in source_table_index:
            source_table, idx = source_table_index.replace("]", "").split("[")
        elif "(" in source_table_index:
            source_table, idx = source_table_index.replace(")", "").split("(")
        else:
            source_table = source_table_index

        # split schema from table name
        if "." in source_table:
            schema_table = source_table
            source_table = source_table.split(".")[1]
        else:
            schema_table = source_table

        # alias
        orig_alias = "".join([c[0] for c in source_table.split("_")])
        alias_idx = orig_alias + idx

        # if not alias exists in dict

        if not alias_idx in [
            dict_.get("alias") for dict_ in class_obj.sources.values()
        ]:
            class_obj.sources[source_table_index] = {}
            class_obj.sources[source_table_index]["alias"] = alias_idx
            class_obj.sources[source_table_index]["source_table"] = schema_table
            alias = alias_idx

        # alias already used
        else:
            idx = 1
            alias = f"{orig_alias}{idx}"
            while source_table_index not in class_obj.sources.keys():

                alias = f"{orig_alias}{idx}"

                # add unique alias
                if not alias in [dict_.get("alias") for dict_ in class_obj.sources.values()]:
                    class_obj.sources[source_table_index] = {}
                    class_obj.sources[source_table_index]["alias"] = alias
                    class_obj.sources[source_table_index]["source_table"] = schema_table
                idx += 1

        return alias

    def convertStructureToStgTable(self):
        """Convert main table to staging table."""

        self.base_schema_table = self.schema_table
        self.schema = "staging"
        self.table_name = "stg_" + self.table_name
        self.schema_table = "staging." + self.table_name
        self.indexes = []
        self.primary_keys = []
        self.foreign_keys = []
        self.final_table = False

        for i in range(len(self.columns)-1, -1, -1):

            # set to empty
            self.nullables[i] = ""
            self.descriptions[i] = ""

            # remove rows for record updated & created
            if self.columns[i] in ("record_created", "record_updated", "record_deleted"):
                self.columns.pop(i)
                self.types.pop(i)
                self.nullables.pop(i)
                self.descriptions.pop(i)
                self.source_columns.pop(i)
                self.source_alias.pop(i)

    def convertStructureToMainFromStg(self, staging_schema_table:str):
        """Convert main table to have source as staging table."""

        # main table
        for i in range(len(self.columns)):

            self.source_columns[i] = ""
            self.source_alias[i] = "stg"
            self.sources = OrderedDict({staging_schema_table:
                {"alias": "stg", "source_table": staging_schema_table}
            })

    def convertStructureToTmpTable(self):
        """Convert main table to tmp table."""

        self.base_schema_table = self.schema_table

        # changed vars
        self.table_name = f"{self.schema.replace('sys_','')}_{self.table_name}"
        self.schema = "tmp"
        self.schema_table = f"{self.schema}.{self.table_name}"
        self.indexes = []
        self.primary_keys = []
        self.foreign_keys = []
        self.final_table = False

        for i in range(len(self.columns)-1, -1, -1):

            # set to empty
            self.nullables[i] = ""
            self.descriptions[i] = ""

            # remove rows for record updated & created
            if self.columns[i] in ("record_created", "record_updated", "record_deleted"):
                self.columns.pop(i)
                self.types.pop(i)
                self.nullables.pop(i)
                self.descriptions.pop(i)
                self.source_columns.pop(i)
                self.source_alias.pop(i)

    def convertStructureToMainFromTmp(self, tmp_schema_table:str):
        """Convert main table to have source as tmp table."""

        # main table
        for i in range(len(self.columns)):

            self.source_columns[i] = ""
            self.source_alias[i] = "tmp"
            self.sources = OrderedDict({tmp_schema_table:
                {"alias": "tmp", "source_table": tmp_schema_table}
            })


class View:
    """Class for tables."""

    def __init__(
        self,
        schema:str,
        foundation_table_name:str,
        view_name:str=None,
        columns:list=None,
        descriptions:list=None,
        source_columns:list=None,
        source_alias:list=None,
        sources:dict=None,
        materialized:bool=False
    ):
        """View attributes."""

        self.schema = schema
        self.view_name = view_name
        self.foundation_table_name = foundation_table_name
        self.materialized = materialized

        # lists length of columns not empty
        self.columns = columns or []
        self.descriptions = descriptions or []
        self.source_columns = source_columns or []
        self.source_alias = source_alias or []

        # any length or empty
        self.sources = sources or OrderedDict()

        # view name if not already given
        if not view_name:
            self.viewNameFromTable()

    def updateAttribute(self, property, value):
        """Set attributes of class instance outside of class."""

        setattr(self, property, value)

    def viewNameFromTable(self):
        """Set view name."""

        if self.materialized:
            self.view_name = f"{self.foundation_table_name}_mv"
        else:
            self.view_name = f"{self.foundation_table_name}_v"

    def viewDataToDict(self, view_header, view_structure):
        """Run Table function on view class."""

        Table.structureDataToAttributes(
            self, view_header, view_structure, self.foundation_table_name, view=True
        )
