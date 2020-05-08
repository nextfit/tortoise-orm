from dataclasses import dataclass, field
from hashlib import sha256
from typing import List, Set


class BaseSchemaGenerator:
    DIALECT = "sql"
    TABLE_CREATE_TEMPLATE = 'CREATE TABLE {exists}"{table_name}" ({columns}){extra}{comment};'
    COLUMN_TEMPLATE = '"{name}" {type} {nullable} {unique}{primary}{comment}'
    INDEX_CREATE_TEMPLATE = 'CREATE INDEX {exists}"{index_name}" ON "{table_name}" ({columns});'
    UNIQUE_CONSTRAINT_CREATE_TEMPLATE = 'CONSTRAINT "{index_name}" UNIQUE ({columns})'
    GENERATED_PK_TEMPLATE = '"{column_name}" {generated_sql}{comment}'
    FK_TEMPLATE = ' REFERENCES "{table}" ("{related_column}") ON DELETE {on_delete}{comment}'
    M2M_TABLE_TEMPLATE = (
        'CREATE TABLE {exists}"{table_name}" (\n'
        '    "{backward_key}" {backward_type} NOT NULL REFERENCES "{backward_table}"'
        ' ("{backward_related_column}") ON DELETE CASCADE,\n'
        '    "{forward_key}" {forward_type} NOT NULL REFERENCES "{forward_table}"'
        ' ("{forward_related_column}") ON DELETE CASCADE\n'
        "){extra}{comment};"
    )

    def __init__(self, client) -> None:
        self.client = client

    def _create_column_string(
        self, db_column: str, column_type: str, nullable: str, unique: str, is_primary_key: bool, comment: str
    ) -> str:
        # children can override this function to customize their sql queries

        return self.COLUMN_TEMPLATE.format(
            name=db_column,
            type=column_type,
            nullable=nullable,
            unique="" if is_primary_key else unique,
            comment=comment if self.client.capabilities.inline_comment else "",
            primary=" PRIMARY KEY" if is_primary_key else "",
        ).strip()

    def _create_fk_string(
        self,
        constraint_name: str,
        db_column: str,
        table: str,
        related_column: str,
        on_delete: str,
        comment: str,
    ) -> str:
        return self.FK_TEMPLATE.format(
            db_column=db_column, table=table, related_column=related_column, on_delete=on_delete, comment=comment
        )

    def _table_comment_generator(self, table: str, comment: str) -> str:
        # Databases have their own way of supporting comments for table level
        # needs to be implemented for each supported client
        raise NotImplementedError()  # pragma: nocoverage

    def _column_comment_generator(self, table: str, column: str, comment: str) -> str:
        # Databases have their own way of supporting comments for column level
        # needs to be implemented for each supported client
        raise NotImplementedError()  # pragma: nocoverage

    def _post_table_hook(self) -> str:
        # This method provides a mechanism where you can perform a set of
        # operation on the database table after  it's initialized. This method
        # by default does nothing. If need be, it can be over-written
        return ""

    @staticmethod
    def _get_escape_translation_table() -> List[str]:
        """escape sequence taken based on definition provided by PostgreSQL and MySQL"""
        _escape_table = [chr(x) for x in range(128)]
        _escape_table[0] = "\\0"
        _escape_table[ord("\\")] = "\\\\"
        _escape_table[ord("\n")] = "\\n"
        _escape_table[ord("\r")] = "\\r"
        _escape_table[ord("\032")] = "\\Z"
        _escape_table[ord('"')] = '\\"'
        _escape_table[ord("'")] = "\\'"
        return _escape_table

    def _escape_comment(self, comment: str) -> str:
        # This method provides a default method to escape comment strings as per
        # default standard as applied under mysql like database. This can be
        # overwritten if required to match the database specific escaping.
        return comment.translate(BaseSchemaGenerator._get_escape_translation_table())

    def _table_generate_extra(self, table: str) -> str:
        return ""

    def _get_inner_statements(self) -> List[str]:
        return []

    def quote(self, val: str) -> str:
        return f'"{val}"'

    @staticmethod
    def _make_hash(*args: str, length: int) -> str:
        # Hash a set of string values and get a digest of the given length.
        return sha256(";".join(args).encode("utf-8")).hexdigest()[:length]

    def _generate_index_name(self, prefix, model, column_names: List[str]) -> str:
        # NOTE: for compatibility, db_index name should not be longer than 30
        # characters (Oracle limit).
        # That's why we slice some of the strings here.
        table_name = model._meta.db_table
        index_name = "{}_{}_{}_{}".format(
            prefix,
            table_name[:11],
            column_names[0][:7],
            self._make_hash(table_name, *column_names, length=6),
        )
        return index_name

    def _generate_fk_name(self, from_table, from_column, to_table, to_column) -> str:
        # NOTE: for compatibility, db_index name should not be longer than 30
        # characters (Oracle limit).
        # That's why we slice some of the strings here.
        index_name = "fk_{f}_{t}_{h}".format(
            f=from_table[:8],
            t=to_table[:8],
            h=self._make_hash(from_table, from_column, to_table, to_column, length=8),
        )
        return index_name

    def _get_index_sql(self, model, column_names: List[str], safe: bool) -> str:
        return self.INDEX_CREATE_TEMPLATE.format(
            exists="IF NOT EXISTS " if safe else "",
            index_name=self._generate_index_name("idx", model, column_names),
            table_name=model._meta.db_table,
            columns=", ".join([self.quote(f) for f in column_names]),
        )

    def _get_unique_constraint_sql(self, model, column_names: List[str]) -> str:
        return self.UNIQUE_CONSTRAINT_CREATE_TEMPLATE.format(
            index_name=self._generate_index_name("uid", model, column_names),
            columns=", ".join([self.quote(f) for f in column_names]),
        )

    def get_table_sql(self, model, safe=True) -> dict:
        columns_to_create = []
        columns_with_index = []
        m2m_tables_to_create = []
        references = set()

        for field_name, column_name in model._meta.field_to_db_column_name_map.items():
            field_object = model._meta.fields_map[field_name]
            comment = (
                self._column_comment_generator(
                    table=model._meta.db_table, column=column_name, comment=field_object.description)
                if field_object.description
                else ""
            )

            # TODO: PK generation needs to move out of schema generator.
            if field_object.primary_key:
                if field_object.generated:
                    generated_sql = field_object.get_for_dialect(self.DIALECT, "GENERATED_SQL")
                    if generated_sql:  # pragma: nobranch
                        columns_to_create.append(
                            self.GENERATED_PK_TEMPLATE.format(
                                column_name=column_name, generated_sql=generated_sql, comment=comment,
                            )
                        )
                        continue

            nullable = "NOT NULL" if not field_object.null else ""
            unique = "UNIQUE" if field_object.unique else ""

            if field_object.reference:
                comment = (
                    self._column_comment_generator(
                        table=model._meta.db_table,
                        column=column_name,
                        comment=field_object.reference.description,
                    )
                    if field_object.reference.description
                    else ""
                )

                field_creation_string = self._create_column_string(
                    db_column=column_name,
                    column_type=field_object.get_for_dialect(self.DIALECT, "SQL_TYPE"),
                    nullable=nullable,
                    unique=unique,
                    is_primary_key=field_object.primary_key,
                    comment="",
                ) + self._create_fk_string(
                    constraint_name=self._generate_fk_name(
                        model._meta.db_table,
                        column_name,
                        field_object.reference.remote_model._meta.db_table,
                        field_object.reference.remote_model._meta.pk_db_column,
                    ),
                    db_column=column_name,
                    table=field_object.reference.remote_model._meta.db_table,
                    related_column=field_object.reference.remote_model._meta.pk_db_column,
                    on_delete=field_object.reference.on_delete,
                    comment=comment,
                )

                references.add(field_object.reference.remote_model._meta.db_table)

            else:
                field_creation_string = self._create_column_string(
                    db_column=column_name,
                    column_type=field_object.get_for_dialect(self.DIALECT, "SQL_TYPE"),
                    nullable=nullable,
                    unique=unique,
                    is_primary_key=field_object.primary_key,
                    comment=comment,
                )

            columns_to_create.append(field_creation_string)

            if field_object.db_index and not field_object.primary_key:
                columns_with_index.append(column_name)

        if model._meta.unique_together:
            for unique_together_list in model._meta.unique_together:
                unique_together_to_create = []

                for field_name in unique_together_list:
                    field_object = model._meta.fields_map[field_name]
                    unique_together_to_create.append(field_object.db_column or field_name)

                columns_to_create.append(
                    self._get_unique_constraint_sql(model, unique_together_to_create)
                )

        # Indexes.
        _indexes = [
            self._get_index_sql(model, [column_name], safe=safe) for column_name in columns_with_index
        ]

        if model._meta.indexes:
            for indexes_list in model._meta.indexes:
                indexes_to_create = [
                    model._meta.fields_map[field_name].db_column or field_name for field_name in indexes_list
                ]

                _indexes.append(self._get_index_sql(model, indexes_to_create, safe=safe))

        indexes_create_strings = [val for val in list(dict.fromkeys(_indexes)) if val]

        columns_to_create.extend(self._get_inner_statements())

        table_columns_string = "\n    {}\n".format(",\n    ".join(columns_to_create))
        table_comment = (
            self._table_comment_generator(
                table=model._meta.db_table, comment=model._meta.table_description
            )
            if model._meta.table_description
            else ""
        )

        table_create_string = self.TABLE_CREATE_TEMPLATE.format(
            exists="IF NOT EXISTS " if safe else "",
            table_name=model._meta.db_table,
            columns=table_columns_string,
            comment=table_comment,
            extra=self._table_generate_extra(table=model._meta.db_table),
        )

        table_create_string = "\n".join([table_create_string, *indexes_create_strings])
        table_create_string += self._post_table_hook()

        from tortoise.fields import ManyToManyField
        for field in model._meta.fields_map.values():
            if isinstance(field, ManyToManyField) and not field.auto_created:
                m2m_create_string = self.M2M_TABLE_TEMPLATE.format(
                    exists="IF NOT EXISTS " if safe else "",
                    table_name=field.through,
                    backward_table=model._meta.db_table,
                    forward_table=field.remote_model._meta.db_table,
                    backward_related_column=model._meta.pk_db_column,
                    forward_related_column=field.remote_model._meta.pk_db_column,
                    backward_key=field.backward_key,
                    backward_type=model._meta.pk.get_for_dialect(self.DIALECT, "SQL_TYPE"),
                    forward_key=field.forward_key,
                    forward_type=field.remote_model._meta.pk.get_for_dialect(self.DIALECT, "SQL_TYPE"),
                    extra=self._table_generate_extra(table=field.through),
                    comment=
                        self._table_comment_generator(table=field.through, comment=field.description)
                        if field.description else "",
                )
                m2m_create_string += self._post_table_hook()
                m2m_tables_to_create.append(m2m_create_string)

        return {
            "db_table": model._meta.db_table,
            "model": model,
            "table_creation_string": table_create_string,
            "references": references,
            "m2m_tables": m2m_tables_to_create,
        }
