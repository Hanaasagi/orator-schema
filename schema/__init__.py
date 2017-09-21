import ast
from collections import defaultdict


code = """
from orator.migrations import Migration


class CreateTableUsers(Migration):

    def up(self):
        with self.schema.create('users') as table:
            table.increments('id')
            table.timestamps()

        with self.schema.create('finends') as table:
            table.increments('id')
            table.integer('user_id')
            table.timestamps()

    def down(self):
        self.schema.drop('users')
"""



ast_code = ast.parse(code)


gen = ast.walk(ast_code)

collection = []

schema = defaultdict(lambda: {})


class Builder(ast.NodeVisitor):

    def _generate_table(self, table_name, exprs):
        collection.append(exprs)
        for expr in exprs:
            field_type = expr.value.func.attr
            if field_type == 'timestamps':
                if len(expr.value.args):
                    assert len(expr.value.args) == 1
                    schema[table_name][expr.value.args[0]] = field_type
                else:
                    schema[table_name]['created_at'] = field_type
                    schema[table_name]['updated_at'] = field_type
                continue
            assert len(expr.value.args) == 1
            field = expr.value.args[0].s
            schema[table_name][field]= field_type

    def visit_FunctionDef(self, node):
        if node.name == 'up':
            for ast_with in node.body:
                assert isinstance(ast_with, ast.With)
                for with_item in ast_with.items:
                    assert isinstance(with_item, ast.withitem)
                    assert len(with_item.context_expr.args) == 1
                    table_name = with_item.context_expr.args[0].s
                    self._generate_table(table_name, ast_with.body.copy())


if __name__ == '__main__':
    Builder().visit(ast_code)
    import json
    print(json.dumps(schema, indent=2, sort_keys=True))

