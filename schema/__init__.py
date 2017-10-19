import ast
from collections import defaultdict, deque
from .utils import emap, reverse_path, get_value, get_function_name,\
    get_function_arg

code = """
from orator.migrations import Migration
class CreateTableUsers(Migration):
    def up(self):
        with self.schema.create('users') as table:
            table.increments('id')
            table.timestamps()
            table.foreign('ad_place_id').reference('id').on('ad_places').on_delete('cascade')
        with self.schema.create('finends') as table:
            table.increments('id')
            table.integer('user_id')
            table.timestamps()
    def down(self):
        self.schema.drop('users')
"""

COMMAND = {
    'big_increments': '{} BIGINT AUTO_INCREMENT',
    'big_integer': '{} BIGINT',
    'binary': '{} BLOB',
    'boolean': '{} BOOLEAN',
    'char': '{} CHAR',
    'date': '{} DATE',
    'datetime': '{} DATETIME',
    'decimal': '{} DECIMAL',
    'double': '{} DOUBLE',
    'enum': '{} ENUM',
    'float': '{} FLOAT',
    'increments': '{} INT AUTO_INCREMENT PRIMARY_KEY',
    'integer': '{} INTEGER',
    'json': '{} JSON',
    'long_text': '{} LONGTEXT',
    'medium_integer': '{} MEDIUMINT',
    'medium_text': '{} MEDIUMTEXT',
    'morphs': '{}_id INTEGER,\n {}_type VARCHAR',
    'nullable_timestamps': '{} TIMESTAMPS',
    'small_integer': '{} SMALLINT',
    'soft_deletes': '',
    'string': '{} VARCHAR({})',
    'text': '{} TEXT',
    'time': '{} TIME',
    'timestamp': '{} TIMESTAMPS',
    'foreign': '{} FOREIGH KEY'
}

OPTIONAL = {
    'nullable': '',
    'default': 'DEFAULT {}',
    'unsigned': '',
}

collection = []

default_dict = lambda: defaultdict(default_dict)
schema = default_dict()

# TODO
# put in one class
ACTION = type('Action', (object,), {
    'change': 'change',
    'rename_column': 'rename_column',
    'drop_column': 'drop_column',
    'drop_foreign': 'drop_foreign'
})

tmp_test = []

CONSTRAINT = {
    'nullable': lambda field: field.set_null(),
    'unique': lambda field: field.set_unique(),
    'reference': lambda field, *args: field.set_reference(*args),
    'on': lambda field, *args: field.set_reference(*args),
    'unsigned': lambda field: field.set_unsigned(),
    'on_delete': lambda field, *args: field.set_on_delete(),
    'default': lambda field, *args: field.set_default(*args),
}


class Field:

    def __init__(self, name, ttype):
        # name is a redundant field
        self._name = name
        self._ttype = ttype

    def reset(self):
        self._ttype = None
        self._unique = False
        self._nullable = False
        self._default = None

    def set_name(self, new_name):
        self._name = new_name

    def set_type(self, new_type):
        self._ttype = new_type

    def set_null(self):
        self._nullable = True

    def set_unique(self):
        self._unique = True

    def remove_unique(self):
        self._unique = False

    def set_default(self, value):
        self._default = value

    def remove_default(self):
        self._default = None

    def __str__(self):
        obj_info = super().__str__()
        return '<{}, __dict__={}>'.format(
            obj_info.strip('<>'), self.__dict__)

    def _get_meta(self):
        meta = defaultdict(lambda: '')
        meta.update(self.__dict__)
        return meta

    def __dump__(self):
        meta = self._get_meta()
        tmpl = ["table.{_ttype}('{_name}')".format_map(meta)]
        meta.pop('_ttype')
        meta.pop('_name')
        for k in filter(lambda s: s.startswith('_') and not s.startswith('__'),
                        meta.keys()):
            v = meta[k]
            if v:
                tmpl.append('.{}()'.format(k.lstrip('_')))
        return ''.join(tmpl)
        # return "table.{_ttype}('{_name}').{_unique}()".format_map(meta)


def handle_field_constraint(field, nodes):
    while len(nodes) > 0:
        node = nodes.pop(0)
        CONSTRAINT[get_function_name(node)](field, *[get_value(x) for x in node.args])


def handle_table_constraint(table_name, fields, constraint_type):
    pass


def to_list(arg):
    if isinstance(arg, (tuple, list)):
        return list(arg)
    if isinstance(arg, (bytes, str)):
        return [arg]


def handle_index(table_name, node):
    args = node.args
    assert len(args) == 1
    cols = get_value(args.pop(0))
    ttype = node.func.attr

    if ttype.startswith('drop_'):
        do_drop_index(table_name, ttype.lstrip('drop_'), cols)
    else:
        do_add_index(table_name, ttype, cols)


def do_drop_index(table_name, ttype, cols):
    print(schema[table_name]['_ORM_META'])
    schema[table_name]['_ORM_META'].pop(cols)


def do_add_index(table_name, ttype, cols):
    index_name = '_'.join((table_name, *to_list(cols), ttype))
    schema[table_name]['_ORM_META'][index_name] = ttype


def do_cahnge_col(table_name, node):
    """handle like following:
       table.string('name', 50).nullable().change()
    """
    path = reverse_path(node)
    tmp_test.append(path.copy())
    assert len(path) > 0

    assert path.pop(0).id == 'table'

    field = path.pop(0)
    assert len(field.args) > 0
    field_name, *args = field.args
    field_name = get_value(field_name)
    field_type = get_function_name(field)
    assert field_name in schema[table_name]
    field_obj = schema[table_name][field_name]
    field_obj.reset()
    assert isinstance(field_obj, Field)
    field_obj.set_type(field_type)
    handle_field_constraint(field_obj, path)


def do_drop_col(table_name, cols):
    for col in cols:
        assert get_value(col) in schema[table_name]
        schema[table_name].pop(get_value(col))


def do_rename_col(table_name, old, new):
    old_name, new_name = map(get_value, (old, new))
    field = schema[table_name].pop(old_name)
    field.set_name(new_name)
    schema[table_name][new_name] = field


def do_add_col(table_name, obj):
    path = reverse_path(obj)

    assert path.popleft().id == 'table'

    field = path.popleft()
    field_type = get_function_name(field)
    # Really here? ugly
    field_name, *args = field.args or [ast.Str('timestamps')]
    field_name = get_value(field_name)
    assert field_name not in schema[table_name]
    field_obj = schema[table_name][field_name] = FieldFactory.create(
        field_name, field_type)
    handle_field_constraint(field_obj, path)

def _generate_table(table_name, exprs):
    """generate SQL table representation"""
    for expr in exprs:
        obj = expr.value
        assert isinstance(obj, ast.Call)
        path = reverse_path(obj)
        assert path.popleft().id == 'table'
        collection.append(path)

        node = path.popleft()
        field_type = get_function_name(node)
        field_name = get_function_arg(node)

        field = FieldFactory.create() 


        if is_index:
            handle_index(table_name, obj)
            continue

        # do some action
        if obj.func.attr == ACTION.change:
            do_cahnge_col(table_name, obj.func.value)
        elif obj.func.attr == ACTION.rename_column:
            assert len(obj.args) == 2
            do_rename_col(table_name, *obj.args)
        elif obj.func.attr == ACTION.drop_column:
            do_drop_col(table_name, obj.args)
        else:
            do_add_col(table_name, obj)


class Walker(ast.NodeVisitor):
    """visit the ast node and generate a table object"""


    def visit_FunctionDef(self, node):
        """visit python functon definition"""
        # only parse the up function
        if node.name != 'up':
            return
        for ast_with in node.body:
            assert isinstance(ast_with, ast.With)
            for with_item in ast_with.items:
                assert isinstance(with_item, ast.withitem)
                assert len(with_item.context_expr.args) == 1
                table_name = with_item.context_expr.args[0].s
                _generate_table(table_name, ast_with.body.copy())


def flatten(struct):
    result = []

    def wrapper(struct):
        if isinstance(struct, tuple):
            result.append(struct[0])
            wrapper(struct[-1])
        elif isinstance(struct, list):
            for i in struct:
                wrapper(i)
        else:
            result.append(struct)
    wrapper(struct)
    return result


def dump():
    for table_name, table_field in schema.items():
        print("with self.schema.create('{}') as table:".format(table_name))
        for field_name, keyword in table_field.items():
            if field_name == '_ORM_META':
                for k, v in keyword.items():
                    temp = list(map(lambda x: "'" + x + "'",
                                    k.split('_')[1:-1]))
                    k = ','.join(temp)
                    if len(temp) > 1:
                        k = '[{}]'.format(k)
                    print('    ', 'table.{}({})'.format(v, k))
            else:
                print('    ', keyword.__dump__())
        print('\n')


if __name__ == '__main__':
    for code in (code,):
        ast_code = ast.parse(code)
        Walker().visit(ast_code)
    dump()
