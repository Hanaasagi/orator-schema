import ast
from collections import defaultdict

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

    def __init__(self, name, type_):
        # name is a redundant field
        self._name = name
        self._type_ = type_

    def reset(self):
        self._type_ = None
        self._unique = False
        self._nullable = False
        self._default = None

    def set_name(self, new_name):
        self._name = new_name

    def set_type(self, new_type):
        self._type_ = new_type

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
        tmpl = ["table.{_type_}('{_name}')".format_map(meta)]
        meta.pop('_type_')
        meta.pop('_name')
        for k in filter(lambda s: s.startswith('_') and not s.startswith('__'),
                        meta.keys()):
            v = meta[k]
            if v:
                tmpl.append('.{}()'.format(k.lstrip('_')))
        return ''.join(tmpl)
        # return "table.{_type_}('{_name}').{_unique}()".format_map(meta)


class IntegerField(Field):

    def set_unsigned(self):
        self._unsigned = True

    def reset(self):
        self._unsigned = False
        super().reset()


class SmallIntegerField(IntegerField):
    pass


class BigIntegerField(IntegerField):
    pass


class StringField(Field):

    def set_length(self, length):
        self._length = length


class TimestampsField(Field):

    def __dump__(self):
        meta = self._get_meta()
        tmpl = ["table.{_type_}()".format_map(meta)]
        meta.pop('_type_')
        meta.pop('_name')
        for k in filter(lambda s: s.startswith('_') and not s.startswith('__'),
                        meta.keys()):
            v = meta[k]
            if v:
                tmpl.append('.{}()'.format(k.lstrip('_')))
        return ''.join(tmpl)


class TimestampField(Field):
    pass


class ForeignField(Field):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ref_table = None
        self._ref_col = None
        self.others = None

    def set_reference(self, ref_col):
        self._ref_col = ref_col

    def set_on(self, ref_table):
        self._ref_table = ref_table

    def set_on_delete(self):
        self._on_delete = True


class FieldFactory:

    mapping = {
        'string': StringField,
        'foreign': ForeignField,
        'small_integer': SmallIntegerField,
        'timestamp': TimestampField,
        'timestamps': TimestampsField,
    }

    default = Field

    @classmethod
    def create(cls, name, type_, *args, **kwargs):
        kls = cls.mapping.get(type_, cls.default)
        return kls(name, type_, *args, **kwargs)


def reverse_path(node):
    path = [node]
    while isinstance(node, ast.Call):
        path.append(node.func.value)
        node = node.func.value
    return list(reversed(path))


def get_value(node):
    """
    get value form ast.Str, ast.Num
    """
    if isinstance(node, ast.Str):
        return node.s
    elif isinstance(node, ast.Num):
        return node.n
    elif isinstance(node, ast.List):
        return [get_value(x) for x in node.elts]
    else:
        tmp_test.append(node)
        raise TypeError('Unknown AST Type {}'.format(node))


def get_type(node):
    """
    get type
    """
    return node.func.attr


def handle_field_constraint(field, nodes):
    while len(nodes) > 0:
        node = nodes.pop(0)
        CONSTRAINT[get_type(node)](field, *[get_value(x) for x in node.args])


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
    type_ = node.func.attr

    if type_.startswith('drop_'):
        do_drop_index(table_name, type_.lstrip('drop_'), cols)
    else:
        do_add_index(table_name, type_, cols)


def do_drop_index(table_name, type_, cols):
    print(schema[table_name]['_ORM_META'])
    schema[table_name]['_ORM_META'].pop(cols)


def do_add_index(table_name, type_, cols):
    index_name = '_'.join((table_name, *to_list(cols), type_))
    schema[table_name]['_ORM_META'][index_name] = type_


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
    field_type = get_type(field)
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

    assert path.pop(0).id == 'table'

    field = path.pop(0)
    field_type = get_type(field)
    # Really here? ugly
    field_name, *args = field.args or [ast.Str('timestamps')]
    field_name = get_value(field_name)
    assert field_name not in schema[table_name]
    field_obj = schema[table_name][field_name] = FieldFactory.create(
        field_name, field_type)
    handle_field_constraint(field_obj, path)


class Builder(ast.NodeVisitor):

    def _generate_table(self, table_name, exprs):
        # collection.append(exprs)
        for expr in exprs:
            collection.append(expr)
            obj = expr.value
            assert isinstance(obj, ast.Call)

            try:
                is_index = (obj.func.value.id == 'table' and (
                            obj.func.attr in ('primary', 'unique', 'index') or
                            obj.func.attr in ('drop_primary', 'drop_unique',
                                              'drop_index')))
            except Exception:
                is_index = False

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

    def visit_FunctionDef(self, node):
        if node.name != 'up':
            return
        for ast_with in node.body:
            assert isinstance(ast_with, ast.With)
            for with_item in ast_with.items:
                assert isinstance(with_item, ast.withitem)
                assert len(with_item.context_expr.args) == 1
                table_name = with_item.context_expr.args[0].s
                self._generate_table(table_name, ast_with.body.copy())


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
        Builder().visit(ast_code)
    dump()
