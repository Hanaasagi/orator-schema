emap = lambda *args, **kwargs: list(map(*args, **kwargs))


def reverse_path(node):
    """reversed ast path"""
    path = [node]
    while isinstance(node, ast.Call):
        path.append(node.func.value)
        node = node.func.value
    return deque(reversed(path))


def get_value(node):
    """get value form ast.Str, ast.Num, ast.List"""
    if isinstance(node, ast.Str):
        return node.s
    elif isinstance(node, ast.Num):
        return node.n
    elif isinstance(node, ast.List):
        return [get_value(x) for x in node.elts]
    else:
        tmp_test.append(node)
        raise TypeError('Unknown AST Type {}'.format(node))


def get_function_name(node):
    """ get field type
    node: ast.Call
    rtype: str
    """
    return node.func.attr


def get_function_arg(node):
    """ get field args
    node: ast.Call
    rtype: list
    """
    return emap(get_value, node.args)
