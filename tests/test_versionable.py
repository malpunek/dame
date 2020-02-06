# TODO show some love to these tests, rethink, cleanup and write the docs!
from tempfile import TemporaryDirectory
from pathlib import Path
from importlib import import_module, reload
import fileinput


from dame.versionable import Versionable, Unversionable


code_base = """
from dame.versionable import Versionable

class TestT(Versionable):
    {}
{}
    def __init__(self, **kwargs):
        self.register_params(**kwargs)
        for key, value in kwargs.items():
            setattr(self, key, value)
{}
"""


dir_path = Path(__file__).resolve().parent
tmp_dir = TemporaryDirectory(dir=dir_path)
open(Path(tmp_dir.name) / "__init__.py", "w").close()


def make_versionable(doc, attrs, apply):
    def indent(code, spaces):
        return "".join(f"{' ' * spaces}{line}\n" for line in code.split("\n"))

    doc_code = f'"""{doc}"""'
    attr_code = "".join(f"{key} = {value}\n" for key, value in attrs.items())
    attr_code = indent(attr_code, 4)
    apply_code = indent(apply, 8)[4:]
    return code_base.format(doc_code, attr_code, apply_code)


class tmp_module:
    def __init__(self, name, content):
        self.fname = f"{name}.py"
        self.mod_name = Path(tmp_dir.name).name
        self.path = Path(tmp_dir.name) / self.fname
        self.content = content

    def __enter__(self):
        self.f = open(self.path, "w+")
        self.f.write(self.content)
        self.f.close()
        imp_name = f".{self.mod_name}.{self.fname[:-3]}"
        self.mod = import_module(imp_name, package="tests")
        return self.mod

    def __exit__(self, exc_type, exc_value, traceback):
        return False


def test_basic():
    with tmp_module("transformA", make_versionable("", {}, "")) as ta:
        tae1 = ta.TestT()
        tae2 = ta.TestT()

        assert tae1.version() == tae2.version()


def test_params():
    with tmp_module("transformA", make_versionable("", {}, "")) as ta:

        tak1 = ta.TestT(ala="kota")
        tak2 = ta.TestT(basia="kota")
        tak3 = ta.TestT(ala="psa")
        tak4 = ta.TestT(ala="kota")

        assert tak1.version() == tak4.version()
        assert tak1.version() != tak2.version()
        assert tak1.version() != tak3.version()
        assert tak2.version() != tak3.version()


def test_code():
    code = "def whatever():\npass"

    with tmp_module("transA", make_versionable("", {}, code)) as ta:
        tt = ta.TestT()
        ver1 = tt.version()
        for line in fileinput.input(files=(ta.__file__,), inplace=True):
            if "whatever" in line:
                print("    def whatever2():")
            else:
                print(line[:-1])
        ta = reload(ta)
        tt2 = ta.TestT()
        ver2 = tt2.version()

        assert ver1 != ver2


class X(Versionable):
    def x():
        pass


class Y(Versionable):
    def y():
        pass


def test_version_str():
    x, y = X(), Y()
    assert x.version() != y.version()
    x.__version_str__ = "1"
    y.__version_str__ = "1"
    assert x.version() == y.version()


class Z(Unversionable):
    pass


def test_unversionable():
    p, q = Z(), Z()
    assert p.version() != q.version()
