from inspect import getsource
from hashlib import sha256
import secrets
import string


class Versionable:
    """TODO write documentation."""

    def register_params(self, **kwargs):
        if getattr(self, "_versionables", None) is None:
            self._versionables = {}
        self._versionables.update(**kwargs)

    def version(self):
        """Detects changes in subclass's code and parameters.

        Dame automagically invalidates cache and storage on changes in Transforms.
        For this to work Dame needs a way to determine whether the Transform had
        changed since the last use.

        Returns:
            str: Hash computed over source code of the class and it's parameters
        """
        if hasattr(self, "__version_str__") and self.__version_str__ is not None:
            return self.__version_str__
        result = sha256()
        # TODO strip docs and comments from getsource. Maybe using astunparse?
        result.update(getsource(self.__class__).encode())
        result.update(self.get_params_hash().encode())
        return result.hexdigest()

    def get_params_hash(self):
        result = sha256()
        for key, value in getattr(self, "_versionables", {}).items():
            result.update(key.encode())
            result.update(self.get_version_str(value).encode())
        return result.hexdigest()

    def get_version_str(self, x):
        if hasattr(x, "__version_str__"):
            return x.__version_str__
        elif hasattr(x, "version"):
            return x.version()
        # TODO issue a warning for objects (str(x) == "<some_class object at 0x838>")
        return str(x)


class Unversionable(Versionable):
    """TODO write documentation."""

    @property
    def __version_str__(self):
        if not hasattr(self, "__version_str_gen__"):
            # https://stackoverflow.com/questions/2257441/random-string-generation-with-upper-case-letters-and-digits/23728630#23728630
            random_str = "".join(
                secrets.choice(string.ascii_uppercase + string.digits)
                for _ in range(100)
            )
            res = sha256()
            res.update(random_str.encode())
            self.__version_str_gen__ = res.hexdigest()
        return self.__version_str_gen__
