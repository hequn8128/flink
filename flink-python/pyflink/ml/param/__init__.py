################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import jsonpickle

__all__ = ['WithParams', 'Params', 'ParamInfo', 'TypeConverters']


class WithParams(object):
    """
    Parameters are widely used in machine learning realm. This class defines a common interface to
    interact with classes with parameters.
    """

    def get_params(self):
        pass

    def set(self, k, v):
        self.get_params().set(k, v)
        return self

    def get(self, k):
        return self.get_params().get(k)

    def _set(self, **kwargs):
        """
        Sets user-supplied params.
        """
        for param, value in kwargs.items():
            p = getattr(self, param)
            if value is not None:
                try:
                    value = p.type_converter(value)
                except TypeError as e:
                    raise TypeError('Invalid param value given for param "%s". %s' % (p.name, e))
            self.get_params().set(p, value)
        return self


class Params(object):
    """
    The map-like container class for parameter. This class is provided to unify the interaction with
    parameters.
    """
    def __init__(self):
        self._paramMap = {}

    def set(self, info, value):
        """
         Return the number of params.

        :param info: the info of the specific parameter to set.
        :param value: the value to be set to the specific parameter.
        :return: return the current Params.
        """
        self._paramMap[info] = value
        return self

    def get(self, info):
        if info not in self._paramMap:
            if not info.is_optional:
                raise ValueError("Missing non-optional parameter %s" % info.name)
            elif not info.has_default_value:
                raise ValueError("Cannot find default value for optional parameter %s" % info.name)
            else:
                return info.default_value
        else:
            return self._paramMap[info]

    def remove(self, info):
        self._paramMap.pop(info)

    def contains(self, info):
        return info in self._paramMap

    def size(self):
        return len(self._paramMap)

    def clear(self):
        self._paramMap.clear()

    def is_empty(self):
        return len(self._paramMap) == 0

    def to_json(self):
        return jsonpickle.encode(self._paramMap, keys=True)

    def load_json(self, j):
        self._paramMap.update(jsonpickle.decode(j, keys=True))
        return self

    @staticmethod
    def from_json(j):
        return Params().load_json(j)

    def merge(self, other_params):
        if other_params is not None:
            self._paramMap.update(other_params._paramMap)
        return self

    def clone(self):
        new_params = Params()
        new_params._paramMap.update(self._paramMap)
        return new_params


class ParamInfo(object):
    """
    Definition of a parameter, including name, description, type_converter and so on.
    """
    def __init__(self, name, description, is_optional=True, has_default_value=False, default_value=None, type_converter=None):
        self.name = str(name)
        self.description = str(description)
        self.is_optional = is_optional
        self.has_default_value = has_default_value
        self.default_value = default_value
        self.type_converter = TypeConverters.identity if type_converter is None else type_converter

    def __str__(self):
        return self.name

    def __repr__(self):
        return "Param(name=%r, description=%r)" % (self.name, self.description)

    def __hash__(self):
        return hash(str(self.name))

    def __eq__(self, other):
        if isinstance(other, ParamInfo):
            return self.name == other.name
        else:
            return False


class TypeConverters(object):
    """
    Factory methods for common type conversion functions for `Param.typeConverter`.
    """

    @staticmethod
    def _is_numeric(value):
        vtype = type(value)
        return vtype in [int, float] or vtype.__name__ == 'long'

    @staticmethod
    def _is_integer(value):
        return TypeConverters._is_numeric(value) and float(value).is_integer()

    @staticmethod
    def _can_convert_to_list(value):
        vtype = type(value)
        return vtype in [list, tuple, range]

    @staticmethod
    def _can_convert_to_string(value):
        return isinstance(value, str)

    @staticmethod
    def identity(value):
        """
        Dummy converter that just returns value.
        """
        return value

    @staticmethod
    def to_list(value):
        """
        Convert a value to a list, if possible.
        """
        if type(value) == list:
            return value
        else:
            raise TypeError("Could not convert %s to list" % value)

    @staticmethod
    def to_list_float(value):
        """
        Convert a value to list of floats, if possible.
        """
        if TypeConverters._can_convert_to_list(value):
            value = TypeConverters.toList(value)
            if all(map(lambda v: TypeConverters._is_numeric(v), value)):
                return [float(v) for v in value]
        raise TypeError("Could not convert %s to list of floats" % value)

    @staticmethod
    def to_list_list_float(value):
        """
        Convert a value to list of list of floats, if possible.
        """
        if TypeConverters._can_convert_to_list(value):
            value = TypeConverters.toList(value)
            return [TypeConverters.toListFloat(v) for v in value]
        raise TypeError("Could not convert %s to list of list of floats" % value)

    @staticmethod
    def to_list_int(value):
        """
        Convert a value to list of ints, if possible.
        """
        if TypeConverters._can_convert_to_list(value):
            value = TypeConverters.toList(value)
            if all(map(lambda v: TypeConverters._is_integer(v), value)):
                return [int(v) for v in value]
        raise TypeError("Could not convert %s to list of ints" % value)

    @staticmethod
    def to_list_string(value):
        """
        Convert a value to list of strings, if possible.
        """
        if TypeConverters._can_convert_to_list(value):
            value = TypeConverters.toList(value)
            if all(map(lambda v: TypeConverters._can_convert_to_string(v), value)):
                return [TypeConverters.toString(v) for v in value]
        raise TypeError("Could not convert %s to list of strings" % value)

    @staticmethod
    def to_float(value):
        """
        Convert a value to a float, if possible.
        """
        if TypeConverters._is_numeric(value):
            return float(value)
        else:
            raise TypeError("Could not convert %s to float" % value)

    @staticmethod
    def to_int(value):
        """
        Convert a value to an int, if possible.
        """
        if TypeConverters._is_integer(value):
            return int(value)
        else:
            raise TypeError("Could not convert %s to int" % value)

    @staticmethod
    def to_string(value):
        """
        Convert a value to a string, if possible.
        """
        if isinstance(value, str):
            return value
        else:
            raise TypeError("Could not convert %s to string type" % type(value))

    @staticmethod
    def to_boolean(value):
        """
        Convert a value to a boolean, if possible.
        """
        if type(value) == bool:
            return value
        else:
            raise TypeError("Boolean Param requires value of type bool. Found %s." % type(value))
