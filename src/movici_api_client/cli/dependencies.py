import typing as t
T = t.TypeVar('T')
_repository = {}

def get(tp: t.Type[T]) ->t.Optional[T]:
    return _repository[tp]

def set(obj):
    _repository[type(obj)] =obj

def reset():
    _repository.clear() 
