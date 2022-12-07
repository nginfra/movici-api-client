__MOVICI_FLAGS__ = "__movici_flags__"

def set_flag(obj, flag: str):
    flags = getattr(obj, __MOVICI_FLAGS__, set())
    flags.add(flag)
    setattr(obj, __MOVICI_FLAGS__, flags)
    
    
def has_flag(obj, flag:str) -> bool:
    return flag in getattr(obj, __MOVICI_FLAGS__, set())
    
def remove_flag(obj, flag:str):
    flags: set
    if flags:= getattr(obj, __MOVICI_FLAGS__, None):
        flags.discard(flag)
        

def command(func):
    set_flag(func, "command")