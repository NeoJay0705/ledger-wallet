"""The two approved, fixed host resource profiles; intentionally not a general scheduler."""
from dataclasses import dataclass

@dataclass(frozen=True)
class Profile:
    cpus: int
    db_cores: str
    quota: int
    shards: int
    client_cpus: str
    workers: int
    sampler_cpus: str
    forbidden_client: frozenset

PROFILES = {
    3: Profile(3, '0-2', 3, 3, '3-5', 3, '6-7', frozenset(range(0, 3)) | frozenset(range(8, 11))),
    6: Profile(6, '0-5', 6, 6, '6-7', 2, '14-15', frozenset(range(0, 6)) | frozenset(range(8, 14))),
}

def cpus_from_list(value):
    try:
        result=set()
        for part in value.split(','):
            ends=part.split('-')
            if len(ends)==1: result.add(int(ends[0]))
            elif len(ends)==2:
                lo,hi=map(int,ends)
                if hi<lo: raise ValueError()
                result.update(range(lo,hi+1))
            else: raise ValueError()
        if not result or min(result)<0: raise ValueError()
        return result
    except (ValueError, TypeError):
        raise ValueError(f'invalid CPU list: {value!r}')

def get_profile(value):
    try: return PROFILES[int(value)]
    except (ValueError, KeyError, TypeError): raise ValueError('CPU profile must be 3 or 6')

def validate_client_cpus(profile, value):
    cpus=cpus_from_list(value)
    conflict=cpus & profile.forbidden_client
    if conflict: raise ValueError(f'client CPUs overlap DB CPUs or SMT siblings: {sorted(conflict)}')
    return cpus
