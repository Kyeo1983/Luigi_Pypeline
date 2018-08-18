import os

class OSPath:
    @classmethod
    def path(self, p):
        if os.name == 'nt':
            # on windows
            return str(p)
        else:
            return p.resolve()