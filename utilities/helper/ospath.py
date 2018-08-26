import os

"""
Module relating to OS Pathing.
"""
class OSPath:
    """
    Internal class to handle paths according to Mac / Windows.
    """
    @classmethod
    def path(self, p):
        """
        Given a PathLib's Path object, returns a resolved format depending on calling OS is Mac or Windows.        
        
        :param p: pathlib's Path object representing an OS path.
        :type p: pathlib.Path
        :returns: String representation of the path suitable for the OS to use in a open(...) call.
        """
        if os.name == 'nt':
            # on windows
            return str(p)
        else:
            return p.resolve()