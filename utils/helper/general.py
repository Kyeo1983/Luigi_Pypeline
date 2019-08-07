import os

class UtilHelper:
    """
    General utilities
    """
    @classmethod
    def get_first_valid_string(self, inlist):
        for i in inlist:
            if i is not None and len(i.strip()) > 0:
                return i
        return None
