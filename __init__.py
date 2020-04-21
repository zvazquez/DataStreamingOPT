from setuptools_scm import get_version

#Set version based on Git version and commit
__version__ = get_version(root='../', relative_to=__file__)