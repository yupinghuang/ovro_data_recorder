import os
import glob
from subprocess import check_call, check_output, CalledProcessError
from setuptools import setup, Extension, find_namespace_packages

try:
    import numpy as np
except Exception as e:
    raise RuntimeError(f"numpy is required to run setup.py: {str(e)}")


def get_version():
    """Determine a version based on the git repo info."""
    
    # Part 1 - the "official" version
    with open('VERSION', 'r') as fh:
        version = fh.read().strip()
    
    # Part 2 - query the git repo info.  If this fails make a note of it and
    # return an un-altered "official" version
    repo_version = 'unknown'
    try:
        git_branch = check_output(['git', 'branch', '--show-current'],
                                  cwd=os.path.dirname(__file__))
        git_branch = git_branch.decode().strip().rstrip()
        if os.getenv('READTHEDOCS', None) is not None:
            git_branch = 'rtd'
            
        git_hash = check_output(['git', 'log', '-n', '1', '--pretty=format:%H'],
                                cwd=os.path.dirname(__file__))
        git_hash = git_hash.decode().strip().rstrip()
        
        git_dirty = 0
        try:
            check_call(['git', 'diff-index', '--quiet', '--cached', 'HEAD', '--'],
                       cwd=os.path.dirname(__file__))
        except CalledProcessError:
            git_dirty += 1
        try:
            check_call(['git', 'diff-files', '--quiet'],
                       cwd=os.path.dirname(__file__))
        except CalledProcessError:
            git_dirty += 1
            
        repo_version = git_branch+'.'+git_hash[:7]
        if git_dirty > 0:
            repo_version += '.dirty'
            
    except CalledProcessError as e:
        print(f"Failed to determine git repo versioning - {str(e)}")
        
    version = version+'+'+repo_version
    
    return version


def write_version_info():
    """Write the version info to a module in ovro_data_recorder."""
    
    odrVersion = get_version()
    odrShortVersion = '.'.join(odrVersion.split('.')[:2])
    odrLocalVersion = odrVersion.split('+', 1)[-1]
    
    with open('ovro_data_recorder/version.py', 'w') as fh:
        fh.write(f"""# This file is automatically generated by setup.py

version = '{odrVersion}'
full_version = '{odrVersion}'
short_version = '{odrShortVersion}'
local_version = '{odrLocalVersion}'
""")


ExtensionModules = [Extension('gridder', ['ovro_data_recorder/gridder.cpp',],
                              include_dirs=[np.get_include()],
                              libraries=['m', 'fftw3f'],
                              extra_compile_args=['-DNPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION',]),]


# Update the version information
write_version_info()


setup(
    name = 'ovro_data_recorder',
    version = get_version(),
    description = 'Data recorder software for the Owens Valley Long Wavelength Array station',
    license='BSD3',
    packages=find_namespace_packages(),
    scripts=glob.glob('scripts/*.py'),
    setup_requires = ['numpy>=1.7'],
    install_requires = ['astropy', 'python-casacore', 'numpy', 'scipy', 'h5py', 'pillow',
                        'etcd3', 'jinja2', 'bifrost', 'mnc_python', 'lwa_antpos',
                        'lwa352_pipeline_control', 'lwa_observing'],
    include_package_data = True,  
    ext_package = 'ovro_data_recorder', 
    ext_modules = ExtensionModules,
    zip_safe = False
)
