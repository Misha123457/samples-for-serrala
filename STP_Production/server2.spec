# -*- mode: python ; coding: utf-8 -*-



# -*- coding: utf-8 -*-
"""
Created on Fri Sep  6 22:22:05 2019

@author: m.zhukov
"""

from PyInstaller.building.build_main import Analysis, PYZ, EXE, COLLECT, BUNDLE, TOC


def collect_pkg_data(package, include_py_files=False, subdir=None):
    import os
    from PyInstaller.utils.hooks import get_package_paths, remove_prefix, PY_IGNORE_EXTENSIONS

    # Accept only strings as packages.
    if type(package) is not str:
        raise ValueError

    pkg_base, pkg_dir = get_package_paths(package)
    if subdir:
        pkg_dir = os.path.join(pkg_dir, subdir)
    # Walk through all file in the given package, looking for data files.
    data_toc = TOC()
    for dir_path, dir_names, files in os.walk(pkg_dir):
        for f in files:
            extension = os.path.splitext(f)[1]
            if include_py_files or (extension not in PY_IGNORE_EXTENSIONS):
                source_file = os.path.join(dir_path, f)
                dest_folder = remove_prefix(dir_path, os.path.dirname(pkg_base) + os.sep)
                dest_file = os.path.join(dest_folder, f)
                data_toc.append((dest_file, source_file, 'DATA'))

    return data_toc

pkg_data = collect_pkg_data('swagger_ui_bundle')

block_cipher = None

needed_files = [(r'swagger2.yml',
                           r'swagger2.yml',
                          'DATA')]
a = Analysis(['server.py'],
             pathex=['C:\\Users\\m.zhukov\\Documents\\STPProduction'],
             binaries=[],
             datas=[('templates','templates'),('openapi_spec_validator','openapi_spec_validator')],
             hiddenimports=['pymongo', 'pandas', 'datetime', 'http.client', 'json', 'ssl', 'bson.json_util','jinja2','connexion'],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
			 
pyz = PYZ(a.pure)
exe = EXE(pyz,
          a.scripts,
		  a.binaries,
		  a.zipfiles,
	      pkg_data,
          a.datas,
		  needed_files,
          [],
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          console=True,		 
          upx_exclude=[],
          name='server')

