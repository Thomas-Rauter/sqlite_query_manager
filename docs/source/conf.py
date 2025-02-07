import os
import sys
sys.path.insert(0, os.path.abspath('../../sqlite_manager'))


# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'sqlite_manager'
copyright = '2025, Thomas Rauter'
author = 'Thomas Rauter'
release = '0.1.0'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',  # For NumPy/Google-style docstrings
]


templates_path = ['_templates']
exclude_patterns = ['**/__init__.py']


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "furo"
html_theme_options = {
    "sidebar_hide_name": True,  # Hide the project name in the sidebar
}
html_static_path = []
