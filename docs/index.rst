.. hbp_archive documentation master file, created by
   sphinx-quickstart on Thu Oct  4 13:39:37 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

=======================================
Welcome to hbp_archive's documentation!
=======================================

.. toctree::
   :maxdepth: 2


.. automodule:: hbp_archive

Regarding CSCS Authentication
=============================
The Python Client attempts to simplify the CSCS authentication process.
The users have the following options (in order of priority):

#. Setting an environment variable named ``CSCS_PASS`` with your CSCS password.
   On Linux, this can be done as:

   ``export CSCS_PASS='putyourpasswordhere'``

   Environment variables set like this are only stored temporally. When you exit
   the running instance of bash by exiting the terminal, they get discarded. To
   save this permanentally, write the above command into `~/.bashrc` or `~/.profile`
   (you might need to reload these files by, for example, ``source ~/.bashrc``)

#. Enter your CSCS password when prompted by the Python Client.

File
===============
.. autoclass:: File
    :members:

Container
===============
.. autoclass:: Container
    :members:

PublicContainer
===============
.. autoclass:: PublicContainer
   :members:

Project
===============
.. autoclass:: Project
   :members:

Archive
===============
.. autoclass:: Archive
   :members:

Misc
===============
.. autofunction:: scale_bytes
.. autofunction:: set_logger
