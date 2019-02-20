# Copyright (c) 2017-2018 CNRS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
A high-level API for interacting with the Human Brain Project archival storage at CSCS.

Author: Andrew Davison and Shailesh Appukuttan, CNRS

License: Apache License, Version 2.0, see LICENSE.txt

Documentation: https://hbp-archive.readthedocs.io

Installation::

    pip install hbp_archive


Example Usage
=============

.. code-block:: python

    from hbp_archive import Container, PublicContainer, Project, Archive


    # Working with a public container

    container = PublicContainer("https://object.cscs.ch/v1/AUTH_id/my_container")
    files = container.list()
    local_file = container.download("README.txt")
    print(container.read("README.txt"))
    number_of_files = container.count()
    size_in_MB = container.size("MB")

    # Working with a private container

    container = Container("MyContainer", username="xyzabc")  # you will be prompted for your password
    files = container.list()
    local_file = container.download("README.txt", overwrite=True)  # default is not to overwrite existing files
    print(container.read("README.txt"))
    number_of_files = container.count()
    size_in_MB = container.size("MB")

    container.move("my_file.dat", "a_subdirectory", "new_name.dat")  # move/rename file within a container

    # Reading a file directly, without downloading it

    with container.open("my_data.txt") as fp:
        data = np.loadtxt(fp)

    # Working with a project

    my_proj = Project('MyProject', username="xyzabc")
    container = my_proj.get_container("MyContainer")

    # Listing all your projects

    archive = Archive(username="xyzabc")
    projects = archive.projects
    container = archive.find_container("MyContainer")  # will search through all projects

"""

from __future__ import division
import getpass
import os
import sys
from keystoneauth1.identity import v3
from keystoneauth1 import session
from keystoneauth1.exceptions.auth import AuthorizationFailure
from keystoneauth1.extras._saml2 import V3Saml2Password
from keystoneclient.v3 import client as ksclient
import swiftclient.client as swiftclient
from swiftclient.exceptions import ClientException
try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path  # Python 2 backport
import requests
import logging
try:
    raw_input
except NameError:  # Python 3
    raw_input = input

__version__ = "0.8.1"

OS_AUTH_URL = 'https://pollux.cscs.ch:13000/v3'
OS_IDENTITY_PROVIDER = 'cscskc'
OS_IDENTITY_PROVIDER_URL = 'https://auth.cscs.ch/auth/realms/cscs/protocol/saml/'

logging.basicConfig(stream=sys.stdout, level=logging.WARNING)
logger = logging.getLogger("hbp_archive")

def scale_bytes(value, units):
    """Convert a value in bytes to a different unit.

    Parameters
    ----------
    value : int
        Value (in bytes) to be converted.
    units : string
        Requested units for output.
        Options: 'bytes', 'kB', 'MB', 'GB', 'TB'

    Returns
    -------
    float
        Value in requested units.
    """
    allowed_units = {
        'bytes': 1,
        'kB': 1024,
        'MB': 1048576,
        'GB': 1073741824,
        'TB': 1099511627776
    }
    if units not in allowed_units:
        raise ValueError("Units must be one of {}".format(list(allowed_units.keys())))
    scale = allowed_units[units]
    return value / scale

def set_logger(location="screen", level="INFO"):
    """Set the logging specifications for this module.

    Parameters
    ----------
    location : string / None, optional
        Can be set to following options:
        - 'screen' (case insensitive; default) : display log messages on screen
        - None : disable logging
        - Any other input will be considered as filename for logging to a file
    level : string, option
        Specify the logging level.
        Options: 'DEBUG'/'INFO'/'WARNING'/'ERROR'/'CRITICAL'
    """
    # Remove all existing handlers
    for handler in logger.root.handlers[:]:
        logger.root.removeHandler(handler)
    if location and level not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        raise Exception("level should be specified as: 'DEBUG'/'INFO'/'WARNING'/'ERROR'/'CRITICAL'")
    if not location:
        logger.disabled = True
    else:
        logger.disabled = False
        if location.lower() == "screen":
            logging.basicConfig(stream=sys.stdout, level=eval("logging.{}".format(level)))
        else:
            if not location.endswith(".log"):
                location = location + ".log"
            logging.basicConfig(filename=location, level=eval("logging.{}".format(level)))


class File(object):
    """A representation of a file in a container.

    The following actions can be performed:

    ====================================   ====================================
    Action                                 Method
    ====================================   ====================================
    Get directory name                     :attr:`dirname`
    Get file name                          :attr:`basename`
    Download a file                        :meth:`download`
    Read contents of a file                :meth:`read`
    Move a file                            :meth:`move`
    Rename a file                          :meth:`rename`
    Copy a file                            :meth:`copy`
    Delete a file                          :meth:`delete`
    Get size of file                       :meth:`size`
    ====================================   ====================================
    """

    def __init__(self, name, bytes, content_type, hash, last_modified, container=None):
        self.name = name
        self.bytes = bytes
        self.content_type = content_type
        self.hash = hash
        self.last_modified = last_modified
        self.container = container

    def __str__(self):
        return "'{}'".format(self.name)

    def __repr__(self):
        return "'{}'".format(self.name)

    @property
    def dirname(self):
        """Returns the directory name from file path.

        Returns
        -------
        string
             Directory path of file.
        """
        return os.path.dirname(self.name)

    @property
    def basename(self):
        """Returns the file name from file path.

        Returns
        -------
        string
             Name of file.
        """
        return os.path.basename(self.name)

    def download(self, local_directory, with_tree=True, overwrite=False):
        """Download this file to a local directory.

        Parameters
        ----------
        local_directory : string
            Local directory path where file is to be saved.
        with_tree : boolean, optional
            Specify if directory structure of file is to be retained.
        overwrite : boolean, optional
            Specify if any already existing file should be overwritten.

        Returns
        -------
        string
             Path of file created inside specified local directory.
        """
        if self.container:
            self.container.download(self.name, local_directory=local_directory, with_tree=with_tree, overwrite=overwrite)
        else:
            raise Exception("Parent container not known, unable to download")

    def read(self, decode='utf-8', accept=[]):
        """Read and return the contents of this file in the container.

        Parameters
        ----------
        file_path : string
            Path of file to be retrieved.
        decode : string, optional
            Files containing text will be decoded using specified encoding
            (default: 'utf-8'). To prevent any attempt at decoding, set `decode=False`.
        accept : boolean, optional
            To force decoding, put the expected content type in `accept`.

        Returns
        -------
        string (unicode)
            Contents of the specified file.
        """
        if self.container:
            return self.container.read(self.name, decode=decode, accept=accept)
        else:
            raise Exception("Parent container not known, unable to read file contents")

    def move(self, target_directory, new_name=None, overwrite=False):
        """Move this file to the specified directory.

        Parameters
        ----------
        target_directory : string
            Target directory where the file is to be moved.
        new_name : string, optional
            New name to be assigned to file (including extension, if any).
        overwrite : boolean, optional
            Specify if any already existing file should be overwritten.
        """
        if self.container:
            self.container.move(self.name, target_directory=target_directory, new_name=new_name, overwrite=overwrite)
        else:
            raise Exception("Parent container not known, unable to move")

    def rename(self, new_name, overwrite=False):
        """Rename this file within the source directory.

        Parameters
        ----------
        new_name : string
            New name to be assigned to file (including extension, if any).
        overwrite : boolean, optional
            Specify if any already existing file should be overwritten.
        """
        self.move(target_directory=os.path.dirname(self.name), new_name=new_name, overwrite=overwrite)

    def copy(self, target_directory, new_name=None, overwrite=False):
        """Copy this file to specified directory.

        Parameters
        ----------
        target_directory : string
            Target directory where the file is to be copied.
        new_name : string, optional
            New name to be assigned to file (including extension, if any).
        overwrite : boolean, optional
            Specify if any already existing file at target location should be overwritten.
        """
        self.container.copy(self.name, target_directory=os.path.dirname(self.name), new_name=new_name, overwrite=overwrite)

    def delete(self):
        """Delete this file."""
        self.container.delete(self.name)

    def size(self, units='bytes'):
        """Return the size of this file in the requested unit (default bytes).

        Parameters
        ----------
        units : string
            Requested units for output.
            Options: 'bytes' (default), 'kB', 'MB', 'GB', 'TB'

        Returns
        -------
        float
            Size of specified file in requested units.
        """
        return scale_bytes(self.bytes, units)


class Container(object):
    """A representation of a CSCS storage container. Can be used to operate both
    public and private CSCS containers. A CSCS account is needed to use this class.

    The following actions can be performed:

    ====================================   ====================================
    Action                                 Method
    ====================================   ====================================
    Get metadata about the container       :attr:`metadata`
    Get url if container is public         :attr:`public_url`
    List all files in container            :meth:`list`
    Return a file from given path          :meth:`get`
    Get number of files in container       :meth:`count`
    Get total size of data in container    :meth:`size`
    Upload file(s) to container            :meth:`upload`
    Download a file from container         :meth:`download`
    Read contents of file in container     :meth:`read`
    Copy a file in container               :meth:`copy`
    Move a file in container               :meth:`move`
    Delete a file in container             :meth:`delete`
    Copy a directory in container          :meth:`copy_directory`
    Move a directory in container          :meth:`move_directory`
    Delete a directory  in container       :meth:`delete_directory`
    List users with access to container    :meth:`access_control`
    Grant container access to user         :meth:`grant_access`
    Revoke container access from user      :meth:`revoke_access`
    ====================================   ====================================
    """

    def __init__(self, container, username, token=None, project=None):
        if project is None:
            archive = Archive(username, token=token)
            project = archive.find_container(container).project
        elif isinstance(project, str):
            project = Project(project, username=username, token=token)
        self.project = project
        self.name = container
        self._metadata = None

    def __str__(self):
        return "'{}/{}'".format(self.project, self.name)

    def __repr__(self):
        return "Container('{}', project='{}', username='{}')".format(
            self.name, self.project.name, self.project.archive.username)

    @property
    def metadata(self):
        """Metadata about the container.

        Returns
        -------
        dict
            Dictionary with metadata about the container.
        """
        if self._metadata is None:
            self._metadata = self.project._connection.head_container(self.name)
        return self._metadata

    @property
    def public_url(self):
        """Get url if container is public.

        Returns
        -------
        string
            URL to access public container; returns None for private containers.
        """
        if "PUBLIC" in self.access_control()["read"]:
            return "https://object.cscs.ch/v1/AUTH_{self.project.id}/{self.name}".format(self=self)
        else:
            return None

    def list(self):  # , content_type=None, newer_than=None, older_than=None):
        """List all files in the container.

        Returns
        -------
        list
            List of `hbp_archive.File` objects existing in container.
        """
        self._metadata, contents = self.project._connection.get_container(self.name)
        return [File(container=self, **item) for item in contents]

    def get(self, file_path):
        """Return a File object for the file at the given path.

        Parameters
        ----------
        file_path : string
            Path of file to be retrieved.

        Returns
        -------
        `hbp_archive.File`
            Requested `hbp_archive.File` object from container.
        """
        for f in self.list():  # very inefficient
            if f.name == file_path:
                return f
        raise ValueError("Path '{}' does not exist".format(file_path))

    def count(self):
        """Number of files in the container

        Returns
        -------
        int
            Count of number of files in the container.
        """
        return int(self.metadata['x-container-object-count'])

    def size(self, units='bytes'):
        """Total size of all data in the container

        Parameters
        ----------
        units : string
            Requested units for output.
            Options: 'bytes' (default), 'kB', 'MB', 'GB', 'TB'

        Returns
        -------
        float
            Total size of all data in the container in requested units.
        """
        return scale_bytes(int(self.metadata['x-container-bytes-used']), units)

    def upload(self, local_paths, remote_directory="", overwrite=False):
        """Upload file(s) to the container.

        Parameters
        ----------
        local_paths : string, list of strings
            Local path of file(s) to be uploaded.
        remote_directory : string, optional
            Remote directory path where data is to be uploaded. Default is root directory.
        overwrite : boolean, optional
            Specify if any already existing file at target should be overwritten.

        Returns
        -------
        list
            List of strings indicating file paths created on container.

        Note
        ----
        Using the command-line "swift upload" will likely be faster since
        it uses a pool of threads to perform multiple uploads in parallel.
        It is thus recommended for bulk uploads.
        """
        if isinstance(local_paths, str):
            local_paths = [local_paths]
        remote_paths = []

        contents = [f.name for f in self.list()]
        for path in local_paths:
            remote_path = os.path.join(remote_directory, os.path.basename(path))
            if not overwrite and remote_path in contents:
                raise Exception("Target file path '{}' already exists! Set `overwrite=True` to overwrite file.".format(remote_path))
            with open(path, 'rb') as f:
                file_data = f.read()
                self.project._connection.put_object(self.name, remote_path, file_data)
                remote_paths.append(remote_path)
        return remote_paths

    def download(self, file_path, local_directory=".", with_tree=True, overwrite=False):
        """Download a file from the container.

        Parameters
        ----------
        file_path : string
            Path of file to be downloaded.
        local_directory : string, optional
            Local directory path where file is to be saved.
        with_tree : boolean, optional
            Specify if directory structure of file is to be retained.
        overwrite : boolean, optional
            Specify if any already existing file should be overwritten.

        Returns
        -------
        string
             Path of file created inside specified local directory.
        """
        # todo: allow file_path to be a File object
        headers, contents = self.project._connection.get_object(self.name, file_path)
        if with_tree:
            local_directory = os.path.join(os.path.abspath(local_directory),
                                           *os.path.dirname(file_path).split("/"))
        Path(local_directory).mkdir(parents=True, exist_ok=True)
        local_path = os.path.join(local_directory, os.path.basename(file_path))
        if not overwrite and os.path.exists(local_path):
            raise IOError("Destination file '{}' already exists! Set `overwrite=True` to overwrite file.".format(local_path))
        with open(local_path, "wb") as local:
            local.write(contents)
        return local_path
        # todo: check hash

    def read(self, file_path, decode='utf-8', accept=[]):
        """Read and return the contents of a file in the container.

        Parameters
        ----------
        file_path : string
            Path of file to be retrieved.
        decode : string, optional
            Files containing text will be decoded using specified encoding
            (default: 'utf-8'). To prevent any attempt at decoding, set `decode=False`.
        accept : boolean, optional
            To force decoding, put the expected content type in `accept`.

        Returns
        -------
        string (unicode)
            Contents of the specified file.
        """
        text_content_types = ["application/json", ]
        headers, contents = self.project._connection.get_object(self.name, file_path)
        # todo: check hash
        content_type = headers["content-type"]
        ct_parts = content_type.split("/")
        if (ct_parts[0] == "text" or content_type in text_content_types or content_type in accept) and decode:
            return contents.decode(decode)
        else:
            return contents

    def copy(self, file_path, target_directory, new_name=None, overwrite=False):
        """Copy a file to the specified directory.

        Parameters
        ----------
        file_path : string
            Path of file to be copied.
        target_directory : string
            Target directory where the file is to be copied.
        new_name : string, optional
            New name to be assigned to file (including extension, if any).
        overwrite : boolean, optional
            Specify if any already existing file should be overwritten.
        """
        if not new_name:
            new_name = os.path.basename(file_path)

        contents = [f.name for f in self.list()]
        path = os.path.join(target_directory, new_name)
        if file_path not in contents:
            raise Exception("Source file path '{}' does not exist!".format(file_path))
        if not overwrite and path in contents:
            raise Exception("Target file path '{}' already exists! Set `overwrite=True` to overwrite file.".format(path))
        self.project._connection.copy_object(self.name, file_path, destination=os.path.join(self.name, path))
        logger.info("Successfully copied the object")

    def move(self, file_path, target_directory, new_name=None, overwrite=False):
        """Move a file to the specified directory.

        Parameters
        ----------
        file_path : string
            Path of file to be moved.
        target_directory : string
            Target directory where the file is to be moved.
        new_name : string, optional
            New name to be assigned to file (including extension, if any).
        overwrite : boolean, optional
            Specify if any already existing file should be overwritten.
        """
        if not new_name:
            new_name = os.path.basename(file_path)
        contents = [f.name for f in self.list()]
        path = os.path.join(target_directory, new_name)
        if file_path not in contents:
            raise Exception("Source file path '{}' does not exist!".format(file_path))
        if not overwrite and path in contents:
            raise Exception("Target file path '{}' already exists! Set `overwrite=True` to overwrite file.".format(path))
        self.project._connection.copy_object(self.name, file_path, destination=os.path.join(self.name, path))
        self.project._connection.delete_object(self.name, file_path)
        if os.path.dirname(file_path) == target_directory:
            logger.info("Successfully renamed the object")
        else:
            logger.info("Successfully moved the object")

    def delete(self, file_path):
        """Delete the specified file.

        Parameters
        ----------
        file_path : string
            Path of file to be deleted.
        """
        # For some inexplicable reason, in some cases the file does not get
        # deleted after executing this the first time. In these cases, we need
        # to repeat this operation to delete the file. It would thus be wise
        # to verify if the file is actually deleted or not, before proceeding.
        contents = [f.name for f in self.list()]
        if file_path not in contents:
            raise Exception("Specified file path {} does not exist!".format(file_path))
        ctr = 0
        while ctr<5 and file_path in contents:
            self.project._connection.delete_object(self.name, file_path)
            contents = [f.name for f in self.list()]
        if file_path in contents:
            raise Exception("Unable to delete the file '{}'".format(file_path))
        else:
            logger.info("Successfully deleted the object")

    def copy_directory(self, directory_path, target_directory, new_name=None, overwrite=False):
        """Copy a directory to the specified directory location.
           The original tree structure of the directory will be maintained at
           the target location.

        Parameters
        ----------
        directory_path : string
            Path of directory to be copied.
        target_directory : string
            Path of target directory where specified directory is to be copied.
        new_name : string, optional
            New name to be assigned to directory.
        overwrite : boolean, optional
            Specify if any already existing files at target location should be
            overwritten. If False (default value), then only non-conflicting
            files will be copied over.
        """
        if directory_path[-1] != '/':
            directory_path += '/'
        if not new_name:
            new_name = os.path.basename(directory_path)
        all_files = self.list()
        dir_files = [f for f in all_files if f.name.startswith(directory_path)]
        if not dir_files:
            raise Exception("Specified directory '{}' does not exist in this container!".format(directory_path[:-1]))
        else:
            logger.info("***** Directory Copy Details *****")
            for f in dir_files:
                logger.info("Filename: {}".format(f.name))
                self.copy(f.name, os.path.join(target_directory, new_name), overwrite=overwrite)

    def move_directory(self, directory_path, target_directory, new_name=None, overwrite=False):
        """Move a directory to the specified directory location.
           Can also be used to rename a directory.
           The original tree structure of the directory will be maintained at
           the target location.

        Parameters
        ----------
        directory_path : string
            Path of directory to be copied.
        target_directory : string
            Path of target directory where specified directory is to be copied.
        new_name : string, optional
            New name to be assigned to directory.
        overwrite : boolean, optional
            Specify if any already existing files at target location should be
            overwritten. If False (default value), then only non-conflicting
            files will be copied over.
        """
        if directory_path[-1] != '/':
            directory_path += '/'
        if not new_name:
            new_name = os.path.basename(directory_path)
        all_files = self.list()
        dir_files = [f for f in all_files if f.name.startswith(directory_path)]
        if not dir_files:
            raise Exception("Specified directory '{}' does not exist in this container!".format(directory_path[:-1]))
        else:
            logger.info("***** Directory Move Details *****")
            for f in dir_files:
                logger.info("Filename: {}".format(f.name))
                self.move(f.name, os.path.join(target_directory, new_name), overwrite=overwrite)

    def delete_directory(self, directory_path):
        """Delete the specified directory (and its contents).

        Parameters
        ----------
        directory_path : string
            Path of directory to be deleted.
        """
        if directory_path[-1] != '/':
            directory_path += '/'
        all_files = self.list()
        dir_files = [f for f in all_files if f.name.startswith(directory_path)]
        if not dir_files:
            raise Exception("Specified directory '{}' does not exist in this container!".format(directory_path[:-1]))
        else:
            logger.info("***** Directory Delete Details *****")
            for f in dir_files:
                logger.info("Filename: {}".format(f.name))
                self.delete(f.name)

    def access_control(self, show_usernames=True):
        """List the users that have access to this container.

        Parameters
        ----------
        show_usernames : boolean, optional
            default is `True`

        Returns
        -------
        dict
            Dictionary with keys 'read' and 'write'; each having a value in the
            form of a list of usernames
        """
        acl = {}
        for key in ("read", "write"):
            item = self.metadata.get('x-container-{}'.format(key), [])
            if item:
                item = item.split(",")
            acl[key] = item
        if show_usernames:  # map user id to username
            user_id_map = self.project.users
            for key in ("read", "write"):
                is_public = False
                user_ids = []
                for item in acl[key]:
                    if item in ('.r:*', '.rlistings'):
                        is_public = True
                    else:
                        user_ids.append(item.split(":")[1])  # each item is "project:user_id"
                acl[key] = [user_id_map.get(user_id, user_id) for user_id in user_ids]
                if is_public:
                    acl[key].append("PUBLIC")
        return acl

    def grant_access(self, username, mode='read'):
        """
        Give read or write access to the given user.

        Parameters
        ----------
        username : string
            username of user to be granted access;
            set to 'PUBLIC' to give public read-only access (no password required)
        mode : string, optional
            the access permission to be granted: 'read'/'write'; default = 'read'

        Note
        ----
        Use restricted to Superusers/Operators.
        """
        if username == "PUBLIC":
            mode = 'read'
        current_acl = self.access_control(show_usernames=True)[mode]
        if username in current_acl:
            logger.info("User {} already has {} access to this container!".format(username, mode))
        else:
            if username == "PUBLIC":
                new_acl = self.access_control(show_usernames=False)[
                    mode] + ['.r:*', '.rlistings']
            else:
                name_map = {v: k for k, v in self.project.users.items()}
                user_id = name_map[username]
                new_acl = self.access_control(show_usernames=False)[
                    mode] + ["{}:{}".format(self.project.id, user_id)]
            headers = {"x-container-{}".format(mode): ",".join(new_acl)}
            response = self.project._connection.post_container(self.name, headers)
            self._metadata = None  # needs to be refreshed
            logger.info("User {} has been granted {} access to this container.".format(username, mode))

    def revoke_access(self, username, mode='read'):
        """
        Remove read or write access from the given user.

        Parameters
        ----------
        username : string
            username of user to be revoked access;
            set to 'PUBLIC' to make a container private
        mode : string, optional
            the access permission to be revoked: 'read'/'write'; default = 'read'

        Note
        ----
        Use restricted to Superusers/Operators.
        """
        if username == "PUBLIC":
            mode = 'read'
        current_acl = self.access_control(show_usernames=True)[mode]
        if username not in current_acl:
            logger.info("User {} does not have {} access to this container!".format(username, mode))
        else:
            acl = self.access_control(show_usernames=False)[mode]
            if username == "PUBLIC":
                acl.remove('.r:*')
                acl.remove('.rlistings')
            else:
                name_map = {v: k for k, v in self.project.users.items()}
                user_id = name_map[username]
                for item in acl:
                    if item.endswith(":{}".format(user_id)):
                        acl.remove(item)
            headers = {"x-container-{}".format(mode): ",".join(acl)}
            response = self.project._connection.post_container(self.name, headers)
            self._metadata = None  # needs to be refreshed
            logger.info("User {} has been revoked {} access to this container.".format(username, mode))


class PublicContainer(object):  # todo: figure out inheritance relationship with Container
    """A representation of a public CSCS storage container. Can be used to operate
    only public CSCS containers. A CSCS account is not needed to use this class.

    The following actions can be performed:

    ====================================   ====================================
    Action                                 Method
    ====================================   ====================================
    List all files in container            :meth:`list`
    Return a file from given path          :meth:`get`
    Get number of files in container       :meth:`count`
    Get total size of data in container    :meth:`size`
    Download a file from container         :meth:`download`
    Read contents of file in container     :meth:`read`
    ====================================   ====================================

    Note
    ----
    This class only permits read-only operations. For other features,
    you may access a public container via the :class:`Container` class.
    """

    def __init__(self, url):
        self.url = url
        self.name = url.split("/")[-1]
        self.project = None
        self._content_list = None

    def __str__(self):
        return self.url

    def __repr__(self):
        return "PublicContainer('{}')".format(self.url)

    def list(self):  # todo: allow refreshing, in case contents have changed
        """List all files in the container.

        Returns
        -------
        list
            List of `hbp_archive.File` objects existing in container.
        """
        if self._content_list is None:
            response = requests.get(self.url, headers={"Accept": "application/json"})
            if response.ok:
                self._content_list = [File(container=self, **entry) for entry in response.json()]
            else:
                raise Exception(response.content)
        return self._content_list

    def get(self, file_path):
        """Return a File object for the file at the given path.

        Parameters
        ----------
        file_path : string
            Path of file to be retrieved.

        Returns
        -------
        `hbp_archive.File`
            Requested `hbp_archive.File` object from container.
        """
        for f in self.list():  # very inefficient
            if f.name == file_path:
                return f
        raise ValueError("Path '{}' does not exist".format(file_path))

    def count(self):
        """Number of files in the container.

        Returns
        -------
        int
            Count of number of files in the container.
        """
        return len(self.list())

    def size(self, units='bytes'):
        """Total size of all data in the container.

        Parameters
        ----------
        units : string
            Requested units for output.
            Options: 'bytes' (default), 'kB', 'MB', 'GB', 'TB'

        Returns
        -------
        float
            Total size of all data in the container in requested units.
        """
        total_bytes = sum(f.bytes for f in self.list())
        return scale_bytes(total_bytes, units)

    def download(self, file_path, local_directory=".", with_tree=True, overwrite=False):
        """Download a file from the container.

        file_path : string
            Path of file to be downloaded.
        local_directory : string, optional
            Local directory path where file is to be saved.
        with_tree : boolean, optional
            Specify if directory structure of file is to be retained.
        overwrite : boolean, optional
            Specify if any already existing file should be overwritten.

        Returns
        -------
        string
             Path of file created inside specified local directory.
        """
        # todo: allow file_path to be a File object
        # todo: implement direct streaming to file without
        #       storing copy in memory, see for example
        #       https://stackoverflow.com/questions/13137817/how-to-download-image-using-requests
        response = requests.get(self.url + "/" + file_path)
        if response.ok:
            contents = response.content
        else:
            raise Exception(response.content)
        if with_tree:
            local_directory = os.path.join(os.path.abspath(local_directory),
                                           *os.path.dirname(file_path).split("/"))
        Path(local_directory).mkdir(parents=True, exist_ok=True)
        local_path = os.path.join(local_directory, os.path.basename(file_path))
        if not overwrite and os.path.exists(local_path):
            raise IOError("Destination file ({}) already exists! Set `overwrite=True` to overwrite file.".format(local_path))
        with open(local_path, 'wb') as local:
            local.write(contents)
        return local_path
        # todo: check hash

    def read(self, file_path, decode='utf-8', accept=[]):
        """Read and return the contents of a file in the container.

        Parameters
        ----------
        file_path : string
            Path of file to be retrieved.
        decode : string, optional
            Files containing text will be decoded using specified encoding
            (default: 'utf-8'). To prevent any attempt at decoding, set `decode=False`.
        accept : boolean, optional
            To force decoding, put the expected content type in `accept`.

        Returns
        -------
        string (unicode)
            Contents of the specified file.
        """
        text_content_types = ["application/json", ]
        response = requests.get(self.url + "/" + file_path)
        if response.ok:
            contents = response.content
            headers = response.headers
        else:
            raise Exception(response.content)
        # todo: check hash
        content_type = headers["Content-Type"]
        if ";" in content_type:
            content_type, encoding = content_type.split(";")
            # todo: handle conflict between encoding and "decode" argument
        ct_parts = content_type.split("/")
        if (ct_parts[0] == "text" or content_type in text_content_types or content_type in accept) and decode:
            return contents.decode(decode)
        else:
            return contents


class Project(object):
    """A representation of a CSCS Project.

    The following actions can be performed:

    ====================================   ====================================
    Action                                 Method / Property
    ====================================   ====================================
    Create a container inside project      :meth:`create_container`
    Rename a container inside project      :meth:`rename_container`
    Delete a container inside project      :meth:`delete_container`
    Get a container from project           :meth:`get_container`
    List containers that you can access    :attr:`containers`
    Get names of containers in project     :attr:`container_names`
    Get mapping of usernames to user ids   :attr:`users`
    ====================================   ====================================
    """

    def __init__(self, project, username, token=None, archive=None):
        if archive is None:
            archive = Archive(username, token=token)
        ks_project = archive._ks_projects[project]
        self.archive = archive
        self.id = ks_project.id
        self.name = ks_project.name
        self._session = None
        self.__connection = None
        self._containers = None
        self._user_id_map = None

    def __str__(self):
        return self.name

    def __repr__(self):
        return "Project('{}', username='{}')".format(self.name, self.archive.username)

    @property
    def _connection(self):
        if self.__connection is None:
            if self._session is None:
                self._set_scope()
            self.__connection = swiftclient.Connection(session=self._session)
        return self.__connection

    def _set_scope(self):
        auth = v3.Token(auth_url=OS_AUTH_URL,
                        token=self.archive._session.get_token(),
                        project_id=self.id)
        self._session = session.Session(auth=auth)

    def _get_container_info(self):
        try:
            headers, containers = self._connection.get_account()
        except ClientException:
            containers = []
        return containers

    def create_container(self, container_name, public=False):
        """
        Create a container inside the current project

        Parameters
        ----------
        container_name : string
            name to be assigned to container
        public : boolean, optional
            specify if container is to be made public; default is private

        Note
        ----
        Use restricted to Superusers/Operators.
        """
        if container_name in self.container_names:
            raise Exception("Container named '{}' already exists!".format(container_name))
        self._connection.put_container(container_name) # doesn't return anything on success
        if public:
            c = self.get_container(container_name)
            c.grant_access("PUBLIC")
        logger.info("Successfully created the container named '{}'".format(container_name))

    def rename_container(self):
        """
        Rename a container inside the current project

        Note
        ----
        Use restricted to Superusers/Operators.
        """
        raise NotImplementedError("It is not possible to directly rename a container."
            "\nSee 'https://bugs.launchpad.net/swift/+bug/1231540' for more details."
            "\nWorkaround: copy contents of existing container to a new container "
            "(with desired name) and then delete the old container.")

    def delete_container(self, container_name):
        """
        Delete a container from the current project

        Parameters
        ----------
        container_name : string
            name of container to be deleted

        Note
        ----
        Use restricted to Superusers/Operators.
        """
        if container_name not in self.container_names:
            raise Exception("Container named '{}' does not exist, or you don't have access to it!".format(container_name))
        c = self.get_container(container_name)
        print("Are you sure you wish to delete the container named '{}' containing '{}' item(s)?".format(container_name, c.count()))
        print("If yes, type in the name of the container to proceed. Any other input will cancel this operation.")
        c_name = raw_input("Input: ")
        if c_name != container_name:
            logger.info("Operation cancelled. Container '{}' is NOT deleted.".format(container_name))
            return
        items = c.list()
        for item in items:
            c.delete(item.name)
        self._connection.delete_container(container_name) # doesn't return anything on success
        logger.info("Successfully deleted the container named '{}'. '{}' item(s) deleted.".format(container_name, c.count()))

    def get_container(self, name):
        """Get a container from project.

        Parameters
        ----------
        name : string
            name of the container to be retrieved.

        Returns
        -------
        'hbp_archive.Container'
            Requested Container object from Project.
        """
        if name not in self.containers:
            container = Container(name, self.archive.username, project=self)
            container.metadata  # check that we can connect to the container
            self._containers[name] = container
        return self.containers[name]

    @property
    def containers(self):
        """Containers you have access to in this project.

        Returns
        -------
        dict
            Dictionary with keys as names of containers and their values being
            the corresponding 'hbp_archive.Container' object.
        """
        if self._containers is None:
            self._containers = {name: Container(name, username=self.archive.username, project=self)
                                for name in self.container_names if not name.endswith("_versions")}
        return self._containers

    @property
    def container_names(self):
        """Returns a list of container names

        Returns
        -------
        list
            List of strings indicating container names in Project.
        """
        return [item['name'] for item in self._get_container_info()]

    @property
    def users(self):
        """Return a mapping from usernames to user ids

        Returns
        -------
        dict
            dict of mapping from usernames to user ids.
        """
        if self._user_id_map is None:
            self._user_id_map = {}
            proj_info = self.containers.get('project_info', None)
            if proj_info:
                user_id_doc = proj_info.read('user_ids', accept=['application/octet-stream'])
                in_user_list = False
                for line in user_id_doc.split("\n"):
                    if line:
                        if line.startswith("# user ids"):
                            in_user_list = True
                        elif in_user_list:
                            user_id, username = line.split(" ")
                            self._user_id_map[user_id] = username
        return self._user_id_map


class Archive(object):
    """A representation of the Human Brain Project archival storage
    (Pollux SWIFT) at CSCS.

    The following actions can be performed:

    ====================================   ====================================
    Action                                 Method / Property
    ====================================   ====================================
    List projects that you can access      :attr:`projects`
    Search for container in all projects   :meth:`find_container`
    ====================================   ====================================
    """

    def __init__(self, username, token=None):
        self.username = username
        if token:
            auth = v3.Token(auth_url=OS_AUTH_URL, token=token)
        else:
            pwd = os.environ.get('CSCS_PASS')
            if not pwd:
                pwd = getpass.getpass("Password: ")
            auth = V3Saml2Password(auth_url=OS_AUTH_URL,
                                   identity_provider=OS_IDENTITY_PROVIDER,
                                   protocol='mapped',
                                   identity_provider_url=OS_IDENTITY_PROVIDER_URL,
                                   username=username,
                                   password=pwd)

        self._session = session.Session(auth=auth)
        self._client = ksclient.Client(session=self._session, interface='public')
        try:
            self.user_id = self._session.get_user_id()
        except AuthorizationFailure:
            raise Exception("Couldn't authenticate! Incorrect username.")
        except IndexError:
            raise Exception("Couldn't authenticate! Incorrect password.")
        self._ks_projects = {ksprj.name: ksprj
                             for ksprj in self._client.projects.list(user=self.user_id)}
        self._projects = None

    @property
    def projects(self):
        """Projects you have access to

        Returns
        -------
        dict
            Dictionary with keys as names of projects and their values being
            the corresponding 'hbp_archive.Project' object.
        """
        if self._projects is None:
            self._projects = {ksprj_name: Project(ksprj_name, username=self.username, archive=self)
                              for ksprj_name in self._ks_projects}
        return self._projects

    def find_container(self, container):
        """
        Search through all projects for the container with the given name.

        Parameters
        ----------
        name : string
            name of the container to be searched

        Returns
        -------
        'hbp_archive.Container'
            Requested Container object from Project.
        """
        for project in self.projects.values():
            try:
                return project.get_container(container)
            except ClientException:
                pass
        raise ValueError(
            "Container {} not found. Please check your access permissions.".format(container))
