# Copyright (c) 2017 CNRS
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

Author: Andrew Davison, CNRS

Usage:

    from hbp_archive import Container, Project, Archive

    # Working with a single container

    container = Container("MyContainer", username="xyzabc")  # you will be prompted for your password
    files = container.list()
    local_file = container.download("README.txt")
    number_of_files = container.count()
    size_in_MB = container.size("MB")

    # Working with a project

    sp6 = Project('MyProject', username="xyzabc")
    containers = sp6.containers

    # Listing all your projects

    archive = Archive(username="xyzabc")
    projects = archive.projects
    container = archive.find_container("MyContainer")  # will search through all projects


"""

import getpass, os
from keystoneauth1.identity import v3
from keystoneauth1 import session
from keystoneauth1.extras._saml2 import V3Saml2Password
from keystoneclient.v3 import client as ksclient
import swiftclient.client as swiftclient
from swiftclient.exceptions import ClientException

__version__ = "0.1.0"

OS_AUTH_URL = 'https://pollux.cscs.ch:13000/v3'
OS_IDENTITY_PROVIDER = 'cscskc'
OS_IDENTITY_PROVIDER_URL = 'https://kc.cscs.ch/auth/realms/cscs/protocol/saml/'


class File(object):
    """
    A representation of a file in a container.
    """

    def __init__(self, name, bytes, content_type, hash, last_modified):
        self.name = name
        self.bytes = bytes
        self.content_type = content_type
        self.hash = hash
        self.last_modified = last_modified
    
    def __str__(self):
        return "'{}'".format(self.name)

    def __repr__(self):
        return "'{}'".format(self.name)

    @property
    def dirname(self):
        return os.path.dirname(self.name)

    @property
    def basename(self):
        return os.path.basename(self.name)
    

class Container(object):
    """
    A representation of a storage container, 
    with methods for listing, counting, downloading, etc.
    the files it contains.
    """
    
    def __init__(self, container, username, token=None, project=None):
        if project is None:
            archive = Archive(username, token=token)
            project = archive.find_container(container)
        self.project = project
        self.name = container
        self._metadata = None

    def __str__(self):
        return "'{}/{}'".format(self.project, self.name)

    @property
    def metadata(self):
        """Metadata about the container"""
        if self._metadata is None:
            self._metadata = self.project._connection.head_container(self.name)
        return self._metadata

    def list(self):  #, content_type=None, newer_than=None, older_than=None):
        """List all files in the container."""
        self._metadata, contents = self.project._connection.get_container(self.name)
        return [File(**item) for item in contents]
    
    def count(self):
        """Number of files in the container"""
        return int(self.metadata['x-container-object-count'])

    def size(self, units='bytes'):
        """Total size of all data in the container"""
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
        return int(self.metadata['x-container-bytes-used'])/scale

    def download(self, file_path, local_directory="."):
        """Download a file from the container"""
        headers, contents = self.project._connection.get_object(self.name, file_path)
        local_directory = os.path.join(os.path.abspath(local_directory),
                                       *os.path.dirname(file_path).split("/"))
        os.makedirs(local_directory, exist_ok=True)
        local_path = os.path.join(local_directory, os.path.basename(file_path))
        with open(local_path, 'wb') as local:
            local.write(contents)
        return local_path
        # todo: check hash


class Project(object):
    """
    A representation of a Project,
    with methods for listing containers and users
    associated with that project.
    """
    
    def __init__(self, project, username, token=None, archive=None):
        if archive is None:
            archive = Archive(username, token=token)
        ks_project = archive._ks_projects[project]
        self.archive = archive
        self.id = ks_project.id
        self.name = ks_project.name
        self._session = None
        self._connection = None
        self._containers = None

    def __str__(self):
        return self.name

    def _set_scope(self):
        auth = v3.Token(auth_url=OS_AUTH_URL, 
                        token=self.archive._session.get_token(), 
                        project_id=self.id)
        self._session = session.Session(auth=auth)

    def _get_container_info(self):
        if self._connection is None:
            if self._session is None:
                self._set_scope()
            self._connection = swiftclient.Connection(session=self._session)
        try:
            headers, containers = self._connection.get_account()
        except ClientException:
            containers = []
        return containers

    @property
    def containers(self):
        """Containers you have access to in this project."""
        if self._containers is None:
            self._containers = {name: Container(name, username=self.archive.username, project=self)
                                for name in self.container_names if not name.endswith("_versions")}
        return self._containers

    @property
    def container_names(self):
        return [item['name'] for item in self._get_container_info()]


class Archive(object):
    """
    A representation of the Human Brain Project archival storage (Pollux SWIFT) at CSCS,
    with methods for listing the projects you are associated with,
    and for searching for containers by name.
    """

    def __init__(self, username, token=None):
        self.username = username
        if token:
            auth = v3.Token(auth_url=OS_AUTH_URL, token=token)
        else: 
            pwd = getpass.getpass("Password: ")
            auth = V3Saml2Password(auth_url=OS_AUTH_URL, 
                                   identity_provider=OS_IDENTITY_PROVIDER,
                                   protocol='mapped', 
                                   identity_provider_url=OS_IDENTITY_PROVIDER_URL, 
                                   username=username, 
                                   password=pwd)

        self._session = session.Session(auth=auth)
        self._client  = ksclient.Client(session=self._session, interface='public')
        self.user_id = self._session.get_user_id()
        self._ks_projects = {ksprj.name: ksprj 
                             for ksprj in self._client.projects.list(user=self.user_id)}
        self._projects = None
            
    @property
    def projects(self):
        """Projects you have access to"""
        if self._projects is None:
            self._projects = {ksprj_name: Project(ksprj_name, username=self.username, archive=self)
                              for ksprj_name in self._ks_projects}
        return self._projects

    def find_container(self, container):
        """
        Return the Project that contains the requested container name.
        
        If the container is not found, raise an Exception
        """
        for project in self.projects.values():
            if container in project.container_names:
                return project
