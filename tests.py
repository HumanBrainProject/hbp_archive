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
Unit tests for hbp_archive

"""

import os
import mock
from unittest import TestCase
from hbp_archive import Archive, Project, Container, PublicContainer


class ArchiveTest(TestCase):

    @classmethod
    @mock.patch('getpass.getpass')
    def setUpClass(cls, getpw):
        getpw.return_value = os.environ["HBP_ARCHIVE_PASSWORD"]
        username = os.environ["HBP_ARCHIVE_USERNAME"]
        cls.arch = Archive(username)

    def test_project_list(self):
        projects = self.arch.projects
        self.assertEqual(sorted([p.name for p in projects.values()]),
                         ["bp0", "bp00sp01", "bp00sp06"])

    def test_find_container(self):
        container_name = "sp6_validation_data"
        container = self.arch.find_container(container_name)
        self.assertEqual(container.name, container_name)
        self.assertEqual(container.project.name, "bp00sp06")

    def test_find_container_with_invalid_name(self):
        self.assertRaises(ValueError, self.arch.find_container, "iucghaiwgcmazic84")


class ProjectTest(TestCase):

    @classmethod
    @mock.patch('getpass.getpass')
    def setUpClass(cls, getpw):
        getpw.return_value = os.environ["HBP_ARCHIVE_PASSWORD"]
        username = os.environ["HBP_ARCHIVE_USERNAME"]
        cls.prj = Project("bp00sp06", username)

    def test_repr(self):
        self.assertEqual(self.prj.name, "bp00sp06")
        self.assertEqual(str(self.prj), "bp00sp06")
        self.assertEqual(repr(self.prj),
                         "Project('{}', username='{}')".format(self.prj.name,
                                                               self.prj.archive.username))

    def test_users(self):
        self.assertEqual(self.prj.users, {})  # empty for normal user account


class ContainerTest(TestCase):

    @classmethod
    @mock.patch('getpass.getpass')
    def setUpClass(cls, getpw):
        getpw.return_value = os.environ["HBP_ARCHIVE_PASSWORD"]
        username = os.environ["HBP_ARCHIVE_USERNAME"]
        cls.container = Container("sp6_validation_data", username)

    def test_repr(self):
        self.assertEqual(repr(self.container),
                         "Container('{}', project='{}', username='{}')".format(
            self.container.name,
            self.container.project.name,
            self.container.project.archive.username))
        self.assertEqual(str(self.container),
                         "'{}/{}'".format(self.container.project, self.container.name))

    def test_list(self):
        self.assertIn("README.txt", [f.name for f in self.container.list()])

    def test_count(self):
        self.assertGreater(self.container.count(), 0)

    def test_size(self):
        size_bytes = self.container.size()
        size_kB = self.container.size('kB')
        size_TB = self.container.size('TB')
        self.assertGreater(size_bytes, 0)
        self.assertLess(size_TB, 1)
        self.assertEqual(size_kB * 1024, size_bytes)
        self.assertRaises(ValueError, self.container.size, 'cats')

    def test_read(self):
        content = self.container.read("README.txt")
        self.assertGreater(len(content), 0)

    def test_access_control(self):
        self.assertEqual(self.container.access_control(),
                         {'read': [], 'write': []})  # empty for normal user account

    def test_download(self):
        test_filename = "README.txt"
        tmp_testdir = "tmp_test"
        expected_local_path = os.path.abspath(os.path.join(tmp_testdir, test_filename))
        if os.path.exists(expected_local_path):
            os.remove(expected_local_path)
        
        local_path = self.container.download(test_filename, local_directory=tmp_testdir)
        self.assertEqual(local_path, expected_local_path)
        self.assert_(os.path.exists(local_path))

        os.remove(local_path)


class PublicContainerTest(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.container = PublicContainer("https://object.cscs.ch/v1/AUTH_c0a333ecf7c045809321ce9d9ecdfdea/sp6_validation_data")

    def test_repr(self):
        self.assertEqual(repr(self.container),
                         "PublicContainer('{}')".format(self.container.url))
        self.assertEqual(str(self.container),
                         self.container.url)

    def test_list(self):
        self.assertIn("README.txt", [f.name for f in self.container.list()])

    def test_count(self):
        self.assertGreater(self.container.count(), 0)

    def test_size(self):
        size_bytes = self.container.size()
        size_kB = self.container.size('kB')
        size_TB = self.container.size('TB')
        self.assertGreater(size_bytes, 0)
        self.assertLess(size_TB, 1)
        self.assertEqual(size_kB * 1024, size_bytes)
        self.assertRaises(ValueError, self.container.size, 'cats')

    def test_read(self):
        content = self.container.read("README.txt")
        self.assertGreater(len(content), 0)

    def test_download(self):
        test_filename = "README.txt"
        tmp_testdir = "tmp_test"
        expected_local_path = os.path.abspath(os.path.join(tmp_testdir, test_filename))
        if os.path.exists(expected_local_path):
            os.remove(expected_local_path)
        
        local_path = self.container.download(test_filename, local_directory=tmp_testdir)
        self.assertEqual(local_path, expected_local_path)
        self.assert_(os.path.exists(local_path))

        os.remove(local_path)


class FileTest(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.container = PublicContainer("https://object.cscs.ch/v1/AUTH_c0a333ecf7c045809321ce9d9ecdfdea/sp6_validation_data")

    def test_read(self):
        content1 = self.container.read("README.txt")
        content2 = self.container.get("README.txt").read()
        self.assertEqual(content1, content2)
