A high-level API for interacting with the Human Brain Project archival storage at CSCS.

Author: Andrew Davison, CNRS

Usage:

```python
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
```