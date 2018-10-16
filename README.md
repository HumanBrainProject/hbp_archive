A high-level API for interacting with the Human Brain Project archival storage at CSCS.

Authors: Andrew Davison and Shailesh Appukuttan, CNRS

Documentation: https://hbp-archive.readthedocs.io

Installation: `pip install hbp_archive`

Usage:

```python
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
```
