## Yo yo yo

This directory contains the following files

### ifsutils.tgz

This is a python library that has been packaged using gzip. The internal structure is identical to that of _ifsutils/ifsutils_

This is copied into the image and stored in the _/opt/spark/pypackages_ directory. 

In order to use this library in an argo workflow 'mainApplication' script, the _/opt/spark/pypackages_ directory must be added to the system path 

This can be done in a python script with the following code
```python
import sys

sys.path.append('/opt/spark/pypackages')
from ifsutils import (your code goes here...)
```

### ifsutils

This is a skeleton around the ifsutils/ifsutils code which can be used to create a pip installable version of the code. 

**Note:** The _wheel_ library **is not used** in the image itself, you don't need to do this to get the example working. This is just for your local machine, so that you have intellisense for the scripts developed which use the custom libraries.

To install using pip:

1. Make any changes necessary to the code in ifsutils/ifsutils
2. Make any changes necessary to the setup.py script
3. Run *python setup.py bdist_wheel*
4. Run _pip install ifsutils/dist/(name of whl)_

We don't use pip to install the library into the image, instead we just bundle the scripts in a tgz and copy them across.

In order to package the files for inclusion in the image

1. Run _tar -czvf ifsutils.tgz ifsutils/ifsutils_
2. If the new ifsutils.tgz is not already in the packages folder of the image, copy it there so that the Dockerfile correctly

Note you can also run _make tar-libs_ to do this

