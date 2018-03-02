## Image: abacosamples/py3_func ##

This image builds off the abacosamples/py3_func base image with some standard scientific libraries
such as numpy, scipy and matplotlib.


### Usage ###

Define a function that leverages numpy to compute the eigenvalues of the dot product of two matrices
with a given size and standard deviation.
```shell
import numpy as np

def f(std_dev, size):
    A = np.random.normal(0, std_dev, (size, size))
    B = np.random.normal(0, std_dev, (size, size))
    C = np.dot(A, B)
    r = np.linalg.eig(C)[0]
    return r
```

Create an agavepy client and an Abaco Executor that utilizes this image:
```shell
ag = Agave(api_server='https://api.tacc.cloud', token='my_token')
ex = AbacoExecutor(ag, image='abacosamples/py3_sci_base_func')
```

Call the function using the executor synchronously:
```shell
r = ex.blocking_call(f, 100,4096);
```

or asynchronously:
```shell
arsp = ex.submit(f, 100, 4096)
```

check if the result is done:
```shell
arsp.done()
Out: False
```

or just block until the result is ready:
```shell
arsp.result()
Out:
[array([-1.77582237e+07+37571575.56250072j,
        -1.77582237e+07-37571575.56250072j,
        -2.67116990e+07+30528964.29646419j, ...,
         2.45206251e+04       +0.        j,
        -7.58916091e+03       +0.        j,
         1.92003661e+03       +0.        j])]
```
