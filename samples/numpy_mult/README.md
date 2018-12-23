## Image: abacosamples/numpy_mult ##

This actor can be used to benchmark Abaco performance. It provides a CPU-bound workload by multiplying two large, random
matrices together and computing the eigenvalues of their product.

### Usage ###

This actor can be used without sending any arguments through the message. Optionally, the `std_dev` and `size` arguments
may be passed in a JSON message.