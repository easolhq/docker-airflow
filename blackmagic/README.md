# Black Magic

A featherweight module to run Node code in Python.

<center>
  <img src="misc/photo.jpg" width="325" alt="">
  <!-- source: https://en.wikipedia.org/wiki/Black_magic#/media/File:John_Dee_and_Edward_Keeley.jpg -->
</center>

> **black magic** (noun): magic involving the exercise of supernatural powers for the supposed invocation of evil spirits

Currently this is used as a wrapper for [astronomerio/cryptobject](https://github.com/astronomerio/cryptobject) to encrypt and decrypt objects. Our cryptobject is a thin wrapper that uses the underlying library [crypto-js](https://www.npmjs.com/package/crypto-js). While crypto-js is easy to use, the high-level interface that it provides is non-standard. Since we need encryption and decryption to be interoperable across languages, this high-level interface is an issue because equivalent packages in other languages don't provide the same, for instance [cryptography](https://pypi.python.org/pypi/cryptography) in Python.

This module is a workaround for that. It runs Node inside Python and proxies function calls back and forth serializing and deserializing JSON objects as needed. Currently it serves only this use case, however, the underlying code be extended easily to serve other use cases and other languages. The idea was inspired by Airflow's [XComs](https://airflow.incubator.apache.org/concepts.html#xcoms).

## Usage

The following commands assume you're running from the python root dir:

```
cd py
```

To run the example:

```
./run_example.sh
```

(For code, see [example.py](https://github.com/astronomerio/blackmagic/blob/master/py/example.py).)

To run tests:

```
./run_tests.sh
```

To lint:

```
./lint.sh
```

## Setup

There are a couple uncommon steps to make this possible.

Overall, we have to build node from source and define its prefix (root) as our virtual environment. This convince node that "global" is our virtual environment. (If you have node modules installed on your system globally outside of this "global", they will not be picked up. As far as I can tell there is no concept of nested globals.)

Create virtual environment and activate:

```
# (I'm using virtualenvwrapper)
mkproject -p python2 blackmagic
```

*Note that the virtual environment must be Python 2 due to a limitation in Node's build process.*

Install node _inside_ of the virtual environment [(reference)](https://lincolnloop.com/blog/installing-nodejs-and-npm-python-virtualenv/):

```
cdvirtualenv

# grab node lts
curl https://nodejs.org/dist/v6.11.1/node-v6.11.1.tar.gz | tar xvz
cd node-v*
./configure --prefix=${VIRTUAL_ENV}

# build from source (may take a few minutes)
make -j4

# install
make install -j4
```

Test that node is working:

``` 
./node -e "console.log('hello node ' + process.version)"
```

You should see the same version number as running `$ node --version` and `$ which node` should also point to the virtual env version.

Install node dependencies:

```
npm install -g cryptobject minimist
```

Install node dev dependencies (optional - to lint):

```
npm install -g eslint eslint-config-google
```

(Switch to your project directory, then the Python dir root inside that.)

```
# (I'm using virtualenvwrapper)
cdproject
cd py
```

Install Python dependencies:

```
pip install -r requirements.txt
```

Install Python dev dependencies (optional - to lint or run unit tests):

```
pip install -r requirements_dev.txt
```

Run tests, examples, etc:

See section *Usage* above.
