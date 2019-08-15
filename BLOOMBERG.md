# Installation of Python Bloomberg API

Below we outline the steps you need to install the Python Bloomberg API to access Bloomberg data via findatapy. We are
assuming a Windows installation here, and that we have Bloomberg Terminal and the Desktop API installed already.

However, it should be possible to install the Bloomberg Server API on a Linux/Mac OS X machine and communicate with blpapi,
however I have not tested this.

* To use Bloomberg you will need to have a data licence (either to access Desktop API or Server API)
* Please check your Bloomberg licence before installing anything to understand all its terms and how you can use the data
* Use experimental Python 3.4 version from Bloomberg http://www.bloomberglabs.com/api/libraries/
* Also download C++ version of Bloomberg API and extract into any location
    * eg. C:\blp\blpapi_cpp_3.9.10.1
* For Python 3.5 - need to compile blpapi source using Visual C++ compiler yourself
    * Install Microsoft Visual Studio 2015 (Community Edition is free) and careful to include Visual C++ or it is quicker simply
    to install [Visual C++ 2015 build](http://landinghub.visualstudio.com/visual-cpp-build-tools)
         * You may need to add the following (or similar) to your Windows `PATH`, `C:\Program Files (x86)\Windows Kits\8.1\bin\x64`
         * This should prevent the following compilation error where 'rl.exe' is not found
         * Also we may need to add an environment variable `VS140COMNTOOLS` which points to
         `C:\Program Files (x86)\Microsoft Visual Studio 14.0\Common7\Tools\` so the C++ compiler can be found
    * Before doing do be sure to add environment variables for the Bloomberg DLL (blpapi3_64.dll) to Windows `PATH` variable
        * eg. C:\blp\blpapi_cpp_3.9.10.1\bin
    * Make sure `BLPAPI_ROOT` root is set as an environmental variable in Windows
        * eg. C:\blp\blpapi_cpp_3.9.10.1
    * On Windows to change PATH and BLPAPI_ROOT environment variables go to Control Panel / System and Security /
    System / Advanced System Settings / Advanced / Environment Variables
    * python setup.py build (to build & compile - alternatively if you have Python 3.5 and Windows, you can download file
    [blpapi-3.9.0.zip](https://github.com/cuemacro/findatapy/blob/master/blpapi-3.9.0.zip), which I have
    pre-built using Visual C++ 2015 version 14.0)
    * `python setup.py install` (to install)
* Prebuilt binaries are provided for Python 2.7, 3.5 and 3.6 for Windows in both 32 and 64 bits - which means we can skip the build steps above
    * Might need to tweak registry to avoid "Python 3.4 not found in registry error" (blppython.reg example) when using this executable
    * You can install this prebuilt binary via pip eg.
        * python -m pip install --index-url=https://bloomberg.bintray.com/pip/simple blpapi
    * But note that a local installation of the C++ API is required both for importing the blpapi module in Python
    and for building the module from sources, if needed.
* conda installation for Anaconda
    * It is also possible to install `blpapi` using `conda` (note: I haven't tested this though)
    * `conda install -c conda-forge blpapi`

* The library doesn't support the old Bloomberg COM API (since much slower than the new Open API)
