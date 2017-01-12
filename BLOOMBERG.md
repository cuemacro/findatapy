# Installation of Python Bloomberg API

Below we outline the steps you need to install the Python Bloomberg API to access Bloomberg data via findatapy. We are
assuming a Windows installation here (for example if you have a Bloomberg Terminal). However, it should be possible to install
the Server API on a Linux box.

* To use Bloomberg you will need to have a licence (either to access Desktop API or Server API)
* Use experimental Python 3.4 version from Bloomberg http://www.bloomberglabs.com/api/libraries/
* Also download C++ version of Bloomberg API and extract into any location
    * eg. C:\blp\blpapi_cpp_3.9.10.1
* For Python 3.5 - need to compile blpapi source using Microsoft Visual Studio 2015 yourself
    * Install Microsoft Visual Studio 2015 (Community Edition is free)
    * Before doing do be sure to add environment variables for the Bloomberg DLL (blpapi3_64.dll) to PATH variable
        * eg. C:\blp\blpapi_cpp_3.9.10.1\bin
    * Make sure BLPAPI_ROOT root is set as an environmental variable in Windows
        * eg. C:\blp\blpapi_cpp_3.9.10.1
    * On Windows to change PATH and BLPAPI_ROOT environment variables go to Control Panel / System and Security /
    System / Advanced System Settings / Advanced / Environment Variables
    * python setup.py build
    * python setup.py install
* For Python 3.4 - prebuilt executable can be run, which means we can skip the build steps above
    * Might need to tweak registry to avoid "Python 3.4 not found in registry error" (blppython.reg example) when using this executable
* The library doesn't support the old Bloomberg COM API (since much slower than the new Open API)
