#summary Installation Notes
#labels Phase-Deploy,Featured

# Overview #

Do NOT attempt to build from a checkout from the source code repository: that is for wizards only.  Instead, grab one of the released source tarballs from the [downloads](http://code.google.com/p/tcerl/downloads/list) tab.

There are two pieces: tcerldrv, the C driver; and tcerl, the Erlang code.
Both pieces use automake to build.[[1](#1.md)]

# tcerldrv #

  * The default prefix is "/usr".  If you don't like that, make sure to use the --prefix configure option.
    * if you run into strange compilation issues, you can try passing the --disable-hardcore option to configure; this will disable the "treat warnings as errors" flag to the C compiler. At the time I wrote the software, there were no warnings, but as the standard library and compilers evolve that can change.

  * You need pkg-config.
    * Ubuntu/Debian: aptitude install pkg-config
    * OS/X (fink): apt-get install pkgconfig
    * freebsd: [install the port](http://www.freshports.org/devel/pkg-config)
    * others: please let me know so i can update here

  * You need tokyocabinet with development headers; I use version 1.2.5, and haven't tested other versions.
    * Ubuntu (hardy): aptitude install libtokyocabinet-dev
    * others: Download and build the [source](http://downloads.sourceforge.net/tokyocabinet/tokyocabinet-1.2.5.tar.gz?modtime=1209093521&big_mirror=0)

  * You need erl\_driver.h, erl\_marshal.h, and erl\_interface.h.
    * Ubuntu/Debian: aptitude install erlang-dev erlang-src
    * OS/X (fink): apt-get install erlang-otp
    * freebsd: [install the port](http://www.freshports.org/lang/erlang)
    * others: please let me know so i can update here

# tcerl #

  * The default prefix is wherever tcerldrv was installed, if that can be found in the path, otherwise "/usr".  If you don't like that, make sure to use the --prefix configure option.
    * When tcerl is started up, it attempts to auto-detect where the tcerldrv has been installed.  Ideally this always works, but just in case, you can use application:set\_env/3 with the tcerl application variable 'tcerldrvprefix' to let the code know where to look.

  * You need a working Erlang installation.
    * Ubuntu/Debian: aptitude install erlang
    * OS/X (fink): apt-get install erlang-otp
    * freebsd: [install the port](http://www.freshports.org/lang/erlang)
    * others: please let me know so i can update here

  * You need [eunit](http://support.process-one.net/doc/display/CONTRIBS/EUnit) to run the tests.  It's advised you run the tests if you are installing onto an untested environment.
    * If the tests fail, output is in tests/`*`.test.out

# Tested Environments #

  * Ubuntu i386 gusty (32 bit; 64 bit)
  * OS/X 10.4 with fink
    * you might have to tweak your include and linker flags to build tcerldrv if you put erlang somewhere i haven't anticipated (e.g., if you are not using fink).  let me know what you had to do and i'll add it to configure.ac.local for the next release.

# Footnotes #

## 1 ##

More precisely, downloadable source tarballs use automake to build. A source code checkout of the repository uses [framewerk](http://code.google.com/p/fwtemplates) to build, and if you don't know what that is, you probably want to stick with the tarballs.