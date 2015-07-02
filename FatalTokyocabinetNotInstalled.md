
```
On Mon, 31 Aug 2009, Roger Critchlow wrote:

> Hi --
>
> I'm trying to build tcerldrv-1.3.1g from .tar.gz on Ubuntu,
> ./configure fails with:
>
> ?? checking for native package type... deb (autodetected)
> ?? checking for build dependencies... package/deb/dependency-closure:
> fatal: 'tokyocabinet' not installed
>
> pkg-config --exists tokyocabinet returns true, and the following are
installed:
>
> ii? libtokyocabinet-dbg??????????????? 1.2.1-1
> ?? Tokyo Cabinet Database Libraries [runtime]
> ii? libtokyocabinet-dev??????????????? 1.2.1-1
> ?? Tokyo Cabinet Database Libraries [development]
> ii? libtokyocabinet3?????????????????? 1.2.1-1
> ?? Tokyo Cabinet Database Libraries [runtime]
> ii? tokyocabinet-bin?????????????????? 1.2.1-1
> ?? Tokyo Cabinet Database Utilities
> ii? tokyocabinet-doc?????????????????? 1.2.1-1
> ?? Tokyo Cabinet Database Documentation
>
> Any idea why this is failing or how to get around it?
>
> Thanks,
>
> -- rec --
>

Yes.  This is an advanced error, actually.

The issue is that configure is detecting debian as the native package
type.  This means that you have installed the framewerk package chain as
debian packages (cool).  What you are now encountering is a "distribution"
issue ... namely, I use Ubuntu but augment it with packages as needed and
thus I'm effectively building my own distribution.  I'm using hardy (LTS)
which lacks tokyocabinet packages so I made one. Later versions of Ubuntu
have added a tokyocabinet package but called it something different.
Ideally I would've used the same names as later distributions but I
didn't.

You can edit fw-pkgin/config and replace the build dependency
"tokyocabinet" with "libtokyocabinet-dev" and replace the package
dependency "tokyocabinet" with "libtokyocabinet3".

FW_PACKAGE_DEPENDS="libtokyocabinet3, mnesia"
FW_PACKAGE_BUILD_DEPENDS="libtokyocabinet-dev, mnesia"

The next problem you may encounter will be "fatal: 'mnesia' not
installed".  mnesia is the name of the debian package that I made to
contain the mnesiaex patches.  The mnesiaex project can make this package
for you.  If you are not planning to use tcerl via mnesia, but just
directly (e.g., tcbdbets), you can remove this dependency and you'll get a
build without the mnesia extensions.

Cheers,

-- p
```