PKGNAME=ar-compute
SPECFILE=${PKGNAME}.spec
FILES=Makefile ${SPECFILE} 

PKGVERSION=$(shell grep -s '^Version:' $(SPECFILE) | sed -e 's/Version: *//')

dist:
	rm -rf dist
	mkdir -p dist/${PKGNAME}-${PKGVERSION}
	cp -pr ${FILES} helpers status-computation dist/${PKGNAME}-${PKGVERSION}/.
	cd dist ; tar cfz ../${PKGNAME}-${PKGVERSION}.tar.gz ${PKGNAME}-${PKGVERSION}
	rm -rf dist

sources: dist

clean:
	rm -rf ${PKGNAME}-${PKGVERSION}.tar.gz
	rm -rf dist

