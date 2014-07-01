PREFIX=${HOME}/local
BINDIR=${PREFIX}/bin
LIBDIR=${PREFIX}/../lib
INCDIR=${PREFIX}/include

all: ../lib/libJerasure.so ../lib/libJerasure.jni.so

clean:
	make -C jni clean
	make -C src clean
	rm -rf include

install: lib/libJerasure.so
	cp -P lib/libJerasure.so* ${LIBDIR}
	mkdir -p ${INCDIR}/jerasure
	cp include/* ${INCDIR}/jerasure

jni: lib/libJerasure.jni.so

../lib/libJerasure.so:
	make -C src

../lib/libJerasure.jni.so: ../lib/libJerasure.so
	make -C jni
