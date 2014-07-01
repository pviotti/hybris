CC = g++
CFLAGS = -g -O3
# Find Java and set jni header path
JAVAC := $(shell which javac)
JNI_HEADERS_PATH := $(shell readlink -f ${JAVAC} | sed "s:bin/javac::")/include

OBJS = javautility.o JCauchy.o JGalois.o JJerasure.o JLiberation.o JReedSolomon.o

all: ../../lib/libJerasure.jni.so

../../lib/libJerasure.jni.so: ../../lib/libJerasure.jni.so.0
	ln -sf libJerasure.jni.so.0 ../../lib/libJerasure.jni.so

../../lib/libJerasure.jni.so.0: $(OBJS)
	$(CC) -shared -Wl,-soname,libJerasure.jni.so.0 \
		-o ../../lib/libJerasure.jni.so.0 $(OBJS) -Wl,-L../../lib -Wl,-lJerasure

clean: 
	rm -f ../../lib/libJerasure.jni.so* $(OBJS)

%.o: %.cpp
	$(CC) $(CFLAGS) -I$(JNI_HEADERS_PATH) -I$(JNI_HEADERS_PATH)/linux -I ../include -fPIC -c $<

javautility.o: javautility.h
JCauchy.o: JCauchy.h
JGalois.o: JGalois.h
JJerasure.o: JJerasure.h
JLiberation.o: JLiberation.h
JReedSolomon.o: JReedSolomon.h
