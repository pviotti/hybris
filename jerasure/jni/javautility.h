#ifndef _JAVAUTILITY_H
#define _JAVAUTILITY_H

#include <jni.h>
#include <vector>

jint throwNoClassDefError(JNIEnv *env, char *message);
jint throwOutOfMemoryError(JNIEnv *env, char *message);
jint throwIllegalArgumentException(JNIEnv *env, char* message);

/*
* arrayOfArrays and resultData do not have to be initialized.
*/
bool getArrayOfByteArrays(JNIEnv *env, jobjectArray *arrays, std::vector<jbyteArray> *arrayOfArrays, std::vector<jbyte*> *resultData, int numArrays);
void freeArrayOfByteArrays(JNIEnv *env, std::vector<jbyteArray>* arrayOfArrays, std::vector<jbyte*> *resultData, int numArrays);
#endif