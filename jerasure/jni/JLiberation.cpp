#include <stdlib.h>
#include "JLiberation.h"
#include "javautility.h"
#include "liberation.h"

/*
* Class:     eu_vandertil_jerasure_jni_Liberation
* Method:    liberation_coding_bitmatrix
* Signature: (II)[I
*/
JNIEXPORT jintArray JNICALL Java_eu_vandertil_jerasure_jni_Liberation_liberation_1coding_1bitmatrix
	(JNIEnv *env, jclass clazz, jint k, jint w)
{
	bool outOfMemory = false;
	jintArray result = NULL;

	if(k <= w) {
		int* matrix = liberation_coding_bitmatrix(k,w);
		if(matrix != NULL) {
			result = env->NewIntArray(2*k*w*w);

			if(result != NULL) {
				env->SetIntArrayRegion(result, 0, 2*k*w*w, (jint*)matrix);
			} else {
				outOfMemory = true;
			}
		} else {
			outOfMemory = true;
		}

		free(matrix);
	} else {
		throwIllegalArgumentException(env, "k > w");
	}

	if(outOfMemory) {
		throwOutOfMemoryError(env, "Could not allocate enough memory.");
	}

	return result;
}

/*
* Class:     eu_vandertil_jerasure_jni_Liberation
* Method:    liber8tion_coding_bitmatrix
* Signature: (I)[I
*/
JNIEXPORT jintArray JNICALL Java_eu_vandertil_jerasure_jni_Liberation_liber8tion_1coding_1bitmatrix
	(JNIEnv *env, jclass clazz, jint k)
{
	bool outOfMemory = false;
	jintArray result = NULL;

	if(k <= 8) {
		int* matrix = liber8tion_coding_bitmatrix(k);

		if(matrix != NULL) {
			result = env->NewIntArray(2*k*8*8);
			if(result != NULL) {
				env->SetIntArrayRegion(result, 0, 2*k*8*8, (jint*)matrix);
			} else {
				outOfMemory = true;
			}
		} else {
			outOfMemory = true;
		}

		free(matrix);
	} else {
		throwIllegalArgumentException(env, "k > 8");
	}

	if(outOfMemory) {
		throwOutOfMemoryError(env, "");
	}

	return result;
}

/*
* Class:     eu_vandertil_jerasure_jni_Liberation
* Method:    blaum_roth_coding_bitmatrix
* Signature: (II)[I
*/
JNIEXPORT jintArray JNICALL Java_eu_vandertil_jerasure_jni_Liberation_blaum_1roth_1coding_1bitmatrix
	(JNIEnv *env, jclass clazz, jint k, jint w)
{
	bool outOfMemory = false;
	jintArray result = NULL;

	if(k <= w) {
		int* matrix = blaum_roth_coding_bitmatrix(k, w);

		if(matrix != NULL) {
			result = env->NewIntArray(2*k*w*w);
			if(result != NULL) {
				env->SetIntArrayRegion(result, 0, 2*k*w*w, (jint*)matrix);
			} else {
				outOfMemory = true;
			}
		} else {
			outOfMemory = true;
		}

		free(matrix);
	} else {
		throwIllegalArgumentException(env, "k > w");
	}

	if(outOfMemory){
		throwOutOfMemoryError(env, "");
	}

	return result;
}