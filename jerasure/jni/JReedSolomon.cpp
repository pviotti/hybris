#include <stdlib.h>

#include "JReedSolomon.h"
#include "javautility.h"
#include "reed_sol.h"

#define talloc(type, num) (type *) malloc(sizeof(type)*(num))

/*
* Class:     eu_vandertil_jerasure_jni_ReedSolomon
* Method:    reed_sol_vandermonde_coding_matrix
* Signature: (III)[I
*/
JNIEXPORT jintArray JNICALL Java_eu_vandertil_jerasure_jni_ReedSolomon_reed_1sol_1vandermonde_1coding_1matrix
	(JNIEnv *env, jclass clazz, jint k, jint m, jint w)
{
	bool outOfMemory = false;
	jintArray result = NULL;

	if(k <= m) {
		int* matrix = reed_sol_vandermonde_coding_matrix(k, m, w);
		if(matrix != NULL) {
			result = env->NewIntArray(m*k);
			if(result != NULL) {
				env->SetIntArrayRegion(result, 0, m*k, (jint*)matrix);
			} else {
				outOfMemory = true;
			}
		} else {
			outOfMemory = true;
		}

		free(matrix);
	} else {
		throwIllegalArgumentException(env, "k > m");
	}

	if(outOfMemory) {
		throwOutOfMemoryError(env, "Error allocating memory for matrix");
	}

	return result;
}

/*
* Class:     eu_vandertil_jerasure_jni_ReedSolomon
* Method:    reed_sol_extended_vandermonde_matrix
* Signature: (III)[I
*/
JNIEXPORT jintArray JNICALL Java_eu_vandertil_jerasure_jni_ReedSolomon_reed_1sol_1extended_1vandermonde_1matrix
	(JNIEnv *env, jclass clazz, jint rows, jint cols, jint w)
{
	bool outOfMemory = false;
	jintArray result = NULL;

	if(rows <= cols) {
		int* matrix = reed_sol_extended_vandermonde_matrix(rows, cols, w);

		if(matrix != NULL) {
			result = env->NewIntArray(rows*cols);

			if(result != NULL) {
				env->SetIntArrayRegion(result, 0, rows*cols, (jint*)matrix);
			} else {
				outOfMemory = true;
			}
		} else {
			outOfMemory = true;
		}
		free(matrix);
	} else {
		throwIllegalArgumentException(env, "rows > cols");
	}

	if(outOfMemory) {
		throwOutOfMemoryError(env, "");
	}

	return result;
}

/*
* Class:     eu_vandertil_jerasure_jni_ReedSolomon
* Method:    reed_sol_big_vandermonde_distribution_matrix
* Signature: (III)[I
*/
JNIEXPORT jintArray JNICALL Java_eu_vandertil_jerasure_jni_ReedSolomon_reed_1sol_1big_1vandermonde_1distribution_1matrix
	(JNIEnv *env, jclass clazz, jint rows, jint cols, jint w)
{
	bool outOfMemory = false;
	jintArray result = NULL;

	if(rows <= cols) {
		int* matrix = reed_sol_big_vandermonde_distribution_matrix(rows, cols, w);

		if(matrix != NULL) {
			result = env->NewIntArray(rows*cols);

			if(result != NULL) {
				env->SetIntArrayRegion(result, 0, rows*cols, (jint*)matrix);
			} else {
				outOfMemory = true;
			}
		} else {
			outOfMemory = true;
		}

		free(matrix);
	} else {
		throwIllegalArgumentException(env, "rows > cols");
	}

	if(outOfMemory) {
		throwOutOfMemoryError(env, "");
	}

	return result;
}

/*
* Class:     eu_vandertil_jerasure_jni_ReedSolomon
* Method:    reed_sol_r6_encode
* Signature: (II[[B[[BI)I
*/
JNIEXPORT jboolean JNICALL Java_eu_vandertil_jerasure_jni_ReedSolomon_reed_1sol_1r6_1encode
	(JNIEnv *env, jclass clazz, jint k, jint w, jobjectArray jdata_ptrs, jobjectArray jcoding_ptrs, jint size)
{
	int result = 0;
	std::vector<jbyteArray> dataPtrs;
	std::vector<jbyteArray> codingPtrs;
	std::vector<jbyte*> data_ptrs;
	std::vector<jbyte*> coding_ptrs;

	bool getDataDevices = getArrayOfByteArrays(env, &jdata_ptrs, &dataPtrs, &data_ptrs, k);
	bool getCodingDevices = getArrayOfByteArrays(env, &jcoding_ptrs, &codingPtrs, &coding_ptrs, 2);

	if(getDataDevices && getCodingDevices) {
		result = reed_sol_r6_encode(k,w, (char**)&data_ptrs[0], (char**)&coding_ptrs[0],size);
	} else {
		throwOutOfMemoryError(env, "");
	}

	freeArrayOfByteArrays(env, &dataPtrs, &data_ptrs, k);
	freeArrayOfByteArrays(env, &codingPtrs, &coding_ptrs, 2);

	if(result == 1)
		return JNI_TRUE;

	return JNI_FALSE;
}

/*
* Class:     eu_vandertil_jerasure_jni_ReedSolomon
* Method:    reed_sol_r6_coding_matrix
* Signature: (II)[I
*/
JNIEXPORT jintArray JNICALL Java_eu_vandertil_jerasure_jni_ReedSolomon_reed_1sol_1r6_1coding_1matrix
	(JNIEnv *env, jclass clazz, jint k, jint w)
{
	bool outOfMemory = false;
	jintArray result = NULL;

	if (w == 8 || w == 16 || w == 32) {
		int* matrix = reed_sol_r6_coding_matrix(k, w);

		if(matrix != NULL) {
			result = env->NewIntArray(2*k);

			if(result != NULL) {
				env->SetIntArrayRegion(result, 0, 2*k, (jint*)matrix);
			} else {
				outOfMemory = true;
			}
		} else {
			outOfMemory = true;
		}

		free(matrix);
	} else {
		throwIllegalArgumentException(env, "w should be 8, 16 or 32");
	}

	if(outOfMemory) {
		throwOutOfMemoryError(env, "");
	}

	return result;
}

/*
* Class:     eu_vandertil_jerasure_jni_ReedSolomon
* Method:    reed_sol_galois_w08_region_multby_2
* Signature: ([BI)V
*/
JNIEXPORT void JNICALL Java_eu_vandertil_jerasure_jni_ReedSolomon_reed_1sol_1galois_1w08_1region_1multby_12
	(JNIEnv *env, jclass clazz, jbyteArray jregion, jint nbytes)
{
	jbyte* region = env->GetByteArrayElements(jregion, NULL);
	if(region == NULL) {
		throwOutOfMemoryError(env, "Error retrieving region from Java");
		return;
	}

	reed_sol_galois_w08_region_multby_2((char*)region, nbytes);

	env->ReleaseByteArrayElements(jregion, region, NULL);
}

/*
* Class:     eu_vandertil_jerasure_jni_ReedSolomon
* Method:    reed_sol_galois_w16_region_multby_2
* Signature: ([BI)V
*/
JNIEXPORT void JNICALL Java_eu_vandertil_jerasure_jni_ReedSolomon_reed_1sol_1galois_1w16_1region_1multby_12
	(JNIEnv *env, jclass clazz, jbyteArray jregion, jint nbytes)
{
	jbyte* region = env->GetByteArrayElements(jregion, NULL);
	if(region == NULL) {
		throwOutOfMemoryError(env, "Error retrieving region from Java");
		return;
	}

	reed_sol_galois_w16_region_multby_2((char*)region, nbytes);

	env->ReleaseByteArrayElements(jregion, region, NULL);
}
/*
* Class:     eu_vandertil_jerasure_jni_ReedSolomon
* Method:    reed_sol_galois_w32_region_multby_2
* Signature: ([BI)V
*/
JNIEXPORT void JNICALL Java_eu_vandertil_jerasure_jni_ReedSolomon_reed_1sol_1galois_1w32_1region_1multby_12
	(JNIEnv *env, jclass clazz, jbyteArray jregion, jint nbytes)
{
	jbyte* region = env->GetByteArrayElements(jregion, NULL);
	if(region == NULL) {
		throwOutOfMemoryError(env, "Error retrieving region from Java");
		return;
	}

	reed_sol_galois_w32_region_multby_2((char*)region, nbytes);

	env->ReleaseByteArrayElements(jregion, region, NULL);
}
