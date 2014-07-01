#include <stdlib.h>

#include "JJerasure.h"
#include "jerasure.h"
#include "javautility.h"

#define talloc(type, num) (type *) malloc(sizeof(type)*(num))

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_matrix_to_bitmatrix
* Signature: (III[I)[I
*/
JNIEXPORT jintArray JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1matrix_1to_1bitmatrix
	(JNIEnv *env, jclass clazz, jint k, jint m, jint w, jintArray jmatrix)
{
	bool outOfMemory = false;
	jintArray result = NULL;

	jint* matrix = env->GetIntArrayElements(jmatrix, NULL);
	if(matrix != NULL) {
		int* resultMatrix = jerasure_matrix_to_bitmatrix(k, m, w, (int*)matrix);

		if(resultMatrix != NULL) {
			result = env->NewIntArray(k*m*w*w);

			if(result != NULL) {
				env->SetIntArrayRegion(result, 0, k*m*w*w, (jint*)resultMatrix);
			} else {
				outOfMemory = true;
			}
		} else {
			outOfMemory = true;
		}

		free(resultMatrix);
	} else {
		outOfMemory = true;
	}

	if(outOfMemory) {
		throwOutOfMemoryError(env, "Could not allocate memory.");
	}

	env->ReleaseIntArrayElements(jmatrix, matrix, NULL);

	return result;
}

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_do_parity
* Signature: (I[[B[BI)V
*/
JNIEXPORT void JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1do_1parity
	(JNIEnv *env, jclass clazz, jint k, jobjectArray jdata_ptrs, jbyteArray jparity_ptr, jint size)
{
	jbyte* parity_ptr = NULL;
	std::vector<jbyteArray> dataPtrs;
	std::vector<jbyte*> data_ptrs;

	parity_ptr = env->GetByteArrayElements(jparity_ptr, NULL);
	bool getDataDevices = getArrayOfByteArrays(env, &jdata_ptrs, &dataPtrs, &data_ptrs, k);

	if(parity_ptr != NULL && getDataDevices) {
		jerasure_do_parity(k, (char**)&data_ptrs[0], (char*)&parity_ptr[0], size);
	} else {
		throwOutOfMemoryError(env, "Error allocating memory for data");
	}

	env->ReleaseByteArrayElements(jparity_ptr, parity_ptr, NULL);
	freeArrayOfByteArrays(env, &dataPtrs, &data_ptrs, k);
}

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_matrix_encode
* Signature: (III[I[[B[[BI)V
*/
JNIEXPORT void JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1matrix_1encode
	(JNIEnv *env, jclass clazz, jint k, jint m, jint w, jintArray jmatrix, jobjectArray jdata_ptrs, jobjectArray jcoding_ptrs, jint size)
{
	jint* matrix = env->GetIntArrayElements(jmatrix, NULL);
	std::vector<jbyteArray> dataPtrs;
	std::vector<jbyteArray> codingPtrs;
	std::vector<jbyte*> data_ptrs;
	std::vector<jbyte*> coding_ptrs;

	bool getDataDevices = getArrayOfByteArrays(env, &jdata_ptrs, &dataPtrs, &data_ptrs, k);
	bool getCodingDevices = getArrayOfByteArrays(env, &jcoding_ptrs, &codingPtrs, &coding_ptrs, m);

	if(matrix != NULL && getCodingDevices && getCodingDevices) {
		jerasure_matrix_encode(k,m,w, (int*)matrix, (char**)&data_ptrs[0], (char**)&coding_ptrs[0],size);
	} else {
		throwOutOfMemoryError(env, "Could not get coding and data devices from Java");
	}

	env->ReleaseIntArrayElements(jmatrix, matrix, NULL);
	freeArrayOfByteArrays(env, &dataPtrs, &data_ptrs, k);
	freeArrayOfByteArrays(env, &codingPtrs, &coding_ptrs, m);
}

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_bitmatrix_encode
* Signature: (III[I[[B[[BII)V
*/
JNIEXPORT void JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1bitmatrix_1encode
	(JNIEnv *env, jclass clazz, jint k, jint m, jint w, jintArray jbitmatrix, jobjectArray jdata_ptrs, jobjectArray jcoding_ptrs, jint size, jint packetsize)
{
	jint* bitmatrix = env->GetIntArrayElements(jbitmatrix, NULL);
	std::vector<jbyteArray> dataPtrs;
	std::vector<jbyteArray> codingPtrs;
	std::vector<jbyte*> data_ptrs;
	std::vector<jbyte*> coding_ptrs;

	bool getDataDevices = getArrayOfByteArrays(env, &jdata_ptrs, &dataPtrs, &data_ptrs, k);
	bool getCodingDevices = getArrayOfByteArrays(env, &jcoding_ptrs, &codingPtrs, &coding_ptrs, m);

	if(bitmatrix != NULL && getDataDevices && getCodingDevices) {
		jerasure_bitmatrix_encode(k,m,w, (int*)bitmatrix, (char**)&data_ptrs[0], (char**)&coding_ptrs[0], size, packetsize);
	} else {
		throwOutOfMemoryError(env, "Error getting data devices, coding devices and bitmatrix from Java");
	}

	env->ReleaseIntArrayElements(jbitmatrix, bitmatrix, NULL);
	freeArrayOfByteArrays(env, &dataPtrs, &data_ptrs, k);
	freeArrayOfByteArrays(env, &codingPtrs, &coding_ptrs, m);
}

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_matrix_decode
* Signature: (III[IZ[I[[B[[BI)Z
*/
JNIEXPORT jboolean JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1matrix_1decode
	(JNIEnv *env, jclass clazz, jint k, jint m, jint w, jintArray jmatrix, jboolean row_k_ones, jintArray jerasures, jobjectArray jdata_ptrs, jobjectArray jcoding_ptrs, jint size)
{
	int result = -1;
	jint *erasures = NULL, *matrix = NULL;
	std::vector<jbyteArray> dataPtrs;
	std::vector<jbyteArray> codingPtrs;
	std::vector<jbyte*> data_ptrs;
	std::vector<jbyte*> coding_ptrs;

	erasures = env->GetIntArrayElements(jerasures, NULL);
	matrix = env->GetIntArrayElements(jmatrix, NULL);
	bool getDataDevices = getArrayOfByteArrays(env, &jdata_ptrs, &dataPtrs, &data_ptrs, k);
	bool getCodingDevices = getArrayOfByteArrays(env, &jcoding_ptrs, &codingPtrs, &coding_ptrs, m);

	if(erasures != NULL && matrix != NULL && getDataDevices && getCodingDevices)
	{
		result = jerasure_matrix_decode(k, m, w, (int*)matrix, (row_k_ones == JNI_TRUE ? 1 : 0), (int*)erasures, (char**)&data_ptrs[0], (char**)&coding_ptrs[0], size);
	} else {
		throwOutOfMemoryError(env, "");
	}

	env->ReleaseIntArrayElements(jmatrix, matrix, NULL);
	env->ReleaseIntArrayElements(jerasures, erasures, NULL);
	freeArrayOfByteArrays(env, &dataPtrs, &data_ptrs, k);
	freeArrayOfByteArrays(env, &codingPtrs, &coding_ptrs, m);

	if(result == 0)
		return JNI_TRUE;

	return JNI_FALSE;
}

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_bitmatrix_decode
* Signature: (III[IZ[I[[B[[BII)Z
*/
JNIEXPORT jboolean JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1bitmatrix_1decode
	(JNIEnv *env, jclass clazz, jint k, jint m, jint w, jintArray jbitmatrix, jboolean row_k_ones, jintArray jerasures, jobjectArray jdata_ptrs, jobjectArray jcoding_ptrs, jint size, jint packetsize)
{
	int result = -1;
	jint *bitmatrix = NULL, *erasures = NULL;
	std::vector<jbyteArray> dataPtrs;
	std::vector<jbyteArray> codingPtrs;
	std::vector<jbyte*> data_ptrs;
	std::vector<jbyte*> coding_ptrs;

	bitmatrix = env->GetIntArrayElements(jbitmatrix, NULL);
	erasures = env->GetIntArrayElements(jerasures, NULL);
	bool getDataDevices = getArrayOfByteArrays(env, &jdata_ptrs, &dataPtrs, &data_ptrs, k);
	bool getCodingDevices = getArrayOfByteArrays(env, &jcoding_ptrs, &codingPtrs, &coding_ptrs, m);

	if(bitmatrix != NULL && erasures != NULL && getDataDevices && getCodingDevices) {
		result = jerasure_bitmatrix_decode(k,m,w, (int*)bitmatrix, (row_k_ones == JNI_TRUE ? 1 : 0), (int*)erasures, (char**)&data_ptrs[0], (char**)&coding_ptrs[0], size, packetsize);
	} else {
		throwOutOfMemoryError(env, "");
	}

	env->ReleaseIntArrayElements(jbitmatrix, bitmatrix, NULL);
	env->ReleaseIntArrayElements(jerasures, erasures, NULL);
	freeArrayOfByteArrays(env, &dataPtrs, &data_ptrs, k);
	freeArrayOfByteArrays(env, &codingPtrs, &coding_ptrs, m);

	if(result == 0)
		return JNI_TRUE;

	return JNI_FALSE;
}

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_make_decoding_matrix
* Signature: (III[I[Z[I[I)Z
*/
JNIEXPORT jboolean JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1make_1decoding_1matrix
	(JNIEnv *env, jclass clazz, jint k, jint m, jint w, jintArray jmatrix, jbooleanArray jerased, jintArray jdecoding_matrix, jintArray jdm_ids)
{
	int result = -1;
	int *erased = talloc(int, k+m);
	jboolean* erasedj = env->GetBooleanArrayElements(jerased, NULL);
	jint* matrix = env->GetIntArrayElements(jmatrix, NULL);
	jint* decoding_matrix = env->GetIntArrayElements(jdecoding_matrix, NULL);
	jint* dm_ids = env->GetIntArrayElements(jdm_ids, NULL);

	if(matrix != NULL && erased != NULL && erasedj != NULL && matrix != NULL && decoding_matrix != NULL && dm_ids != NULL) {
		for(int i = 0; i < (k+m); ++i) {
			if(erasedj[i] == JNI_TRUE) {
				erased[i] = 1;
			} else {
				erased[i] = 0;
			}
		}

		result = jerasure_make_decoding_matrix(k, m, w, (int*)matrix, erased, (int*)decoding_matrix, (int*)dm_ids);
	} else {
		throwOutOfMemoryError(env, "");
	}

	env->ReleaseBooleanArrayElements(jerased, erasedj, NULL);
	env->ReleaseIntArrayElements(jmatrix, matrix, NULL);
	env->ReleaseIntArrayElements(jdecoding_matrix, decoding_matrix, NULL);
	env->ReleaseIntArrayElements(jdm_ids, dm_ids, NULL);

	free(erased);

	if(result == 0)
		return JNI_TRUE;

	return JNI_FALSE;
}

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_make_decoding_bitmatrix
* Signature: (III[I[Z[I[I)Z
*/
JNIEXPORT jboolean JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1make_1decoding_1bitmatrix
	(JNIEnv *env, jclass clazz, jint k, jint m, jint w, jintArray jmatrix, jbooleanArray jerased, jintArray jdecoding_matrix, jintArray jdm_ids)
{
	int result = -1;
	int* erased = talloc(int, k+m);
	jboolean* erasedj = env->GetBooleanArrayElements(jerased, NULL);
	jint* dm_ids = env->GetIntArrayElements(jdm_ids, NULL);
	jint* decoding_matrix = env->GetIntArrayElements(jdecoding_matrix, NULL);
	jint* matrix = env->GetIntArrayElements(jmatrix, NULL);

	if(erasedj != NULL && erased != NULL && dm_ids != NULL && decoding_matrix != NULL && matrix != NULL) {
		for(int i = 0; i < k+m; ++i) {
			if(erasedj[i] == JNI_TRUE) {
				erased[i] = 1;
			} else {
				erased[i] = 0;
			}
		}

		result = jerasure_make_decoding_bitmatrix(k, m, w, (int*)matrix, erased, (int*)decoding_matrix, (int*)dm_ids);

	} else {
		throwOutOfMemoryError(env, "");
	}

	free(erased);
	env->ReleaseBooleanArrayElements(jerased, erasedj, NULL);
	env->ReleaseIntArrayElements(jdm_ids, dm_ids, NULL);
	env->ReleaseIntArrayElements(jdecoding_matrix, decoding_matrix, NULL);
	env->ReleaseIntArrayElements(jmatrix, matrix, NULL);

	if(result == 0)
		return JNI_TRUE;

	return JNI_FALSE;
}

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_erasures_to_erased
* Signature: (II[I)[Z
*/
JNIEXPORT jbooleanArray JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1erasures_1to_1erased
	(JNIEnv *env, jclass clazz, jint k, jint m, jintArray jerasures)
{
	bool outOfMemory = false;
	jbooleanArray result;

	jint* erasures = env->GetIntArrayElements(jerasures, NULL);

	if(erasures != NULL) {
		int *erased = jerasure_erasures_to_erased(k,m,(int*)erasures);
		if(erased != NULL)
		{
			result = env->NewBooleanArray(k+m);

			if(result != NULL) {
				jboolean* resultValues = env->GetBooleanArrayElements(result, NULL);

				for(int i = 0; i < k+m; ++i) {
					resultValues[i] = (erased[i] == 1 ? JNI_TRUE : JNI_FALSE);
				}

				env->ReleaseBooleanArrayElements(result, resultValues, NULL);
			} else {
				outOfMemory = true;
			}
		} else {
			outOfMemory = true;
		}

		free(erased);
	} else {
		outOfMemory = true;
	}

	if(outOfMemory) {
		throwOutOfMemoryError(env, "");
	}

	env->ReleaseIntArrayElements(jerasures, erasures, NULL);

	return result;
}

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_matrix_dotprod
* Signature: (II[I[II[[B[[BI)V
*/
JNIEXPORT void JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1matrix_1dotprod
	(JNIEnv *env, jclass clazz, jint, jint, jintArray, jintArray, jint, jobjectArray, jobjectArray, jint);

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_bitmatrix_dotprod
* Signature: (II[I[II[[B[[BII)V
*/
JNIEXPORT void JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1bitmatrix_1dotprod
	(JNIEnv *env, jclass clazz, jint, jint, jintArray, jintArray, jint, jobjectArray, jobjectArray, jint, jint);

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_invert_matrix
* Signature: ([I[III)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1invert_1matrix
	(JNIEnv *env, jclass clazz, jintArray, jintArray, jint, jint);

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_invert_bitmatrix
* Signature: ([I[II)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1invert_1bitmatrix
	(JNIEnv *env, jclass clazz, jintArray, jintArray, jint);

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_invertible_matrix
* Signature: ([III)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1invertible_1matrix
	(JNIEnv *env, jclass clazz, jintArray, jint, jint);

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_invertible_bitmatrix
* Signature: ([II)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1invertible_1bitmatrix
	(JNIEnv *env, jclass clazz, jintArray, jint);

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_print_matrix
* Signature: ([IIII)V
*/
JNIEXPORT void JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1print_1matrix
	(JNIEnv *env, jclass clazz, jintArray, jint, jint, jint);

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_print_bitmatrix
* Signature: ([IIII)V
*/
JNIEXPORT void JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1print_1bitmatrix
	(JNIEnv *env, jclass clazz, jintArray, jint, jint, jint);

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_matrix_multiply
* Signature: ([I[IIIIII)[I
*/
JNIEXPORT jintArray JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1matrix_1multiply
	(JNIEnv *env, jclass clazz, jintArray, jintArray, jint, jint, jint, jint, jint);

/*
* Class:     eu_vandertil_jerasure_jni_Jerasure
* Method:    jerasure_get_stats
* Signature: ([D)V
*/
JNIEXPORT void JNICALL Java_eu_vandertil_jerasure_jni_Jerasure_jerasure_1get_1stats
	(JNIEnv *env, jclass clazz, jdoubleArray);