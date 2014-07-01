#include "javautility.h"
#include "JGalois.h"
#include "galois.h"

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_single_multiply
* Signature: (III)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1single_1multiply
	(JNIEnv *env, jclass clazz, jint a, jint b, jint w)
{
	return galois_single_multiply(a, b, w);
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_single_divide
* Signature: (III)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1single_1divide
	(JNIEnv *env, jclass clazz, jint a, jint b, jint w)
{
	return galois_single_divide(a, b, w);
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_log
* Signature: (II)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1log
	(JNIEnv *env, jclass clazz, jint value, jint w)
{
	return galois_log(value, w);
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_ilog
* Signature: (II)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1ilog
	(JNIEnv *env, jclass clazz, jint value, jint w)
{
	return galois_ilog(value, w);
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_create_log_tables
* Signature: (I)I
*/
JNIEXPORT jboolean JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1create_1log_1tables
	(JNIEnv *env, jclass clazz, jint w)
{
	int result = galois_create_log_tables(w);

	return (result == 0 ? JNI_TRUE : JNI_FALSE);
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_logtable_multiply
* Signature: (III)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1logtable_1multiply
	(JNIEnv *env, jclass clazz, jint x, jint y, jint w)
{
	return galois_logtable_multiply(x, y, w);
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_logtable_divide
* Signature: (III)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1logtable_1divide
	(JNIEnv *env, jclass clazz, jint x, jint y, jint w)
{
	return galois_logtable_divide(x, y, w);
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_create_mult_tables
* Signature: (I)I
*/
JNIEXPORT jboolean JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1create_1mult_1tables
	(JNIEnv *env, jclass clazz, jint w)
{
	int result = galois_create_mult_tables(w);

	return (result == 0 ? JNI_TRUE : JNI_FALSE);
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_multtable_multiply
* Signature: (III)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1multtable_1multiply
	(JNIEnv *env, jclass clazz, jint x, jint y, jint w)
{
	return galois_multtable_multiply(x, y, w);
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_multtable_divide
* Signature: (III)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1multtable_1divide
	(JNIEnv *env, jclass clazz, jint x, jint y, jint w)
{
	return galois_multtable_divide(x, y, w);
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_shift_multiply
* Signature: (III)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1shift_1multiply
	(JNIEnv *env, jclass clazz, jint x, jint y, jint w)
{
	return galois_shift_multiply(x, y, w);
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_shift_divide
* Signature: (III)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1shift_1divide
	(JNIEnv *env, jclass clazz, jint x, jint y, jint w)
{
	return galois_shift_divide(x, y, w);
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_create_split_w8_tables
* Signature: ()I
*/
JNIEXPORT jboolean JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1create_1split_1w8_1tables
	(JNIEnv *env, jclass clazz)
{
	return galois_create_split_w8_tables() == 0 ? JNI_TRUE : JNI_FALSE;
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_split_w8_multiply
* Signature: (II)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1split_1w8_1multiply
	(JNIEnv *env, jclass clazz, jint x, jint y)
{
	return galois_split_w8_multiply(x, y);
}
/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_inverse
* Signature: (II)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1inverse
	(JNIEnv *env, jclass clazz, jint x, jint w)
{
	return galois_inverse(x, w);
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_shift_inverse
* Signature: (II)I
*/
JNIEXPORT jint JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1shift_1inverse
	(JNIEnv *env, jclass clazz, jint y, jint w)
{
	return galois_shift_inverse(y, w);
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_region_xor
* Signature: ([B[B[BI)V
*/
JNIEXPORT void JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1region_1xor
	(JNIEnv *env, jclass clazz, jbyteArray jr1, jbyteArray jr2, jbyteArray jr3, jint nbytes)
{
	jbyte* r1 = env->GetByteArrayElements(jr1, NULL);
	jbyte* r2 = env->GetByteArrayElements(jr2, NULL);
	jbyte* r3 = env->GetByteArrayElements(jr3, NULL);

	if(!(r1 == NULL || r2 == NULL || r3 == NULL)) {
		galois_region_xor((char*)r1, (char*)r2, (char*)r3, nbytes);
	}

	env->ReleaseByteArrayElements(jr1, r1, NULL);
	env->ReleaseByteArrayElements(jr2, r2, NULL);
	env->ReleaseByteArrayElements(jr3, r3, NULL);
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_w08_region_multiply
* Signature: ([BII[BZ)V
*/
JNIEXPORT void JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1w08_1region_1multiply
	(JNIEnv *env, jclass clazz, jbyteArray jregion, jint multby, jint nbytes, jbyteArray jr2, jboolean add)
{
	jbyte* region = env->GetByteArrayElements(jregion, NULL);
	if(region == NULL) {
		throwOutOfMemoryError(env, "Error getting region from Java");
		return;
	}

	jbyte* r2 = NULL;
	if(jr2 != NULL)
	{
		r2 = env->GetByteArrayElements(jr2, NULL);
		if(r2 == NULL) {
			throwOutOfMemoryError(env, "Error getting r2 from Java");

			env->ReleaseByteArrayElements(jregion, region, NULL);
			return;
		}
	}

	galois_w08_region_multiply((char*)region, multby, nbytes, (char*)r2, (add == JNI_TRUE ? 1 : 0));

	env->ReleaseByteArrayElements(jregion, region, NULL);
	env->ReleaseByteArrayElements(jr2, r2, NULL);
}

/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_w16_region_multiply
* Signature: ([BII[BZ)V
*/
JNIEXPORT void JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1w16_1region_1multiply
	(JNIEnv *env, jclass clazz, jbyteArray jregion, jint multby, jint nbytes, jbyteArray jr2, jboolean add)
{
	jbyte* region = env->GetByteArrayElements(jregion, NULL);
	if(region == NULL) {
		throwOutOfMemoryError(env, "Error getting region from Java");
		return;
	}

	jbyte* r2 = NULL;
	if(jr2 != NULL)
	{
		r2 = env->GetByteArrayElements(jr2, NULL);

		if(r2 == NULL) {
			throwOutOfMemoryError(env, "Error getting r2 from Java");

			env->ReleaseByteArrayElements(jregion, region, NULL);

			return;
		}
	}

	galois_w16_region_multiply((char*)region, multby, nbytes, (char*)r2, (add == JNI_TRUE ? 1 : 0));

	env->ReleaseByteArrayElements(jregion, region, NULL);
	env->ReleaseByteArrayElements(jr2, r2, NULL);
}
/*
* Class:     eu_vandertil_jerasure_jni_Galois
* Method:    galois_w32_region_multiply
* Signature: ([BII[BZ)V
*/
JNIEXPORT void JNICALL Java_eu_vandertil_jerasure_jni_Galois_galois_1w32_1region_1multiply
	(JNIEnv *env, jclass clazz, jbyteArray jregion, jint multby, jint nbytes, jbyteArray jr2, jboolean add)
{
	jbyte* region = env->GetByteArrayElements(jregion, NULL);
	if(region == NULL) {
		throwOutOfMemoryError(env, "Error getting region from Java");
		return;
	}

	jbyte* r2 = NULL;
	if(jr2 != NULL)
	{
		r2 = env->GetByteArrayElements(jr2, NULL);

		if(r2 == NULL) {
			throwOutOfMemoryError(env, "Error getting r2 from Java");

			env->ReleaseByteArrayElements(jregion, region, NULL);

			return;
		}
	}

	galois_w32_region_multiply((char*)region, multby, nbytes, (char*)r2, (add == JNI_TRUE ? 1 : 0));

	env->ReleaseByteArrayElements(jregion, region, NULL);
	env->ReleaseByteArrayElements(jr2, r2, NULL);
}
