#include "javautility.h"

jint throwNoClassDefError(JNIEnv *env, char *message) {
	jclass exClass;
	char *className = "java/lang/NoClassDefFoundError";

	exClass = env->FindClass(className );
	if ( exClass == NULL )
	{
		return throwNoClassDefError(env, className );
	}

	return env->ThrowNew(exClass, message );
}

jint throwOutOfMemoryError(JNIEnv *env, char *message) {
	jclass exClass;
	char *className = "java/lang/OutOfMemoryError";

	exClass = env->FindClass(className);
	if(exClass == NULL)
	{
		return throwNoClassDefError(env, className);
	}

	return env->ThrowNew(exClass, message);
}

jint throwIllegalArgumentException(JNIEnv *env, char* message) {
	jclass exClass;
	char* className = "java/lang/IllegalArgumentException";

	exClass = env->FindClass(className);
	if(exClass == NULL)
	{
		return throwNoClassDefError(env, className);
	}

	return env->ThrowNew(exClass, message);
}

bool getArrayOfByteArrays(JNIEnv *env, jobjectArray *arrays, std::vector<jbyteArray> *arrayOfArrays, std::vector<jbyte*> *resultData, int numArrays)
{
	if(arrays == NULL || arrayOfArrays == NULL || resultData == NULL) {
		return false;
	}

	for(int i = 0; i < numArrays; ++i) {
		jbyteArray array = (jbyteArray)env->GetObjectArrayElement(*arrays, i);
		arrayOfArrays->push_back(array); // It seems GetObjectArrayElement can't fail? Maybe check for NULL?
		resultData->push_back(env->GetByteArrayElements(arrayOfArrays->back(), NULL));

		if(resultData->back() == NULL) {
			freeArrayOfByteArrays(env, arrayOfArrays, resultData, i);
			return false;
		}
	}

	return true;
}

/*
* Can be used to clean up arrays retrieved from getArrayOfByteArrays()
*/
void freeArrayOfByteArrays(JNIEnv *env, std::vector<jbyteArray> *arrayOfArrays, std::vector<jbyte*> *resultData, int numArrays)
{
	int i;
	if(arrayOfArrays != NULL && resultData != NULL) {
		for(i = 0; i < numArrays; ++i) {
			env->ReleaseByteArrayElements(arrayOfArrays->at(i), resultData->at(i), NULL);
			env->DeleteLocalRef(arrayOfArrays->at(i));
		}
	}
}
