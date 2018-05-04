

#include <jni.h>
#include <sys/types.h>
#include <sys/stat.h>




JNIEXPORT jlong JNICALL Java_Vdb_Native_chmod(JNIEnv *env,
                                              jclass  this,
                                              jstring fname_in)
{
  char *fname = (char*) (*env)->GetStringUTFChars(env, fname_in, 0);

  //printf("fname: %s\n", fname);
  int rc = chmod(fname, 0777);

  (*env)->ReleaseStringUTFChars(env, fname_in, fname);

  return (jlong) rc;
}
