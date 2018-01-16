#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <stdio.h>
#include <assert.h>
#include <stdbool.h>
#define NUM_THREADS      (3)
#define NUM_BINS         (1)
#define NUM_UPDATE_TIMES (100 * 1024)

/** globals */
aerospike   as;
char       *t1_pattern;
char       *t2_pattern;

pthread_cond_t  start_verify;
pthread_cond_t  verify_complete;
pthread_mutex_t verify_mutex;
bool            start;
bool            complete;

pthread_barrier_t barrier;

void *record_update_thread(void *args)
{
	int tid = *(int *)args;

	as_key key;
	as_error err;
	char *pattern = NULL;
	as_record rec;
	int i;

	assert(t1_pattern && t2_pattern);

	as_key_init(&key, "test", "test_set", "test_key");

	for (i = 0; i < NUM_UPDATE_TIMES; ++i) {
		/** Set thread record update pattern */
		if (tid == 0) {
			t1_pattern = "BA";
			pattern = t1_pattern;
		} else if (tid == 1) {
			t2_pattern = "AB";
			pattern = t2_pattern;
		} else {
			assert(0);
		}
		assert(pattern != NULL);

		/** Write pattern */
		as_record_init(&rec, NUM_BINS);
		as_record_set_str(&rec, "bin_1", pattern);

		if (aerospike_key_put(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
			fprintf(stderr, "err: %d, %s\n", err.code, err.message);
			return NULL;
		}

		pthread_barrier_wait(&barrier);
		pthread_mutex_lock(&verify_mutex);
		if (tid == 0) {
			start = true;
			complete = false;
			/** Signal verifier to verify */
			pthread_cond_signal(&start_verify);
		}
		while (complete == false) {
			/** Wait for verifier to complete */
			pthread_cond_wait(&verify_complete, &verify_mutex);
		}
		pthread_mutex_unlock(&verify_mutex);
	}

	return NULL;
}

void *record_verify_thread(void *args)
{
	int tid = *(int *)args;

	as_key key;
	as_key_init(&key, "test", "test_set", "test_key");
	as_record *p_rec = NULL;
	char *value = NULL;
	as_record rec;
	as_error err;

	int i = 0;
	while (i < NUM_UPDATE_TIMES) {
		pthread_mutex_lock(&verify_mutex);

		while (start == false) {
			/** Wait for start_verify */
			pthread_cond_wait(&start_verify, &verify_mutex);
		}
		start = false;
		pthread_mutex_unlock(&verify_mutex);

		/** Read record to verify */
		if (aerospike_key_get(&as, &err, NULL, &key, &p_rec) != AEROSPIKE_OK) {
			fprintf(stderr, "tid: %d, err: %d, %s\n", tid,
				err.code, err.message);
		}
		value = as_record_get_str(p_rec, "bin_1");
		assert((strncmp(value, t1_pattern, strlen(value)) == 0) ||
			(strncmp(value, t2_pattern, strlen(value)) == 0));

		/** Reset the key's value to "AA" */
		as_record_init(&rec, NUM_BINS);
		as_record_set_str(&rec, "bin_1", "AA");

		if (aerospike_key_put(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
			fprintf(stderr, "err: %d, %s\n", err.code, err.message);
			assert(0);
		}

		pthread_mutex_lock(&verify_mutex);
		complete = true;
		pthread_cond_broadcast(&verify_complete);
		pthread_mutex_unlock(&verify_mutex);

		i++;
	}
	return NULL;
}

int main(void)
{
	as_error  err;
	as_config config;

	int i;
	int rc = 0;

	int thread_args[NUM_THREADS];

	fprintf(stdout, "Experiment started\n");

	as_config_init(&config);
	as_config_add_host(&config, "127.0.0.1", 3000);

	aerospike_init(&as, &config);
	aerospike_connect(&as, &err);

	as_key key;
	as_key_init(&key, "test", "test_set", "test_key");

	/** Initialize a single record with only 1 bin */
	as_record rec;
	as_record_init(&rec, NUM_BINS);
	as_record_set_str(&rec, "bin_1", "AA");

	if (aerospike_key_put(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
		fprintf(stderr, "err: %d, %s\n", err.code, err.message);
		return err.code;
	}

	/** Set the initial pattern of both the threads */
	t1_pattern = "AA";
	t2_pattern = "AA";

	pthread_mutex_init(&verify_mutex, NULL);
	pthread_cond_init(&start_verify, NULL);
	pthread_cond_init(&verify_complete, NULL);
	start = false;
	complete = false;

	rc = pthread_barrier_init(&barrier, NULL, 2);
	assert(rc == 0);

	pthread_t  tids[NUM_THREADS];

	/** Create the verifier thread */
	rc = pthread_create(&tids[2], NULL, record_verify_thread,
		&thread_args[2]);
	assert(rc == 0);

	/** Create all the record update thread */
	for (i = 0; i < NUM_THREADS - 1; ++i) {
		thread_args[i] = i;
		rc = pthread_create(&tids[i], NULL, record_update_thread,
			&thread_args[i]);
		assert(rc == 0);
	}

	for (i = 0; i < NUM_THREADS; ++i) {
		rc = pthread_join(tids[i], NULL);
		assert(rc == 0);
	}

	fprintf(stdout, "Experiment done\n");
	fprintf(stdout, "Single bin, single key updates by multiple"
		" threads, gets updated to right value of either of the"
		" thread\n");

	pthread_mutex_destroy(&verify_mutex);
	pthread_cond_destroy(&start_verify);
	pthread_cond_destroy(&verify_complete);

	aerospike_close(&as, &err);
	aerospike_destroy(&as);

	return 0;
}

