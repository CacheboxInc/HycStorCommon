#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <stdio.h>
#include <assert.h>

#define NUM_THREADS      (2)
#define NUM_BINS         (2)
#define NUM_UPDATE_TIMES (100 * 1024)

/** globals */
aerospike as;

void *record_verify_thread(void *args)
{
	as_error err;
	int tid = *(int *)args;
	as_record *p_rec = NULL;
	int i;

	as_key key;
	as_key_init(&key, "test", "test_set", "test_key");

	/** Initial value of A was set in both the bins in main() */
	char check_value = 'A';
	char new_value;
	char *value = NULL;
	char *bin = NULL;
	as_record rec;

	for (i = 0; i < NUM_UPDATE_TIMES; ++i) {
		if (aerospike_key_get(&as, &err, NULL, &key, &p_rec) != AEROSPIKE_OK) {
			fprintf(stderr, "Thread: %d, err: %d, %s\n", tid,
				err.code, err.message);
			assert(0);
		}

		/** Thread 0 writes to bin_1 */
		if (tid == 0) {
			value = as_record_get_str(p_rec, "bin_1");
			bin = "bin_1";

		} else if (tid == 1) {
		/** Thread 1 writes to bin_2 */
			value = as_record_get_str(p_rec, "bin_2");
			bin = "bin_2";
		} else {
			assert(0);
		}
		assert(value != NULL);

		/** Verify data */
		assert(check_value == value[0]);

		//new_value = value[0] + (i +1) % 26;
		new_value = 'A' + ((value[0] - 'A') + 1) % 26;
		char new_value_str[2] = {new_value, '\0'};

		as_record_init(&rec, 1);
		as_record_set_str(&rec, bin, new_value_str);
		if (aerospike_key_put(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
			fprintf(stderr, "err:%d, %s\n", err.code, err.message);
			assert(0);
		}
		check_value = new_value;
	}

	return NULL;
}

int main(void)
{
	as_error err;
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

	/** Initialize a single record with 2 bins */
	as_record rec;
	as_record_init(&rec, NUM_BINS);
	as_record_set_str(&rec, "bin_1", "A");
	as_record_set_str(&rec, "bin_2", "A");

	if (aerospike_key_put(&as, &err, NULL, &key, &rec) != AEROSPIKE_OK) {
		fprintf(stderr, "err:%d, %s\n", err.code, err.message);
		return err.code;
	}

	pthread_t tids[NUM_THREADS];

	for (i = 0; i < NUM_THREADS; ++i) {
		thread_args[i] = i;
		rc = pthread_create(&tids[i], NULL, record_verify_thread,
			&thread_args[i]);
		assert(rc == 0);
	}

	for (i = 0; i < NUM_THREADS; ++i) {
		rc = pthread_join(tids[i], NULL);
		assert(rc == 0);
	}

	fprintf(stdout, "Experiment complete\n");
	fprintf(stdout, "Multiple bins for a single key updates"
		" are transactional\n");

	aerospike_close(&as, &err);
	aerospike_destroy(&as);

	return 0;
}

