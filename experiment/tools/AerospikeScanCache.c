#include <stddef.h>
#include <stdlib.h>
#include <getopt.h>
#include <unistd.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_record.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_status.h>
#include <aerospike/as_val.h>

#include "../../src/include/cksum.h"
#include "../../thirdparty/hyc-datapack/src/include/hyc_compress.h"
#include "../../thirdparty/hyc-datapack/src/include/hyc_encrypt.h"
#include "../../thirdparty/hyc-datapack/src/include/hyc_datapack.h"

#define CACHE_BIN "data_map"
#define OUTPUT_FILE "/tmp/aero_scan_output"

#define BLK_SZ 4096
#define IS_ENCRYPTED 1

const char SHORT_OPTS[] = "h:p:n:s:v:c:b:t:";
const struct option LONG_OPTS[] = {
	{"hosts",     required_argument, 0, 'h'},
	{"port",      optional_argument, 0, 'p'},
	{"namespace", required_argument, 0, 'n'},
	{"set",       required_argument, 0, 's'},
	{"vmdkid",    required_argument, 0, 'v'},
	{"ckpt",      required_argument, 0, 'c'},
	{"blk_sz",    optional_argument, 0, 'b'},
	{"cache_type",optional_argument, 0, 't'},
	{0, 0, 0, 0}
};

typedef struct vm_props {
	int vmdkid;
	int ckpt;
	int blk_sz;
	int cache_type;     // 0 for parent, 1 for child
#ifdef IS_ENCRYPTED
	hyc_encrypt_ctx_t *ectx;
	hyc_compress_ctx_t *ctx;
#endif
} key_props_t;

int fd;

//==========================================================
// Helper functions
//
int get_orig_data(char *in_buff, hyc_compress_ctx_t *ctx, hyc_encrypt_ctx_t *ectx,
		uint32_t mod_size) {

	int ret = -1;

	//First decrypt
	size_t plain_txtsz = hyc_encrypt_plain_bufsz(ectx, in_buff, mod_size);
	char *eo_buf       = calloc(1, plain_txtsz);
	size_t eo_buf_sz   = plain_txtsz;
	ret = hyc_decrypt(ectx, in_buff, mod_size, eo_buf, &eo_buf_sz);

	//Second uncompress
	char *ubuf     = calloc(1, BLK_SZ);
	size_t ubuf_sz = BLK_SZ;

	ret = hyc_uncompress(ctx, eo_buf, eo_buf_sz, ubuf, &ubuf_sz);
	memcpy(in_buff, ubuf, BLK_SZ);
	return ret;
}

void get_value_from_rec(as_record* p_rec, key_props_t *props, uint64_t offset) {

	char *buffer = calloc(1, 256);
	char *tmp_buf = calloc(2, BLK_SZ);
	int rc;
	uint32_t bytes_copied;

	/*  Regardless of the size of block_size at Stord, blk_sz at Aerospike
	 *  will always be 4k, unless changed in Aerospike config.
	 */
	bytes_copied = as_bytes_copy(as_record_get_bytes(p_rec, CACHE_BIN), 0,
			(uint8_t *)tmp_buf, BLK_SZ);

#ifdef IS_ENCRYPTED
	rc = get_orig_data(tmp_buf, props->ctx, props->ectx, bytes_copied);
#endif
	uint16_t cksum = crc_t10dif((const unsigned char *)tmp_buf, BLK_SZ);

	sprintf(buffer, "%ld:%d:%d\n", offset * 512 , props->blk_sz, cksum);
	rc = write(fd, buffer, strlen(buffer));
	if (!rc) {
		printf("failed to write data for offset: %ld\n", offset * 512);
	}

	free(tmp_buf);
	free(buffer);
	return;
}

void rec_to_process(key_props_t *props, as_record* p_rec) {

	char *tmp, *tmp1;
	int vmdkid, ckptid;

	tmp  = calloc(1, 128);

	tmp1 = tmp;
	vmdkid = ckptid = -1;

	char *key_str = as_val_tostring(p_rec->key.valuep);

	/*Copying only key, original string has quotes with it*/
	strncpy(tmp, (key_str) + 1, strlen(key_str) - 2);

	/*Key
	  In case of child cache_type (Vmdkid:ckptid:offset)
	  In case of parent cache_type (ParentDiskVmdkID:offset)
	 */
	vmdkid = atoi(strsep(&tmp, ":"));
	if (vmdkid != props->vmdkid) {
		goto out;
	}

	if (props->cache_type) {
		ckptid = atoi(strsep(&tmp, ":"));
		if (props->ckpt && props->ckpt != ckptid) {
			goto out;
		}
	}

	uint64_t offset = atoi(strsep(&tmp, ":"));
	get_value_from_rec(p_rec, props, offset);
out:
	free(key_str);
	free(tmp1);
	return;
}

//==========================================================
// Scan Callback
//
bool scan_cb(const as_val* p_val, void* udata)
{
	bool ret = true;

	key_props_t *props = (key_props_t *)udata;

	if (!p_val) {
		printf("Scan callback returned null - scan is complete\n");
		return false;
	}

	as_record* p_rec = as_record_fromval(p_val);
	if (!p_rec) {
		printf("AeroSpike Scan callback returned non-as_record object\n");
		return false;
	}

	if (p_rec->key.valuep) {
		rec_to_process(props, p_rec);
	} else {
		printf(" Error, Key is not part of record\n");
		ret = false;
	}

	as_record_destroy(p_rec);
	return ret;
}

//==========================================================
// Aero connection
//
int connect_to_aerospike(aerospike* p_as, char *host, int port)
{
	// Initialize cluster configuration.
	as_config config;
	as_config_init(&config);

	if (!as_config_add_hosts(&config, host, port)) {
		printf("Invalid host(s) %s\n", host);
		as_event_close_loops();
		return 1;
	}

	aerospike_init(p_as, &config);

	as_error err;

	// Connect to the Aerospike database cluster. Assume this is the first thing
	// done after calling example_get_opts(), so it's ok to exit on failure.
	if (aerospike_connect(p_as, &err) != AEROSPIKE_OK) {
		printf("aerospike_connect() returned %d - %s\n", err.code, err.message);
		as_event_close_loops();
		aerospike_destroy(p_as);
		return 1;
	}
	printf("Connected to %s\n", host);
	return 0;
}

void usage()
{
	printf("**********Usage***********\n");
	printf("-h Aerospike host(s)\n");
	printf("-n Namespace to scan\n");
	printf("-s Set to scan\n");
	printf("-v Vmdkid to find while scanning\n");
	printf("-c Checkpoint to find while scanning\n");

	printf("\n*********Optional args************\n");
	printf("-p Aerospike port Default: 3000\n");
	printf("-b Block size     Default: 4096\n");
	printf("-t Cache type     Default: normal\n");

	return;
}

int main(int argc, char* argv[])
{
	int c, i, rc;
	char *host = NULL;
	char *namespace = NULL;
	char *set = NULL;
	char *cache_type = NULL;
	int port;
	int vmdkid;
	int ckpt;
	int blk_sz;

	key_props_t props;

	aerospike as;
	as_error err;
	as_scan scan;

	if (argc == 1) {
		printf("No arguments given!\n");
		usage();
		return 1;
	}
	rc     = 0;
	vmdkid = 0;
	ckpt   = 0;
	port   = 3000;
	blk_sz = BLK_SZ;

	while ((c = getopt_long(argc, argv, SHORT_OPTS, LONG_OPTS, &i)) != -1) {
		switch (c) {
			case 'h':
				host = strdup(optarg);
				break;

			case 'p':
				port = atoi(optarg);
				break;

			case 'n':
				namespace = strdup(optarg);
				break;
			case 's':
				set = strdup(optarg);
				break;
			case 'v':
				vmdkid = atoi(optarg);
				break;
			case 'c':
				ckpt = atoi(optarg);
				break;
			case 'b':
				blk_sz = atoi(optarg);
				break;
			case 't':
				cache_type = strdup(optarg);
				break;

			default:
				printf("unknown option: %s\n", optarg);
				usage();
				rc = 1;
				goto out;
		}
	}
	if (!vmdkid || !ckpt || host == NULL || namespace == NULL || set == NULL) {
		usage();
		rc = 1;
		goto out;
	}

	if (cache_type) {
		props.cache_type = 0; //implies cache_type is parent
	} else {
		props.cache_type = 1; //implies cache_type is child
	}
	props.vmdkid = vmdkid;
	props.ckpt = ckpt;
	props.blk_sz = blk_sz;
#ifdef IS_ENCRYPTED
	//Encryption init
	int encrypt_type = get_encrypt_type("aes128-gcm");
	hyc_disk_uuid_t e_uuid = {0, 0};
	hyc_encrypt_ctx_t *ectx = hyc_encrypt_ctx_init(e_uuid, encrypt_type);

	props.ectx = ectx;

	//Compresssion init
	hyc_disk_uuid_t c_uuid = {0, 0};
	int compress_type = get_compress_type("snappy");
	hyc_compress_ctx_t *ctx = hyc_compress_ctx_init(c_uuid, compress_type, 2, 1);

	props.ctx = ctx;
#endif


	printf("*******************\n");
	printf("Host: %s, port: %d\n", host, port);
	printf("Namespace: %s and Set: %s\n", namespace, set);
	printf("Vmdk_id: %d\n", vmdkid);
	printf("*******************\n");

	rc = connect_to_aerospike(&as, host, port);
	if (rc) {
		goto out;
	}

	fd = open(OUTPUT_FILE, O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR);

	as_scan_init(&scan, namespace, set);

	// Do the scan. This call blocks while the scan is running - callbacks are
	// made in the scope of this call.
	printf("starting scan all ...\n");
	if (aerospike_scan_foreach(&as, &err, NULL, &scan, scan_cb, &props) !=
			AEROSPIKE_OK) {
		printf("aerospike_scan_foreach() returned %d - %s\n", err.code, err.message);
	}

	as_scan_destroy(&scan);
	printf("standard scan successfully completed\n");

	close(fd);
	aerospike_close(&as, &err);
	as_event_close_loops();
	aerospike_destroy(&as);
out:
	free(host);
	free(namespace);
	free(set);

	return rc;
}
