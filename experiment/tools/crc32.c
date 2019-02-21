#include <linux/types.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define BLOCK_SIZE 16384
#define BASE 65521L /* largest prime smaller than 65536 */
#define NMAX 5552
/* NMAX is the largest n such that 255n(n+1)/2 + (n+1)(BASE-1) <= 2^32-1 */

#define DO1(buf,i)  {s1 += buf[i]; s2 += s1;}
#define DO2(buf,i)  DO1(buf,i); DO1(buf,i+1);
#define DO4(buf,i)  DO2(buf,i); DO2(buf,i+2);
#define DO8(buf,i)  DO4(buf,i); DO4(buf,i+4);
#define DO16(buf)   DO8(buf,0); DO8(buf,8);

uint32_t net_checksum(uint32_t adler, char *buf, uint32_t len)
{
	uint32_t s1 = adler & 0xffff;
	uint32_t s2 = (adler >> 16) & 0xffff;
	int      k;
	int      iter;

	if (buf == NULL) {
		return 1;
	}

	iter = 0;

	while (len > 0) {
		k = len < NMAX ? len : NMAX;
		len -= k;
		while (k >= 16) {
			DO16(buf);
			buf += 16;
			k -= 16;
		}

		if (k != 0) {
			do {
				s1 += *buf++;
				s2 += s1;
			} while (--k);
		}

		s1 %= BASE;
		s2 %= BASE;
		iter++;
	}

	return (s2 << 16) | s1;
}

int main(int argc, char *argv[]) {
	//char *block = calloc(BLOCK_SIZE, 1);
	char block[BLOCK_SIZE]={'\0'};
	size_t nread=0, ncur = 0;
	int fd = open(argv[1], O_RDONLY);
	if (fd == -1) {
		printf("file %s not found\n", argv[1]);
		return 1;
	}

	while (nread < BLOCK_SIZE) {
		ncur = read(fd, block + nread, BLOCK_SIZE - nread);  
		nread += ncur;
		//printf("nread till now %lu\n", nread);
	}
	//u can use u/hu/d as format specifier for printing 
	//unsigned short, it would give same result
	printf("%u\n", net_checksum(0, block, sizeof(block)));
	close(fd);
	return 0;
}

