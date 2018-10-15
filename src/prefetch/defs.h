#ifndef __DEFS_H__
#define __DEFS_H__

#include <stdbool.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#define max(a,b)	(((a) > (b))? (a) : (b))
#define min(a,b)	(((a) < (b))? (a) : (b))
#define abs(a)		(((a) < 0)? -1*(a) : (a))

#define VALIDATE_MALLOC(ptr)				\
do {							\
  if ((ptr) == NULL) {					\
    printf("%s:%d malloc error\n", __FILE__, __LINE__);	\
    exit(1);						\
  }							\
} while (0)

#endif /* __DEFS_H__ */
