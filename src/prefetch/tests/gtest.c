#include "defs.h"
#include "ghb.h"

#define N_ACCESSES	(64)
#define N_STREAMS	(4)
#define PREFETCH_DEPTH	(8)

typedef enum {
  SEQUENTIAL =		0,
  POS_STRIDE_2 =	1,
  NEG_STRIDE_3 =	2,
  CORRELATED =		3,
  RANDOM =		4
} pattern_t;

int get_random(int rmax) {
  return (rand() % rmax);
}

uint64_t generate_one_access(pattern_t pattern, int i) {
  int i_stream = (i%N_STREAMS), i_off = (i/N_STREAMS);
  uint64_t lba_base = (((1+i_stream)*1024ULL*1024*1024) +
		       (1)*1024ULL*1024);

  switch(pattern) {
  case RANDOM:
    return (lba_base + get_random(1024*1024));
  case CORRELATED:
    {
      int corr_stride[3] = {0, 1, 2};
      return (lba_base + i_off + corr_stride[i_off%3]);
    }    
  case NEG_STRIDE_3:
    return (lba_base + i_off*(-3));
  case POS_STRIDE_2:
    return (lba_base + i_off*(2));
  case SEQUENTIAL:
  default:
    return (lba_base + i_off);
  }
}

void generate_accesses(pattern_t pattern, uint64_t lbas[]) {
  int i;
  for (i=0; i<N_ACCESSES; i++)
    lbas[i] = generate_one_access(pattern, i);
}

main(int argc, char *argv[]) {
  pattern_t	pattern = SEQUENTIAL;
  uint64_t	lbas[N_ACCESSES];

  ghb_params_t	params;
  ghb_t		ghb;
  int		i, j, nprefetch;
  uint64_t	lba, prefetch_lbas[PREFETCH_DEPTH];

  if (argc > 1)
    pattern = atoi(argv[1]);

  generate_accesses(pattern, lbas);
  printf("Access pattern:\n");
  for (i=0; i<N_ACCESSES; i++) {
    printf("0x%.16" PRIx64 " ", lbas[i]);
    if (((i+1)%N_STREAMS) == 0)
      printf("\n");
  }
  printf("\n");

  params.n_index = 32;
  params.n_history = 1024;
  params.n_lookback = 8;
  params.prefetch_depth = PREFETCH_DEPTH;

  ghb_init(&ghb, &params);
  for (i=0; i<N_ACCESSES; i++) {
    lba = lbas[i];
    printf("[0x%" PRIx64 "]: ", lba);
    nprefetch = ghb_update_and_query(&ghb, 1, lba, prefetch_lbas);
    printf("\t");
    for (j=0; j<nprefetch; j++)
      printf("0x%" PRIx64 ", ", prefetch_lbas[j]);
    printf("\n");
  }
  ghb_finalize(&ghb);
}
