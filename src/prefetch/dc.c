#include "defs.h"
#include "dc.h"

/* -----------------------------------------------------------------------------
 * Private functions
 * -----------------------------------------------------------------------------
 */

static inline
int compute_lba_delta(uint64_t prev, uint64_t cur) {
  return (prev >= cur)? (int)(prev - cur) : -1*(int)(cur - prev);
}

static inline
bool check_for_delta_equal(dc_t *dc_struct,
			   int start_idx, int n_entries) {
  int i, delta = dc_struct->delta_buffer[start_idx];
  int *dbuf = dc_struct->delta_buffer;
  for (i=1; i<n_entries; i++) {
    if (delta != dbuf[start_idx+i])
      return false;
  }
  return true;
}

static inline
bool check_for_delta_match(dc_t *dc_struct,
			   int key_idx, int comp_idx, int n_entries) {
  int i;
  int *dbuf = dc_struct->delta_buffer;
  for (i=0; i<n_entries; i++) {
    if (dbuf[key_idx+i] != dbuf[comp_idx+i])
      return false;
  }
  return true;
}


/* -----------------------------------------------------------------------------
 * Public functions
 * -----------------------------------------------------------------------------
 */

dc_t* dc_alloc_and_init(int n_lookback, int prefetch_depth) {
  dc_t *dc_struct = (dc_t *)malloc(sizeof(dc_t)); VALIDATE_MALLOC(dc_struct);

  memset(dc_struct, 0, sizeof(dc_t));
  dc_struct->n_lookback = n_lookback;
  dc_struct->prefetch_depth = prefetch_depth;
  dc_struct->delta_buffer = (int *)malloc(sizeof(int)*dc_struct->n_lookback);
  dc_reset(dc_struct);

  return dc_struct;
}

void dc_finalize_and_free(dc_t *dc_struct) {
  free(dc_struct->delta_buffer);
  free(dc_struct);
}

void dc_reset(dc_t *dc_struct) {
  dc_struct->prev_lba = 0;
  dc_struct->next_idx = -1;
  dc_struct->fixed_stride = false;
  dc_struct->found_match = false;
}

bool dc_add_record(dc_t *dc_struct, uint64_t lba) {
  int lba_delta;

  /* next_idx == -1 : init prev_lba */
  if (dc_struct->next_idx == -1) {
    dc_struct->prev_lba = lba;
    dc_struct->next_idx += 1;
    return false;
  }

  /* Compute and add delta to delta_buffer */
  lba_delta = compute_lba_delta(dc_struct->prev_lba, lba);
  dc_struct->prev_lba = lba;
  dc_struct->delta_buffer[dc_struct->next_idx] = lba_delta;
  dc_struct->next_idx += 1;

  /* Check for fixed stride */
  if (dc_struct->next_idx == CORR_DEPTH) {
    if (check_for_delta_equal(dc_struct, 0, CORR_DEPTH)) {
      dc_struct->fixed_stride = true;
      return true;
    }
  }

  /* Can check for correlations once we have at least CORR_DEPTH+1
   * entries in the delta buffer ...
   */
  if (dc_struct->next_idx > CORR_DEPTH) {
    dc_struct->found_match = 
      check_for_delta_match(dc_struct, 0, dc_struct->next_idx - CORR_DEPTH, CORR_DEPTH);
    return dc_struct->found_match;
  }

  return false;
}

int dc_get_requests(dc_t *dc_struct, uint64_t last_lba, uint64_t *prefetch_lbas) {
  if (dc_struct->fixed_stride) {
    int i;
    int stride = dc_struct->delta_buffer[0];

   // printf("--> fixed_stride: %d\n", stride);

    for (i=0; i<dc_struct->prefetch_depth; i++)
      prefetch_lbas[i] = last_lba + (i+1)*stride;
    return dc_struct->prefetch_depth;
  } else if (dc_struct->found_match) {
    int i = 0;
    /* Adjust index to skip deltas associated with the current correlation */
    int idx = (dc_struct->next_idx - CORR_DEPTH - 1);
    uint64_t cur_lba = last_lba;

   // printf("--> correlated pattern: ");
    // for (j=idx; j>=0; j--) printf("%d ", dc_struct->delta_buffer[j]);
   	 // printf("\n");

    while ((idx >= 0) && (i < dc_struct->prefetch_depth)) {
      /* Apply delta to compute the LBA */
      cur_lba += dc_struct->delta_buffer[idx];
      prefetch_lbas[i] = cur_lba;
      /* Update idx, i for next iteration
       * Reset idx if we run out of entries
       */
      if (--idx == -1)
	idx = (dc_struct->next_idx - CORR_DEPTH - 1);
      i++;
    }
    return dc_struct->prefetch_depth;
  }

  return 0;
}

