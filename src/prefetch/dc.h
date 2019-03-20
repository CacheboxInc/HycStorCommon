#ifndef __DC_H__
#define __DC_H__

typedef struct {
  /* Parameters */
  int		n_lookback;

  /* Correlation trackers */
  uint64_t 	prev_lba;

#define CORR_DEPTH	(3)
  /* We look for a repeating delta pattern based on
   * matching a prefix of length CORR_DEPTH ...
   * e.g., with a CORR_DEPTH of 3, we can pick up
   * delta patterns such as the following:
   *       [1, 3, 5, ...], [1, 3, 5, ...]
   */

  int		*delta_buffer;
  int		next_idx;

  bool		fixed_stride;
  bool		found_match;
} dc_t;


dc_t* dc_alloc_and_init(int n_lookback);
void dc_finalize_and_free(dc_t *dc_struct);

void dc_reset(dc_t *dc_struct);

/* Add previous lba in stream to the DC structure 
 * - returns true if we have detected a pattern
 */
bool dc_add_record(dc_t *dc_struct, uint64_t lba);

/* Retrieve prefetch recommendations
 * - return value = # of recommendations
 */
int dc_get_requests(dc_t *dc_struct, uint64_t last_lba, uint64_t *prefetch_lbas, 
		int prefetch_depth, bool *is_strided);

#endif	/* __DC_H__ */
